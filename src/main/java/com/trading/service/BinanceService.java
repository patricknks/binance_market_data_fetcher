package com.trading.service;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.trading.config.AppConfig;
import com.trading.util.TimeFrames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Service
public class BinanceService {

    private static final Logger logger = LoggerFactory.getLogger(BinanceService.class);

    /**
     * Window widths, in candles. getLimit() returns one of these and it serves two purposes:
     * it is the API `limit` parameter and, multiplied by the interval, the width of the chunk
     * that gets requested.
     *
     * SINGLE_CANDLE means the caller is up to date; CATCH_UP means at least two candles have
     * passed since the stored application time, which is what the scheduler reads as "a new
     * candle has closed". Keep the values as they are -- DatabaseService.updateLastTransaction
     * and the indicator trigger both branch on them.
     */
    public static final int LIMIT_SINGLE_CANDLE = 1;
    public static final int LIMIT_CATCH_UP = 50;

    private final WebClient webClient;
    private final AppConfig appConfig;
    private final Random random = new Random();

    public BinanceService(AppConfig appConfig) {
        this.appConfig = appConfig;
        this.webClient = WebClient.builder()
                .build();
    }

    public long fetchServerTimeMs() {
        try {
            String resp = webClient.get()
                    .uri("https://api.binance.com/api/v3/time")
                    .retrieve()
                    .bodyToMono(String.class)
                    .timeout(Duration.ofSeconds(appConfig.getBinance().getTimeoutSeconds()))
                    .block();
            if (resp != null && !resp.isEmpty()) {
                com.google.gson.JsonObject obj = JsonParser.parseString(resp).getAsJsonObject();
                if (obj.has("serverTime")) {
                    return obj.get("serverTime").getAsLong();
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to fetch Binance server time: {}", e.getMessage());
        }
        return System.currentTimeMillis();
    }

    /**
     * Live order book snapshot from /fapi/v1/depth, or null on failure.
     *
     * Deliberately no retry loop: the depth sampler runs every 30 seconds and a retry with
     * backoff would only push the snapshot into the next slot. A missed sample just lowers
     * the candle's DEPTH_SAMPLES count.
     */
    public JsonObject fetchOrderBook(int limit) {
        try {
            String response = webClient.get()
                    .uri(appConfig.getBinance().getDepthUrl() + "?symbol={symbol}&limit={limit}",
                            appConfig.getBinance().baseSymbol(), limit)
                    .retrieve()
                    .bodyToMono(String.class)
                    .timeout(Duration.ofSeconds(appConfig.getBinance().getTimeoutSeconds()))
                    .block();
            if (response == null || response.isEmpty()) {
                return null;
            }
            return JsonParser.parseString(response).getAsJsonObject();
        } catch (Exception e) {
            logger.warn("Order book fetch failed: {}", e.getMessage());
            return null;
        }
    }

    // letakkan di dalam class BinanceService (boleh static inner class)
    public static class FetchKlinesResult {
        private final List<JsonArray> klines;
        private final int limit;

        public FetchKlinesResult(List<JsonArray> klines, int limit) {
            this.klines = klines;
            this.limit = limit;
        }

        public List<JsonArray> getKlines() { return klines; }
        public int getLimit() { return limit; }
    }

    public FetchKlinesResult fetchKlines(LocalDateTime startTime, LocalDateTime endTime) {
        List<JsonArray> klines = new ArrayList<>();
        LocalDateTime current = startTime;
        int limit = getLimit(startTime, endTime);

        logger.info("LIMIT set to: {}", limit);
        logger.info("Fetching Binance klines from {} to {} with LIMIT={}...", startTime, endTime, limit);

        if (current.isBefore(endTime)) {
            LocalDateTime chunkEnd = chunkEnd(current, limit, endTime); // limit candles wide

            try {
                String response = callBinanceApi(appConfig.getBinance().klinesUrlForMarket(), current, chunkEnd, true);
                if (response != null && !response.isEmpty()) {
                    JsonArray data = JsonParser.parseString(response).getAsJsonArray();
                    int fetched = data.size();
                    for (JsonElement element : data) klines.add(element.getAsJsonArray());
                    logger.info("Successfully fetched {} klines for chunk {} to {}", fetched, current, chunkEnd);
                    // logger.info(response);
                } else {
                    logger.warn("No data Klines returned for chunk {} to {}", current, chunkEnd);
                }
            } catch (Exception e) {
                logger.error("Klines fetch failed after 3 attempts for chunk {} to {}: {}",
                        current, chunkEnd, e.getMessage());
                throw new RuntimeException("Klines fetch failed", e);
            }

            current = chunkEnd;
        }

        return new FetchKlinesResult(klines, limit);
    }

    public List<JsonElement> fetchOpenInterest(LocalDateTime startTime, LocalDateTime endTime) {
        List<JsonElement> oi = new ArrayList<>();
        LocalDateTime current = startTime;

        if (current.isBefore(endTime)) {
            LocalDateTime chunkEnd = chunkEnd(current, getLimit(startTime, endTime), endTime);

            try {
                String response = callBinanceApi(appConfig.getBinance().getOiUrl(),
                        current, chunkEnd, false);
                if (response != null && !response.isEmpty()) {
                    JsonArray data = JsonParser.parseString(response).getAsJsonArray();
                    for (JsonElement element : data) {
                        oi.add(element);
                    }
                } else {
                    logger.warn("No data Open Interest returned for chunk {} to {}", current, chunkEnd);
                }
            } catch (Exception e) {
                logger.warn("OI fetch failed after 3 attempts for chunk {} to {}: {}", 
                        current, chunkEnd, e.getMessage());
                // Continue without OI data
            }

            current = chunkEnd;
        }

        return oi;
    }

    public List<JsonElement> fetchLongShortRatio(LocalDateTime startTime, LocalDateTime endTime) {
        List<JsonElement> ls = new ArrayList<>();
        LocalDateTime current = startTime;

        if (current.isBefore(endTime)) {
            LocalDateTime chunkEnd = chunkEnd(current, getLimit(startTime, endTime), endTime);

            try {
                String response = callBinanceApi(appConfig.getBinance().getLsUrl(),
                        current, chunkEnd, false);
                if (response != null && !response.isEmpty()) {
                    JsonArray data = JsonParser.parseString(response).getAsJsonArray();
                    for (JsonElement element : data) {
                        ls.add(element);
                    }
                } else {
                    logger.warn("No data Long/Short returned for chunk {} to {}", current, chunkEnd);
                }
            } catch (Exception e) {
                logger.warn("LS fetch failed after 3 attempts for chunk {} to {}: {}", 
                        current, chunkEnd, e.getMessage());
                // Continue without LS data
            }

            current = chunkEnd;
        }

        return ls;
    }

    /**
     * Funding rate history.
     *
     * Unlike the /futures/data/ endpoints this one is not limited to the last 30 days, so it
     * can serve the entire history back to the perpetual's launch (2019-11-27 for ETHUSDT).
     * Responses are ascending and capped at 500 entries, so the range is walked page by page.
     * It also takes no interval/period parameter, hence the dedicated call helper.
     */
    public List<JsonElement> fetchFundingRate(LocalDateTime startTime, LocalDateTime endTime) {
        List<JsonElement> out = new ArrayList<>();
        long cursor = toMs(startTime);
        long endMs = toMs(endTime);
        int page = 0;

        while (cursor <= endMs && page++ < 500) {
            String response = callFundingApi(cursor, endMs);
            if (response == null || response.isEmpty()) {
                break;
            }
            JsonArray data = JsonParser.parseString(response).getAsJsonArray();
            if (data.size() == 0) {
                break;
            }
            long lastTime = cursor;
            for (JsonElement element : data) {
                out.add(element);
                lastTime = element.getAsJsonObject().get("fundingTime").getAsLong();
            }
            if (lastTime <= cursor) {
                break; // no forward progress, stop rather than loop forever
            }
            cursor = lastTime + 1;
        }

        logger.info("Fetched {} funding rate entries from {} to {}", out.size(), startTime, endTime);
        return out;
    }

    private String callFundingApi(long startMs, long endMs) {
        int attempt = 0;
        int maxAttempts = 3;

        while (attempt < maxAttempts) {
            try {
                return webClient.get()
                        .uri(appConfig.getBinance().getFundingUrl()
                                        + "?symbol={symbol}&startTime={start}&endTime={end}&limit={limit}",
                                appConfig.getBinance().baseSymbol(), startMs, endMs, "1000")
                        .retrieve()
                        .bodyToMono(String.class)
                        .timeout(Duration.ofSeconds(appConfig.getBinance().getTimeoutSeconds()))
                        .block();
            } catch (Exception e) {
                attempt++;
                if (attempt >= maxAttempts) {
                    logger.warn("Funding rate fetch failed after {} attempts: {}", maxAttempts, e.getMessage());
                    return null;
                }
                try {
                    Thread.sleep(calculateBackoff(attempt - 1));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return null;
    }

    private String callBinanceApi(String url, LocalDateTime startTime, LocalDateTime endTime, boolean isKlines) {
        long startMs = toMs(startTime);
        long endMs = toMs(endTime);
        String paramName = isKlines ? "interval" : "period";

        int attempt = 0;
        int maxAttempts = 3;

        while (attempt < maxAttempts) {
            try {
                String response = webClient.get()
                        .uri(url + "?symbol={symbol}&" + paramName + "={timeFrame}&startTime={start}&endTime={end}&limit={limit}",
                                appConfig.getBinance().baseSymbol(),
                                appConfig.getBinance().normalisedInterval(),
                                startMs,
                                endMs,
                                "1000")
                        .retrieve()
                        .bodyToMono(String.class)
                        .timeout(Duration.ofSeconds(appConfig.getBinance().getTimeoutSeconds()))
                        .block();
                return response;
            } catch (Exception e) {
                attempt++;
                if (attempt >= maxAttempts) {
                    logger.error("API call failed after {} attempts for {}", maxAttempts, url);
                    throw new RuntimeException("Max API call attempts exceeded for " + url, e);
                }
                long backoffMs = calculateBackoff(attempt - 1);
                double backoffSec = backoffMs / 1000.0;
                logger.warn("{} fetch failed (attempt {}), retrying in {:.2f}s: {}", 
                        isKlines ? "Klines" : "API", attempt, backoffSec, e.getMessage());
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        throw new RuntimeException("API call failed after " + maxAttempts + " attempts");
    }

    private long calculateBackoff(int attempt) {
        long baseDelay = 1000; // 1 second
        long maxDelay = 60000;  // 60 seconds
        long delay = Math.min(baseDelay * (long) Math.pow(2, attempt), maxDelay);
        // Add 0-10% jitter
        long jitter = (long) (delay * random.nextDouble() * 0.1);
        return delay + jitter;
    }

    /**
     * How many candles to ask for, from how far behind the caller is.
     *
     * The thresholds count candles rather than hours, so a 15m run behaves exactly like a 1h
     * run: one candle while up to date, a catch-up window the moment a new candle closes. On
     * a 1h interval this returns what the previous hour-based version returned.
     */
    private int getLimit(LocalDateTime start, LocalDateTime end) {
        long candles = TimeFrames.candlesBetween(start, end, intervalStep());
        if (candles <= 1) return LIMIT_SINGLE_CANDLE;
        if (candles <= 48) return LIMIT_CATCH_UP;
        if (candles <= 500) return 100;
        return 500;
    }

    /** The configured candle width; every window and chunk below is a multiple of it. */
    private Duration intervalStep() {
        return appConfig.getBinance().intervalDuration();
    }

    /** End of a chunk that covers `limit` candles starting at `current`. */
    private LocalDateTime chunkEnd(LocalDateTime current, int limit, LocalDateTime hardEnd) {
        LocalDateTime end = current.plus(intervalStep().multipliedBy(limit));
        return end.isAfter(hardEnd) ? hardEnd : end;
    }

    private long toMs(LocalDateTime time) {
        ZonedDateTime zdt = time.atZone(ZoneId.of("UTC"));
        return zdt.toInstant().toEpochMilli();
    }
}
