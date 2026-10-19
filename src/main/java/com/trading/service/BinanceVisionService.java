package com.trading.service;

import com.trading.config.AppConfig;
import com.trading.depth.DepthBands;
import com.trading.depth.DepthSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Reads historical archives from data.binance.vision.
 *
 * Archive families used:
 *   - klines (spot or futures UM) -> OHLCV and trade columns
 *   - futures UM metrics   -> open interest and long/short ratios (available from 2021-12-01)
 *   - futures UM bookDepth -> order book depth bands (available from 2023-01-01)
 *
 * A missing archive (HTTP 404) is a normal outcome, not an error: it just means that
 * month/day is not published, and the caller moves on.
 */
@Service
public class BinanceVisionService {

    private static final Logger logger = LoggerFactory.getLogger(BinanceVisionService.class);

    private static final DateTimeFormatter MONTH_FMT = DateTimeFormatter.ofPattern("yyyy-MM");
    private static final DateTimeFormatter DAY_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter METRIC_TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Kline archives switched from millisecond to microsecond timestamps starting 2025-01.
     * Millisecond values for any plausible date are ~1.8e12, microsecond ~1.8e15, so this
     * threshold separates them safely.
     */
    private static final long MICROS_THRESHOLD = 100_000_000_000_000L;

    private final AppConfig appConfig;
    private final HttpClient httpClient;

    public BinanceVisionService(AppConfig appConfig) {
        this.appConfig = appConfig;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    /** One kline straight out of the archive, already normalised to milliseconds. */
    public static class KlineRow {
        public long openTimeMs;
        public long closeTimeMs;
        public double openPrice, highPrice, lowPrice, closePrice;
        public double volume, quoteAssetVolume, takerBuyBaseVolume, takerBuyQuoteVolume;
        public long numberOfTrades;
    }

    /** One 5-minute futures metrics sample. */
    public static class MetricsRow {
        public Double sumOpenInterest;
        public Double sumOpenInterestValue;
        /** sum_toptrader_long_short_ratio == REST topLongShortPositionRatio.longShortRatio */
        public Double topTraderPositionRatio;
    }

    public List<KlineRow> fetchKlinesMonth(YearMonth month) {
        String url = String.format("%s/data/%s/monthly/klines/%s/%s/%s-%s-%s.zip",
                appConfig.getWarmup().getBaseUrl(), marketPath(),
                symbol(), interval(), symbol(), interval(), MONTH_FMT.format(month));
        return parseKlines(url);
    }

    public List<KlineRow> fetchKlinesDay(LocalDate day) {
        String url = String.format("%s/data/%s/daily/klines/%s/%s/%s-%s-%s.zip",
                appConfig.getWarmup().getBaseUrl(), marketPath(),
                symbol(), interval(), symbol(), interval(), DAY_FMT.format(day));
        return parseKlines(url);
    }

    /**
     * Archive root for OHLCV. Spot klines reach back to 2017-08 while USD-M perpetual klines
     * only start at 2020-01, so switching markets also shortens the available history.
     */
    private String marketPath() {
        return appConfig.getBinance().perpMarket() ? "futures/um" : "spot";
    }

    /**
     * Futures metrics for one day, keyed by the sample's UTC timestamp.
     * Granularity is 5 minutes, so a day yields 288 entries.
     */
    public Map<LocalDateTime, MetricsRow> fetchMetricsDay(LocalDate day) {
        String url = String.format("%s/data/futures/um/daily/metrics/%s/%s-metrics-%s.zip",
                appConfig.getWarmup().getBaseUrl(), symbol(), symbol(), DAY_FMT.format(day));

        Map<LocalDateTime, MetricsRow> out = new HashMap<>();
        readCsv(url, line -> {
            String[] f = line.split(",", -1);
            if (f.length < 6) return;
            // create_time,symbol,sum_open_interest,sum_open_interest_value,
            // count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,...
            LocalDateTime ts;
            try {
                ts = LocalDateTime.parse(f[0].trim(), METRIC_TS_FMT);
            } catch (Exception e) {
                return; // header row or malformed line
            }
            MetricsRow row = new MetricsRow();
            row.sumOpenInterest = parseDoubleOrNull(f[2]);
            row.sumOpenInterestValue = parseDoubleOrNull(f[3]);
            row.topTraderPositionRatio = parseDoubleOrNull(f[5]);
            out.put(ts, row);
        });
        return out;
    }

    /**
     * Order book depth snapshots for one day, keyed by snapshot time (epoch ms, UTC).
     *
     * Each snapshot spans 10 or 12 rows of timestamp,percentage,depth,notional -- one per
     * band, negative percentage for bids. Only notional (USDT) is kept. Snapshots arrive
     * roughly every 30 seconds but not on exact boundaries (00:06:05, 00:06:32, ...), so they
     * are keyed by their real timestamp and bucketed into candles by the caller.
     *
     * The archive is daily only (no monthly files) and USD-M only.
     */
    public Map<Long, DepthSnapshot> fetchBookDepthDay(LocalDate day) {
        String url = String.format("%s/data/futures/um/daily/bookDepth/%s/%s-bookDepth-%s.zip",
                appConfig.getWarmup().getBaseUrl(), symbol(), symbol(), DAY_FMT.format(day));

        Map<Long, DepthSnapshot> out = new TreeMap<>();
        readCsv(url, line -> addBookDepthLine(out, line));
        return out;
    }

    /** Folds one bookDepth CSV line into its snapshot; header and unknown bands are skipped. */
    static void addBookDepthLine(Map<Long, DepthSnapshot> out, String line) {
        String[] f = line.split(",", -1);
        if (f.length < 4) return;
        long ts;
        double percentage;
        try {
            ts = LocalDateTime.parse(f[0].trim(), METRIC_TS_FMT).toInstant(ZoneOffset.UTC).toEpochMilli();
            percentage = Double.parseDouble(f[1].trim());
        } catch (Exception e) {
            return; // header row or malformed line
        }
        int band = DepthBands.indexOf(Math.abs(percentage));
        Double notional = parseDoubleOrNull(f[3]);
        if (band < 0 || notional == null) return;

        DepthSnapshot snapshot = out.computeIfAbsent(ts, k -> new DepthSnapshot());
        if (percentage < 0) {
            snapshot.bid[band] = notional;
        } else {
            snapshot.ask[band] = notional;
        }
    }

    private List<KlineRow> parseKlines(String url) {
        List<KlineRow> out = new ArrayList<>();
        readCsv(url, line -> {
            String[] f = line.split(",", -1);
            if (f.length < 11) return;
            KlineRow row = new KlineRow();
            try {
                row.openTimeMs = normalizeToMs(Long.parseLong(f[0].trim()));
                row.openPrice = Double.parseDouble(f[1]);
                row.highPrice = Double.parseDouble(f[2]);
                row.lowPrice = Double.parseDouble(f[3]);
                row.closePrice = Double.parseDouble(f[4]);
                row.volume = Double.parseDouble(f[5]);
                row.closeTimeMs = normalizeToMs(Long.parseLong(f[6].trim()));
                row.quoteAssetVolume = Double.parseDouble(f[7]);
                row.numberOfTrades = (long) Double.parseDouble(f[8]);
                row.takerBuyBaseVolume = Double.parseDouble(f[9]);
                row.takerBuyQuoteVolume = Double.parseDouble(f[10]);
            } catch (NumberFormatException e) {
                return; // header row (present in some newer archives) or malformed line
            }
            out.add(row);
        });
        return out;
    }

    /**
     * Downloads a zip and feeds every line of every entry to the consumer.
     * Streams through ZipInputStream so a whole month is never buffered in memory.
     */
    private void readCsv(String url, java.util.function.Consumer<String> lineConsumer) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(appConfig.getWarmup().getHttpTimeoutSeconds()))
                    .GET()
                    .build();

            HttpResponse<InputStream> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

            if (response.statusCode() == 404) {
                logger.info("[Vision] Not published (404): {}", url);
                response.body().close();
                return;
            }
            if (response.statusCode() != 200) {
                logger.warn("[Vision] HTTP {} for {}", response.statusCode(), url);
                response.body().close();
                return;
            }

            int lines = 0;
            try (ZipInputStream zis = new ZipInputStream(response.body())) {
                ZipEntry entry;
                while ((entry = zis.getNextEntry()) != null) {
                    if (entry.isDirectory()) continue;
                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(zis, StandardCharsets.UTF_8));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.isBlank()) continue;
                        lineConsumer.accept(line);
                        lines++;
                    }
                }
            }
            logger.info("[Vision] Read {} lines from {}", lines, url);
        } catch (Exception e) {
            logger.warn("[Vision] Failed to read {}: {}", url, e.getMessage());
        }
    }

    private static long normalizeToMs(long value) {
        return value > MICROS_THRESHOLD ? value / 1000L : value;
    }

    private static Double parseDoubleOrNull(String s) {
        if (s == null || s.isBlank()) return null;
        try {
            return Double.parseDouble(s.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** Archive paths use the real Binance symbol, never the ".P" charting suffix. */
    private String symbol() {
        return appConfig.getBinance().baseSymbol();
    }

    private String interval() {
        // Archive paths are case sensitive and always lower case (.../15m/..., .../1h/...).
        return appConfig.getBinance().normalisedInterval();
    }
}
