package com.trading.scheduler;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.trading.config.AppConfig;
import com.trading.depth.RealtimeDepthBuffer;
import com.trading.service.BinanceService;
import com.trading.service.DatabaseService;
import com.trading.service.HealthCheckListener;
import com.trading.service.ResourceMonitor;
import com.trading.service.WarmupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.trading.util.TimeFrames;

import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Component
public class DataFetcherScheduler {

    private static final Logger logger = LoggerFactory.getLogger(DataFetcherScheduler.class);
    private static final ZoneId JAKARTA = ZoneId.of("Asia/Jakarta");
    private static final ZoneId UTC = ZoneId.of("UTC");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy_MM");
    private static final DateTimeFormatter SECOND_FORMATTER = DateTimeFormatter.ofPattern("s");

    private final BinanceService binanceService;
    private final DatabaseService databaseService;
    private final ResourceMonitor resourceMonitor;
    private final AppConfig appConfig;
    private final WarmupService warmupService;
    private final RealtimeDepthBuffer depthBuffer;

    private int loopCounter = 0;

    private LocalDateTime lastSuccessfulFetchTime = null;

    public DataFetcherScheduler(BinanceService binanceService, DatabaseService databaseService,
                               ResourceMonitor resourceMonitor, AppConfig appConfig,
                               WarmupService warmupService, RealtimeDepthBuffer depthBuffer) {
        this.binanceService = binanceService;
        this.databaseService = databaseService;
        this.resourceMonitor = resourceMonitor;
        this.appConfig = appConfig;
        this.warmupService = warmupService;
        this.depthBuffer = depthBuffer;
    }

    @Scheduled(fixedRateString = "#{@appConfig.refreshRateSeconds * 1000}")
    public void fetchAndStoreData() {
        // Warmup gate. When history is missing or stale we backfill from the archives and
        // skip this tick. runWarmup() returning false means there is nothing left to fill,
        // so we fall through to the realtime path below, which is unchanged.
        if (warmupService.shouldWarmup()) {
            if (warmupService.runWarmup()) {
                return;
            }
        }

        int nowSecond = Integer.valueOf(SECOND_FORMATTER.format(LocalDateTime.now(JAKARTA)));
        logger.info("-------------- START LOOP --------------");
        long loopStartMs = System.currentTimeMillis();
        logger.info("Current Second is : {}", nowSecond);

        if (nowSecond<=55){
            logger.info("----MASUK LOOP----");

            try {            
                HealthCheckListener.updateHeartbeat();

                // NOTE: application time will be synced using kline open_time (top-of-hour)
                // The actual update happens after we fetch klines so we can use the kline open_time

                // Get current DB time
                LocalDateTime currentDbTime = databaseService.getCurrentApplicationTime();

                // One candle back, whatever the configured timeframe is: 1h on a 1h run,
                // 15m on a 15m run. Everything below is expressed in this step.
                Duration step = appConfig.getBinance().intervalDuration();

                // Fetch klines, OI, LS with timing
                long fetchStartMs = System.currentTimeMillis();

                // List<JsonArray> klines = binanceService.fetchKlines(
                //         currentDbTime.minusHours(1),
                //         LocalDateTime.now(UTC)
                // );

                // ######## NEW CODE ##########
                BinanceService.FetchKlinesResult result = binanceService.fetchKlines(
                    currentDbTime.minus(step),
                    LocalDateTime.now(UTC)
                );

                List<JsonArray> klines = result.getKlines();
                int limit = result.getLimit();
                logger.info("fetchKlines LIMIT used: {}", limit);
                // ######## NEW CODE ##########

                if (klines.isEmpty()) {
                    logger.warn("No klines fetched, skipping this cycle");
                    return;
                }

                LocalDateTime businessFromKline = null;
                
                // Sync application time using last kline open_time (matches Python behavior)
                try {
                    JsonArray lastKline = klines.get(klines.size() - 1);
                    long openTimeMsFromKline = lastKline.get(0).getAsLong();
                    LocalDateTime utcDateTimeFromKline = LocalDateTime.ofInstant(Instant.ofEpochMilli(openTimeMsFromKline), UTC);
                    // LocalDateTime businessFromKline = utcDateTimeFromKline.atZone(UTC).withZoneSameInstant(JAKARTA).toLocalDateTime();
                    businessFromKline = utcDateTimeFromKline.atZone(UTC).withZoneSameInstant(JAKARTA).toLocalDateTime();
                    logger.info("BINANCE kline open_time: {}, Updating APPLICATION_TIME to: {}", businessFromKline, businessFromKline);
                    databaseService.updateApplicationTime(openTimeMsFromKline);
                } catch (Exception e) {
                    logger.warn("Failed to sync application time from kline open_time: {}", e.getMessage());
                }

                List<JsonElement> oi = binanceService.fetchOpenInterest(
                        currentDbTime.minus(step),
                        LocalDateTime.now(UTC)
                );

                List<JsonElement> ls = binanceService.fetchLongShortRatio(
                        currentDbTime.minus(step),
                        LocalDateTime.now(UTC)
                );

                // Funding settles every 8 hours regardless of the candle size, so this window
                // stays in hours: reach further back than the kline window to be sure the
                // settlement in effect at the window start is included.
                List<JsonElement> funding = binanceService.fetchFundingRate(
                        currentDbTime.minusHours(17),
                        LocalDateTime.now(UTC)
                );

                long fetchDurationMs = System.currentTimeMillis() - fetchStartMs;
                logger.info("Fetched {} klines, {} OI, {} LS, {} funding in {}ms",
                        klines.size(), oi.size(), ls.size(), funding.size(), fetchDurationMs);

                // Join and build records
                Map<Long, JsonObject> oiMap = buildTimestampMap(oi, "timestamp");
                Map<Long, JsonObject> lsMap = buildTimestampMap(ls, "timestamp");
                NavigableMap<Long, Double> fundingMap = buildFundingMap(funding);

                List<DatabaseService.KlineRecord> records = buildRecords(klines, oiMap, lsMap, fundingMap);

                if (records.isEmpty()) {
                    logger.warn("No valid records built from klines, skipping database operation");
                    return;
                }

                // Insert to DB
                // String partition = DATE_FORMATTER.format(LocalDateTime.now(JAKARTA));

                if (businessFromKline == null) {
                    logger.warn("businessFromKline is null, skipping partition calculation");
                    return;
                }

                String partition = DATE_FORMATTER.format(businessFromKline);

                databaseService.upsertMarketData(records, partition);

                // Update last transaction
                databaseService.updateLastTransaction(limit, partition);

                if (limit == BinanceService.LIMIT_CATCH_UP) {
                    // Candle boundary, not hour boundary: on a 15m run this fires four times
                    // an hour, on a 1h run once, exactly as before.
                    LocalDateTime currentCandle = TimeFrames.floorTo(businessFromKline, step);
                    if (lastSuccessfulFetchTime == null) {
                        // first run: set baseline, biasanya jangan trigger "candle baru"
                        lastSuccessfulFetchTime = currentCandle;
                        logger.info("There's no new data, no need to trigger indicator - lastSuccessfulFetchTime == null");
                    } else if (currentCandle.isAfter(lastSuccessfulFetchTime)) {
                        logger.info("--------------Triggering Port {}--------------",
                                appConfig.getTrigger().getPort());
                        triggerIndicator();

                        // update baseline ke candle terbaru (sekali per event)
                        lastSuccessfulFetchTime = currentCandle;
                    }
                }
                else {
                    logger.info("There's no new data, no need to trigger indicator - Else");
                }
                lastSuccessfulFetchTime = TimeFrames.floorTo(businessFromKline, step);

                // Trigger indicator if new data
                // if (!records.isEmpty()) {
                //     logger.info("--------------Triggering Port 8070--------------");
                //     triggerIndicator();
                // } else {
                //     logger.info("There's no new data, no need to trigger indicator.py");
                // }

                // Resource management
                loopCounter++;
                if (loopCounter % appConfig.getGcIntervalLoops() == 0) {
                    try {
                        resourceMonitor.forceGarbageCollection();
                        logger.info("Forced GC collect executed");
                    } catch (Exception e) {
                        logger.warn("GC collect failed: {}", e.getMessage());
                    }
                }
                resourceMonitor.sampleResourcesAndMaybeExit();

            } catch (Exception e) {
                logger.error("Error Main Loop, because of: {}", e.getMessage(), e);
            } finally {
                long cycleDurationMs = System.currentTimeMillis() - loopStartMs;
                long sleepSeconds = appConfig.getRefreshRateSeconds();
                logger.info("Sleeping {} seconds before next cycle... (cycle took {}ms)", 
                        sleepSeconds, cycleDurationMs);
            }
        }
    }

    private Map<Long, JsonObject> buildTimestampMap(List<JsonElement> items, String timestampField) {
        Map<Long, JsonObject> map = new HashMap<>();
        for (JsonElement item : items) {
            JsonObject obj = item.getAsJsonObject();
            if (obj.has(timestampField)) {
                long ts = obj.get(timestampField).getAsLong();
                // Normalize to milliseconds
                if (ts < 1_000_000_000_000L) {
                    ts = ts * 1000;
                }
                map.put(ts, obj);
            }
        }
        return map;
    }

    /**
     * Funding settlements keyed by settlement hour, so the rate in effect at any hour is the
     * floor entry for that hour.
     *
     * Keys are snapped to the nearest hour because Binance stamps roughly 44% of settlements a
     * few milliseconds late (observed max 47ms). Left unsnapped, the 08:00 settlement lands at
     * 08:00:00.007 and floorEntry(08:00) returns the previous settlement, shifting the whole
     * boundary one hour late.
     */
    private NavigableMap<Long, Double> buildFundingMap(List<JsonElement> items) {
        NavigableMap<Long, Double> map = new TreeMap<>();
        for (JsonElement item : items) {
            JsonObject obj = item.getAsJsonObject();
            if (obj.has("fundingTime") && obj.has("fundingRate")) {
                try {
                    long snapped = Math.round(obj.get("fundingTime").getAsLong() / 3_600_000.0) * 3_600_000L;
                    map.put(snapped, obj.get("fundingRate").getAsDouble());
                } catch (Exception e) {
                    logger.warn("Skipping malformed funding entry: {}", obj);
                }
            }
        }
        return map;
    }

    private List<DatabaseService.KlineRecord> buildRecords(List<JsonArray> klines,
                                                           Map<Long, JsonObject> oiMap,
                                                           Map<Long, JsonObject> lsMap,
                                                           NavigableMap<Long, Double> fundingMap) {
        List<DatabaseService.KlineRecord> records = new ArrayList<>();

        for (JsonArray kline : klines) {
            if (kline.size() < 12) continue;

            DatabaseService.KlineRecord record = new DatabaseService.KlineRecord();

            long openTimeMs = kline.get(0).getAsLong();
            LocalDateTime utcDateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(openTimeMs), UTC
            );
            LocalDateTime businessDate = utcDateTime.atZone(UTC).withZoneSameInstant(JAKARTA).toLocalDateTime();
            LocalDateTime insertDate = LocalDateTime.now(JAKARTA);

            record.utcDateTime = utcDateTime;
            record.businessDate = businessDate;
            record.unixDate = utcDateTime.atZone(UTC).toEpochSecond();
            record.kloseTime = kline.get(6).getAsLong() / 1000;
            record.insertDate = insertDate;

            record.openPrice = kline.get(1).getAsDouble();
            record.highPrice = kline.get(2).getAsDouble();
            record.lowPrice = kline.get(3).getAsDouble();
            record.closePrice = kline.get(4).getAsDouble();
            record.volume = kline.get(5).getAsDouble();
            record.quoteAssetVolume = kline.get(7).getAsDouble();
            record.numberOfTrades = kline.get(8).getAsLong();
            record.takerBuyBaseVolume = kline.get(9).getAsDouble();
            record.takerBuyQuoteVolume = kline.get(10).getAsDouble();

            // Calculate derived fields
            record.avgTradeSize = record.numberOfTrades != 0 
                    ? record.volume / record.numberOfTrades 
                    : 0;
            record.closePriceAdjusted = record.openPrice + 
                    ((record.closePrice - record.openPrice) * (1 - record.avgTradeSize));
            record.aggressiveBuyRatio = record.volume != 0 
                    ? record.takerBuyBaseVolume / record.volume 
                    : 0;
            record.priceEfficiency = (record.highPrice - record.lowPrice) != 0 
                    ? (record.closePrice - record.openPrice) / (record.highPrice - record.lowPrice) 
                    : 0;
            record.tradeImbalance = record.quoteAssetVolume != 0 
                    ? record.takerBuyQuoteVolume / record.quoteAssetVolume 
                    : 0;
            record.icebergScore = record.closePrice != record.openPrice 
                    ? (record.volume * record.numberOfTrades) / Math.abs(record.closePrice - record.openPrice) 
                    : 0;
            record.priceChange = record.openPrice != 0 
                    ? ((record.closePrice / record.openPrice) - 1) * 100 
                    : 0;

            if (record.aggressiveBuyRatio > 0.6 && record.tradeImbalance > 0.6 && record.priceEfficiency > 0.8) {
                record.upsideProbability = "HIGH_UPSIDE";
            } else if (record.aggressiveBuyRatio > 0.5) {
                record.upsideProbability = "MODERATE_UPSIDE";
            } else {
                record.upsideProbability = "NEUTRAL_OR_BEARISH";
            }

            // Join OI & LS
            JsonObject oiData = oiMap.get(openTimeMs);
            if (oiData != null) {
                record.sumOpenInterest = oiData.has("sumOpenInterest") 
                        ? oiData.get("sumOpenInterest").getAsDouble() : null;
                record.sumOpenInterestValue = oiData.has("sumOpenInterestValue") 
                        ? oiData.get("sumOpenInterestValue").getAsDouble() : null;
                record.cmcCirculatingSupply = oiData.has("CMCCirculatingSupply") 
                        ? oiData.get("CMCCirculatingSupply").getAsDouble() : null;
            }

            // Forward-fill: the rate in effect for this hour is the most recent settlement
            // at or before the hour's open.
            Map.Entry<Long, Double> fundingEntry = fundingMap.floorEntry(openTimeMs);
            if (fundingEntry != null) {
                record.fundingRate = fundingEntry.getValue();
            }

            JsonObject lsData = lsMap.get(openTimeMs);
            if (lsData != null) {
                record.longAccount = lsData.has("longAccount") 
                        ? lsData.get("longAccount").getAsDouble() : null;
                record.lsRatio = lsData.has("longShortRatio") 
                        ? lsData.get("longShortRatio").getAsDouble() : null;
                record.shortAccount = lsData.has("shortAccount")
                        ? lsData.get("shortAccount").getAsDouble() : null;
            }

            // Depth averaged over this candle by the 30s sampler; null for candles it has not
            // observed (catch-up rows, or before startup), which the archive sync fills later.
            record.depth = depthBuffer.statsFor(openTimeMs);

            records.add(record);
        }

        return records;
    }

    private void triggerIndicator() {
        try (Socket socket = new Socket(appConfig.getTrigger().getHost(), appConfig.getTrigger().getPort())) {
            socket.getOutputStream().write("ping\n".getBytes());
            socket.getOutputStream().flush();
            logger.info("[TriggerClient] Sent trigger to indicator service");
        } catch (Exception e) {
            logger.warn("[TriggerClient] Failed to trigger indicator: {}", e.getMessage());
        }
    }
}
