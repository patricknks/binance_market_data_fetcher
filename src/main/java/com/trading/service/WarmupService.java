package com.trading.service;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.trading.config.AppConfig;
import com.trading.depth.DepthAggregator;
import com.trading.util.TimeFrames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Historical backfill from data.binance.vision, used when the table is empty or has fallen
 * far behind. Once the data reaches within stale-threshold-days of now, the existing
 * realtime loop takes over and this service stays out of the way.
 *
 * OHLCV comes from the spot archives (matching the realtime klines endpoint) and the open
 * interest / long-short columns come from the futures UM metrics archives.
 */
@Service
public class WarmupService {

    private static final Logger logger = LoggerFactory.getLogger(WarmupService.class);
    private static final ZoneId UTC = ZoneId.of("UTC");
    private static final ZoneId JAKARTA = ZoneId.of("Asia/Jakarta");

    private final AppConfig appConfig;
    private final BinanceVisionService visionService;
    private final DatabaseService databaseService;
    private final BinanceService binanceService;

    public WarmupService(AppConfig appConfig, BinanceVisionService visionService,
                         DatabaseService databaseService, BinanceService binanceService) {
        this.appConfig = appConfig;
        this.visionService = visionService;
        this.databaseService = databaseService;
        this.binanceService = binanceService;
    }

    /**
     * Warmup is due when there is no data at all, or the newest row is older than
     * stale-threshold-days.
     */
    public boolean shouldWarmup() {
        if (!appConfig.getWarmup().isEnabled()) {
            return false;
        }
        LocalDateTime max = databaseService.getMaxBusinessDate();
        if (max == null) {
            logger.info("[Warmup] Table is empty, warmup mode");
            return true;
        }
        LocalDateTime threshold = LocalDateTime.now(JAKARTA)
                .minusDays(appConfig.getWarmup().getStaleThresholdDays());
        boolean stale = max.isBefore(threshold);
        if (stale) {
            logger.info("[Warmup] max(business_date)={} is older than {}, warmup mode", max, threshold);
        }
        return stale;
    }

    /**
     * Fills history up to stale-threshold-days before now.
     *
     * @return true when rows were written. False means there is nothing left to fill, and
     *         the caller must fall through to realtime in the same tick -- otherwise the
     *         moving cutoff would re-trigger warmup forever and realtime would never run.
     */
    public boolean runWarmup() {
        AppConfig.Warmup cfg = appConfig.getWarmup();
        Duration step = appConfig.getBinance().intervalDuration();
        databaseService.ensureApplicationTimeSeed();

        LocalDateTime cutoffUtc = TimeFrames.floorTo(
                LocalDateTime.now(UTC).minusDays(cfg.getStaleThresholdDays()), step);

        // Resume one candle after the newest row, not one hour: on a 15m run the old
        // hour-wide step would skip three candles on every resume.
        LocalDateTime max = databaseService.getMaxBusinessDate();
        LocalDateTime startUtc = (max == null)
                ? cfg.getStartDate().atStartOfDay()
                : max.atZone(JAKARTA).withZoneSameInstant(UTC).toLocalDateTime().plus(step);

        if (!startUtc.isBefore(cutoffUtc)) {
            logger.info("[Warmup] Nothing to fill (start {} >= cutoff {}), handing over to realtime",
                    startUtc, cutoffUtc);
            return false;
        }

        logger.info("[Warmup] ===== WARMUP MODE: {} -> {} (UTC) =====", startUtc, cutoffUtc);

        // The funding endpoint serves full history and the whole series is only a few thousand
        // entries, so fetch it once here rather than per month. Reaching back one settlement
        // interval keeps the forward-fill correct for the very first hour.
        NavigableMap<Long, Double> fundingMap = buildFundingMap(
                binanceService.fetchFundingRate(startUtc.minusHours(17), cutoffUtc));
        logger.info("[Warmup] Loaded {} funding settlements", fundingMap.size());

        long totalRows = 0;
        YearMonth month = YearMonth.from(startUtc);
        YearMonth lastMonth = YearMonth.from(cutoffUtc);

        while (!month.isAfter(lastMonth)) {
            try {
                totalRows += processMonth(month, startUtc, cutoffUtc, fundingMap);
            } catch (Exception e) {
                logger.error("[Warmup] Month {} failed: {}", month, e.getMessage(), e);
            }
            month = month.plusMonths(1);
        }

        logger.info("[Warmup] ===== DONE, {} rows written =====", totalRows);
        return totalRows > 0;
    }

    /**
     * Same keying as the realtime path: settlement hour -> rate, resolved with floorEntry.
     * Keys are snapped to the nearest hour because Binance stamps many settlements a few
     * milliseconds late, which would otherwise push the forward-fill boundary an hour late.
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
                    logger.warn("[Warmup] Skipping malformed funding entry: {}", obj);
                }
            }
        }
        return map;
    }

    private long processMonth(YearMonth month, LocalDateTime startUtc, LocalDateTime cutoffUtc,
                              NavigableMap<Long, Double> fundingMap) {
        List<BinanceVisionService.KlineRow> klines = visionService.fetchKlinesMonth(month);

        if (klines.isEmpty() && (needsDailyFallback(month) || hasDailyData(month, cutoffUtc))) {
            // No monthly archive: either it is not published yet, or the month straddles the
            // first day this symbol traded. Both are only reachable through the dailies.
            klines = fetchMonthFromDailies(month, cutoffUtc);
        }
        if (klines.isEmpty()) {
            logger.info("[Warmup] {} has no archive data, skipping", month);
            return 0;
        }

        // Keep only the candles we actually still need.
        List<BinanceVisionService.KlineRow> wanted = new ArrayList<>();
        for (BinanceVisionService.KlineRow k : klines) {
            LocalDateTime utc = LocalDateTime.ofInstant(Instant.ofEpochMilli(k.openTimeMs), UTC);
            if (!utc.isBefore(startUtc) && utc.isBefore(cutoffUtc)) {
                wanted.add(k);
            }
        }
        if (wanted.isEmpty()) {
            logger.info("[Warmup] {} has no candles inside the target window, skipping", month);
            return 0;
        }

        Map<LocalDateTime, BinanceVisionService.MetricsRow> metrics = loadMetrics(month);
        Map<Long, DepthAggregator> depth = loadDepth(month, cutoffUtc);
        List<DatabaseService.KlineRecord> records = new ArrayList<>(wanted.size());
        int withMetrics = 0;
        int withDepth = 0;
        for (BinanceVisionService.KlineRow k : wanted) {
            DatabaseService.KlineRecord record = buildRecord(k, metrics, fundingMap);
            if (record.sumOpenInterest != null) {
                withMetrics++;
            }
            DepthAggregator candleDepth = depth.get(k.openTimeMs);
            if (candleDepth != null) {
                record.depth = candleDepth.stats();
                withDepth++;
            }
            records.add(record);
        }
        // A whole month of samples that matches nothing means the offset is wrong for this
        // timeframe, which would otherwise show up only as silently null OI columns.
        if (!metrics.isEmpty() && withMetrics == 0) {
            logger.warn("[Warmup] {}: loaded {} metrics samples but none lined up with a candle open. "
                            + "Check app.warmup.metrics-offset-minutes (currently {}) for interval {}.",
                    month, metrics.size(), appConfig.getWarmup().getMetricsOffsetMinutes(),
                    appConfig.getBinance().normalisedInterval());
        }

        if (appConfig.getWarmup().isAutoCreatePartition()) {
            Set<YearMonth> partitions = new LinkedHashSet<>();
            for (DatabaseService.KlineRecord r : records) {
                partitions.add(YearMonth.from(r.businessDate));
            }
            for (YearMonth p : partitions) {
                databaseService.ensurePartition(p);
            }
        }

        int written = databaseService.batchUpsertMarketData(records);

        // Keep application time moving so realtime resumes from the right hour.
        BinanceVisionService.KlineRow last = wanted.get(wanted.size() - 1);
        databaseService.updateApplicationTime(last.openTimeMs);

        logger.info("[Warmup] {} -> {} rows written ({} with metrics, of {} samples available; {} with depth)",
                month, written, withMetrics, metrics.size(), withDepth);
        return written;
    }

    /**
     * Monthly archives are published a few days after a month closes, so only the current and
     * previous month can legitimately be missing one. An older month with no monthly archive
     * simply has no data -- for ETHUSDT.P that is every month before the perpetual launched --
     * and probing its 30 dailies just burns 30 requests per month on guaranteed 404s.
     */
    private boolean needsDailyFallback(YearMonth month) {
        return !month.isBefore(YearMonth.from(LocalDate.now(UTC)).minusMonths(1));
    }

    /**
     * Cheap probe of the month's last day, used to tell "this symbol had not launched yet"
     * apart from "trading started partway through this month". Archive coverage only ever
     * starts and then continues, so the last day is a reliable indicator, and this costs one
     * request per empty month instead of a full 30-day sweep.
     */
    private boolean hasDailyData(YearMonth month, LocalDateTime cutoffUtc) {
        LocalDate probe = month.atEndOfMonth();
        LocalDate cutoffDay = cutoffUtc.toLocalDate();
        if (probe.isAfter(cutoffDay)) {
            probe = cutoffDay;
        }
        if (probe.isBefore(month.atDay(1))) {
            return false;
        }
        return !visionService.fetchKlinesDay(probe).isEmpty();
    }

    private List<BinanceVisionService.KlineRow> fetchMonthFromDailies(YearMonth month, LocalDateTime cutoffUtc) {
        List<BinanceVisionService.KlineRow> all = new ArrayList<>();
        LocalDate day = month.atDay(1);
        LocalDate end = month.atEndOfMonth();
        LocalDate cutoffDay = cutoffUtc.toLocalDate();
        if (end.isAfter(cutoffDay)) {
            end = cutoffDay;
        }
        while (!day.isAfter(end)) {
            all.addAll(visionService.fetchKlinesDay(day));
            day = day.plusDays(1);
        }
        return all;
    }

    /**
     * Loads futures metrics covering the month, keyed by sample time.
     *
     * The previous day is included on purpose: the value a REST call reports for candle open
     * T is the archive sample metrics-offset-minutes earlier, so 00:00 on the 1st needs 23:55
     * from the last day of the previous month.
     */
    private Map<LocalDateTime, BinanceVisionService.MetricsRow> loadMetrics(YearMonth month) {
        Map<LocalDateTime, BinanceVisionService.MetricsRow> all = new HashMap<>();
        LocalDate metricsStart = appConfig.getWarmup().getMetricsStartDate();

        LocalDate day = month.atDay(1).minusDays(1);
        LocalDate end = month.atEndOfMonth();
        LocalDate today = LocalDate.now(UTC);
        if (end.isAfter(today)) {
            end = today;
        }
        while (!day.isAfter(end)) {
            if (!day.isBefore(metricsStart)) {
                all.putAll(visionService.fetchMetricsDay(day));
            }
            day = day.plusDays(1);
        }
        return all;
    }

    /**
     * Order book depth for the month's candles, from the daily bookDepth archive, keyed by
     * candle open (epoch ms). Empty before app.depth.start-date, when the archive does not
     * exist. Stops at the cutoff day: later candles are written by the realtime loop and get
     * their depth from the archive sync instead.
     */
    private Map<Long, DepthAggregator> loadDepth(YearMonth month, LocalDateTime cutoffUtc) {
        Map<Long, DepthAggregator> byCandle = new HashMap<>();
        AppConfig.Depth cfg = appConfig.getDepth();
        if (!cfg.isEnabled()) {
            return byCandle;
        }
        long stepMs = appConfig.getBinance().intervalDuration().toMillis();
        LocalDate day = month.atDay(1);
        LocalDate end = month.atEndOfMonth();
        LocalDate cutoffDay = cutoffUtc.toLocalDate();
        if (end.isAfter(cutoffDay)) {
            end = cutoffDay;
        }
        if (day.isBefore(cfg.getStartDate())) {
            day = cfg.getStartDate();
        }
        while (!day.isAfter(end)) {
            DepthAggregator.addAll(byCandle, visionService.fetchBookDepthDay(day), stepMs);
            day = day.plusDays(1);
        }
        return byCandle;
    }

    /**
     * Mirrors the realtime record building in DataFetcherScheduler.buildRecords so warmup
     * rows are computed identically. Kept as a separate copy because the realtime path must
     * not be modified.
     */
    private DatabaseService.KlineRecord buildRecord(
            BinanceVisionService.KlineRow k,
            Map<LocalDateTime, BinanceVisionService.MetricsRow> metrics,
            NavigableMap<Long, Double> fundingMap) {

        DatabaseService.KlineRecord record = new DatabaseService.KlineRecord();

        LocalDateTime utcDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(k.openTimeMs), UTC);
        record.utcDateTime = utcDateTime;
        record.businessDate = utcDateTime.atZone(UTC).withZoneSameInstant(JAKARTA).toLocalDateTime();
        record.unixDate = utcDateTime.atZone(UTC).toEpochSecond();
        record.kloseTime = k.closeTimeMs / 1000;
        record.insertDate = LocalDateTime.now(JAKARTA);

        record.openPrice = k.openPrice;
        record.highPrice = k.highPrice;
        record.lowPrice = k.lowPrice;
        record.closePrice = k.closePrice;
        record.volume = k.volume;
        record.quoteAssetVolume = k.quoteAssetVolume;
        record.numberOfTrades = k.numberOfTrades;
        record.takerBuyBaseVolume = k.takerBuyBaseVolume;
        record.takerBuyQuoteVolume = k.takerBuyQuoteVolume;

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

        // The REST value for candle open T equals the archive sample taken a fixed offset
        // earlier -- 5 minutes on 1h, verified against live data. Configurable because that
        // offset has not been verified for every timeframe; see metrics-offset-minutes.
        BinanceVisionService.MetricsRow m = metrics.get(
                utcDateTime.minusMinutes(appConfig.getWarmup().getMetricsOffsetMinutes()));
        if (m != null) {
            record.sumOpenInterest = m.sumOpenInterest;
            record.sumOpenInterestValue = m.sumOpenInterestValue;
            if (m.topTraderPositionRatio != null && m.topTraderPositionRatio > 0) {
                double r = m.topTraderPositionRatio;
                record.lsRatio = r;
                record.longAccount = r / (1 + r);
                record.shortAccount = 1 / (1 + r);
            }
        }
        // Forward-fill: the rate in effect for this hour is the most recent settlement at or
        // before the hour's open. Null before the perpetual launched (2019-11-27).
        Map.Entry<Long, Double> fundingEntry = fundingMap.floorEntry(k.openTimeMs);
        if (fundingEntry != null) {
            record.fundingRate = fundingEntry.getValue();
        }

        // cmc_circulating_sply has no archive source and stays null for warmup rows.

        return record;
    }
}
