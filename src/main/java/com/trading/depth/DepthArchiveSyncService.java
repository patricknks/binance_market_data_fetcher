package com.trading.depth;

import com.trading.config.AppConfig;
import com.trading.service.BinanceVisionService;
import com.trading.service.DatabaseService;
import com.trading.service.WarmupService;
import com.trading.util.TimeFrames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fills depth from the bookDepth archive once a day's file is published (the next day).
 *
 * Covers what neither warmup nor the realtime sampler can:
 *   - the 1..5% bands, which the 1000-level live book does not reach
 *   - every candle between warmup's cutoff and now, which the realtime loop catches up on
 *     without any depth at all
 *   - candles the sampler only partly saw, e.g. across a restart
 * and makes the archive the source of truth for anything older than a day: the provisional
 * realtime +-0.2% values are replaced, after logging how far they were from the archive.
 *
 * A day counts as done once all of its candles exist and carry archive bands. Days are
 * remembered in memory, so after a restart the first pass re-checks the lookback window
 * with one cheap count query per day.
 */
@Component
public class DepthArchiveSyncService {

    private static final Logger logger = LoggerFactory.getLogger(DepthArchiveSyncService.class);

    /** A file still missing this many days later is not coming; stop asking for it. */
    private static final int GIVE_UP_AFTER_DAYS = 3;

    private static final long TICK_MS = 60_000L;
    private static final ZoneId JAKARTA = ZoneId.of("Asia/Jakarta");

    private final AppConfig appConfig;
    private final BinanceVisionService visionService;
    private final DatabaseService databaseService;
    private final WarmupService warmupService;
    private final Set<LocalDate> doneDays = ConcurrentHashMap.newKeySet();
    /** 0 = due now; the first tick comes after sync-initial-delay-seconds. */
    private volatile long nextPassAtMs = 0;
    /** Why the last tick deferred the pass, so the reason is logged once, not every minute. */
    private String deferredReason;

    public DepthArchiveSyncService(AppConfig appConfig, BinanceVisionService visionService,
                                   DatabaseService databaseService, WarmupService warmupService) {
        this.appConfig = appConfig;
        this.visionService = visionService;
        this.databaseService = databaseService;
        this.warmupService = warmupService;
    }

    /**
     * Checks once a minute whether a pass is due; passes themselves run every
     * sync-interval-minutes. A pass that cannot run yet (warmup, or realtime still catching
     * up) stays due, so it runs within a minute of the handover instead of a whole interval
     * later. Waiting the full interval left every candle between warmup's cutoff and now
     * without depth for up to an hour after warmup -- or for good, if the app was stopped
     * in the meantime.
     */
    @Scheduled(initialDelayString = "#{@appConfig.depth.syncInitialDelaySeconds * 1000}",
               fixedDelay = TICK_MS)
    public void tick() {
        AppConfig.Depth cfg = appConfig.getDepth();
        if (!cfg.isEnabled() || System.currentTimeMillis() < nextPassAtMs) {
            return;
        }
        // Warmup writes archive depth for everything it backfills; running alongside it would
        // only race it for the same rows.
        if (warmupService.shouldWarmup()) {
            logDeferred("warmup in progress");
            return;
        }
        // After warmup the realtime loop catches up the remaining candles in chunks; a pass
        // in the middle of that would only see part of each day and leave the rest for the
        // next interval.
        if (!realtimeCaughtUp()) {
            logDeferred("realtime still catching up");
            return;
        }
        deferredReason = null;
        sync();
        nextPassAtMs = System.currentTimeMillis() + cfg.getSyncIntervalMinutes() * 60_000L;
    }

    private void logDeferred(String reason) {
        if (!reason.equals(deferredReason)) {
            logger.info("[DepthSync] {}, pass deferred until it is done", reason);
            deferredReason = reason;
        }
    }

    /** True once the newest row is the current candle or the one just before it. */
    private boolean realtimeCaughtUp() {
        LocalDateTime max = databaseService.getMaxBusinessDate();
        if (max == null) {
            return false;
        }
        Duration step = appConfig.getBinance().intervalDuration();
        LocalDateTime currentCandle = TimeFrames.floorTo(LocalDateTime.now(JAKARTA), step);
        return !max.isBefore(currentCandle.minus(step));
    }

    void sync() {
        AppConfig.Depth cfg = appConfig.getDepth();
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        LocalDate from = today.minusDays(cfg.getSyncLookbackDays());
        if (from.isBefore(cfg.getStartDate())) {
            from = cfg.getStartDate();
        }

        int synced = 0;
        for (LocalDate day = from; day.isBefore(today); day = day.plusDays(1)) {
            if (doneDays.contains(day)) {
                continue;
            }
            try {
                if (syncDay(day, today)) {
                    synced++;
                }
            } catch (Exception e) {
                logger.warn("[DepthSync] {} failed: {}", day, e.getMessage());
            }
        }
        logger.info("[DepthSync] pass done: {} day(s) updated, window {} .. {}", synced, from, today.minusDays(1));
    }

    /** @return true when rows were updated */
    private boolean syncDay(LocalDate day, LocalDate today) {
        LocalDateTime start = day.atStartOfDay();
        LocalDateTime end = start.plusDays(1);
        long[] coverage = databaseService.countDepthCoverage(start, end);
        long rows = coverage[0];
        long missing = coverage[1];
        long expected = expectedCandlesPerDay();

        if (rows == 0) {
            return false; // realtime has not written this day yet; check again next pass
        }
        if (missing == 0) {
            if (rows >= expected) {
                doneDays.add(day);
            }
            return false;
        }

        Map<Long, DepthSnapshot> snapshots = visionService.fetchBookDepthDay(day);
        if (snapshots.isEmpty()) {
            if (day.isBefore(today.minusDays(GIVE_UP_AFTER_DAYS))) {
                logger.warn("[DepthSync] {}: no bookDepth archive {} days on, giving up on this day",
                        day, GIVE_UP_AFTER_DAYS);
                doneDays.add(day);
            } else {
                logger.info("[DepthSync] {}: bookDepth archive not published yet", day);
            }
            return false;
        }

        long stepMs = appConfig.getBinance().intervalDuration().toMillis();
        Map<Long, DepthAggregator> byCandle = new HashMap<>();
        DepthAggregator.addAll(byCandle, snapshots, stepMs);
        Map<Long, DepthStats> stats = new HashMap<>();
        for (Map.Entry<Long, DepthAggregator> e : byCandle.entrySet()) {
            stats.put(e.getKey(), e.getValue().stats());
        }

        logRealtimeVsArchive(day, databaseService.loadRealtimeNearBand(start, end), stats);
        int updated = databaseService.updateDepth(stats);
        logger.info("[DepthSync] {}: {} snapshots -> {} candles, {} rows updated ({} rows were missing bands)",
                day, snapshots.size(), stats.size(), updated, missing);

        if (rows >= expected) {
            doneDays.add(day);
        }
        return updated > 0;
    }

    /**
     * The REST-derived +-0.2% figure and the archive's are computed independently (live book
     * with a mid-price boundary vs. Binance's own aggregation), so this is the running check
     * that the two definitions agree. Mean absolute difference, as a fraction of the archive
     * value; a few percent is sampling noise, much more means the definitions differ.
     */
    private void logRealtimeVsArchive(LocalDate day, Map<Long, double[]> realtime, Map<Long, DepthStats> archive) {
        double bidDiff = 0, askDiff = 0;
        int n = 0;
        for (Map.Entry<Long, double[]> e : realtime.entrySet()) {
            DepthStats a = archive.get(e.getKey());
            if (a == null || a.bid[0] == null || a.ask[0] == null || a.bid[0] == 0 || a.ask[0] == 0) {
                continue;
            }
            bidDiff += Math.abs(e.getValue()[0] - a.bid[0]) / a.bid[0];
            askDiff += Math.abs(e.getValue()[1] - a.ask[0]) / a.ask[0];
            n++;
        }
        if (n > 0) {
            logger.info("[DepthSync] {}: realtime vs archive +-0.2% over {} candles: bid mean abs diff {}%, ask {}%",
                    day, n, String.format("%.2f", 100 * bidDiff / n), String.format("%.2f", 100 * askDiff / n));
        }
    }

    private long expectedCandlesPerDay() {
        long stepMs = appConfig.getBinance().intervalDuration().toMillis();
        return Math.max(1, 86_400_000L / stepMs);
    }
}
