package com.trading.depth;

import com.trading.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-candle accumulator between the depth sampler (writer) and the fetch loop (reader).
 *
 * Keyed by candle open time, so a candle that has closed stops receiving samples the moment
 * the next one opens. That matters because the fetch loop upserts the just-closed candle
 * once more after it closes; reading by key means that last write carries the candle's own
 * final average, never a snapshot taken after it closed.
 *
 * Lives in memory only. A restart loses the candle in progress, which then shows a low
 * DEPTH_SAMPLES count until the archive sync replaces it the next day.
 */
@Component
public class RealtimeDepthBuffer {

    private static final Logger logger = LoggerFactory.getLogger(RealtimeDepthBuffer.class);

    /** The archive's cadence. One sample per slot, however often record() is called. */
    public static final long SLOT_MS = 30_000L;

    /** Candles kept after they close, so the late final upsert still finds them. */
    private static final int KEEP_CANDLES = 3;

    private final AppConfig appConfig;
    private final Map<Long, DepthAggregator> byCandle = new ConcurrentHashMap<>();
    private long lastSlot = Long.MIN_VALUE;
    private long currentCandle = Long.MIN_VALUE;

    public RealtimeDepthBuffer(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    /**
     * Adds one live snapshot taken at sampleMs (the book's own transaction time).
     * Returns false when the slot already has a sample and this one was dropped.
     */
    public synchronized boolean record(long sampleMs, DepthSnapshot snapshot) {
        long slot = Math.floorDiv(sampleMs, SLOT_MS);
        if (slot == lastSlot) {
            return false;
        }
        lastSlot = slot;

        long stepMs = stepMs();
        long candle = Math.floorDiv(sampleMs, stepMs) * stepMs;
        if (candle != currentCandle) {
            logClosedCandle(currentCandle);
            currentCandle = candle;
            byCandle.keySet().removeIf(open -> open < candle - KEEP_CANDLES * stepMs);
        }
        byCandle.computeIfAbsent(candle, k -> new DepthAggregator()).add(snapshot);
        return true;
    }

    /** Average so far for the candle opening at candleOpenMs, or null if none was observed. */
    public DepthStats statsFor(long candleOpenMs) {
        DepthAggregator aggregator = byCandle.get(candleOpenMs);
        return aggregator == null ? null : aggregator.stats();
    }

    private void logClosedCandle(long candleOpenMs) {
        DepthAggregator closed = byCandle.get(candleOpenMs);
        if (closed == null) {
            return;
        }
        DepthStats s = closed.stats();
        logger.info("[Depth] candle {} UTC closed: {} samples, +-0.2% bid={} ask={}",
                LocalDateTime.ofInstant(Instant.ofEpochMilli(candleOpenMs), ZoneOffset.UTC),
                s.samples, fmt(s.bid[0]), fmt(s.ask[0]));
    }

    private long stepMs() {
        return appConfig.getBinance().intervalDuration().toMillis();
    }

    private static String fmt(Double v) {
        return v == null ? "null" : String.format("%,.0f", v);
    }
}
