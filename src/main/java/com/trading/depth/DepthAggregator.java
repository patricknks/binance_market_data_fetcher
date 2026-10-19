package com.trading.depth;

import java.util.Map;

/**
 * Running per-band average over the depth snapshots of one candle.
 *
 * Each band keeps its own count because a band can be missing from individual snapshots
 * (archive files before 2026-01-16 have no +-0.2% band; a live snapshot may not reach the
 * outer bands), and averaging those as zero would drag the figure down.
 *
 * Synchronized because the realtime sampler adds from one scheduler thread while the fetch
 * loop reads from another.
 */
public final class DepthAggregator {

    private final double[] bidSum = new double[DepthBands.COUNT];
    private final double[] askSum = new double[DepthBands.COUNT];
    private final int[] bidCount = new int[DepthBands.COUNT];
    private final int[] askCount = new int[DepthBands.COUNT];
    private int samples;

    public synchronized void add(DepthSnapshot snapshot) {
        if (snapshot == null || snapshot.isEmpty()) {
            return;
        }
        for (int i = 0; i < DepthBands.COUNT; i++) {
            if (snapshot.bid[i] != null) {
                bidSum[i] += snapshot.bid[i];
                bidCount[i]++;
            }
            if (snapshot.ask[i] != null) {
                askSum[i] += snapshot.ask[i];
                askCount[i]++;
            }
        }
        samples++;
    }

    public synchronized DepthStats stats() {
        DepthStats stats = new DepthStats();
        for (int i = 0; i < DepthBands.COUNT; i++) {
            stats.bid[i] = bidCount[i] == 0 ? null : bidSum[i] / bidCount[i];
            stats.ask[i] = askCount[i] == 0 ? null : askSum[i] / askCount[i];
        }
        stats.samples = samples == 0 ? null : samples;
        return stats;
    }

    public synchronized int samples() {
        return samples;
    }

    /**
     * Buckets timestamped snapshots into candles: a snapshot belongs to the candle whose
     * [open, open + step) contains its timestamp. Candles are aligned to the epoch, which
     * matches Binance for every interval up to 1d (weekly candles open on Mondays and would
     * not line up).
     */
    public static void addAll(Map<Long, DepthAggregator> target, Map<Long, DepthSnapshot> snapshots, long stepMs) {
        for (Map.Entry<Long, DepthSnapshot> e : snapshots.entrySet()) {
            long candleOpen = Math.floorDiv(e.getKey(), stepMs) * stepMs;
            target.computeIfAbsent(candleOpen, k -> new DepthAggregator()).add(e.getValue());
        }
    }
}
