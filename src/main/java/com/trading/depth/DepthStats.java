package com.trading.depth;

/**
 * Per-candle depth: the average of every snapshot that fell inside the candle, per band,
 * plus how many snapshots there were. A full 1h candle has ~120 (one every 30 seconds), so a
 * lower count marks a candle that was only partly observed, e.g. across a restart.
 */
public final class DepthStats {
    public final Double[] bid = new Double[DepthBands.COUNT];
    public final Double[] ask = new Double[DepthBands.COUNT];
    public Integer samples;
}
