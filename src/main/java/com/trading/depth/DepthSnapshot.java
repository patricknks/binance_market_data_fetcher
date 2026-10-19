package com.trading.depth;

/**
 * One order book depth observation: notional per band, bid and ask side.
 * A null entry means the band is not available in this observation (not in the archive
 * file, or beyond what the live snapshot reached), which is different from zero depth.
 */
public final class DepthSnapshot {
    public final Double[] bid = new Double[DepthBands.COUNT];
    public final Double[] ask = new Double[DepthBands.COUNT];

    public boolean isEmpty() {
        for (int i = 0; i < DepthBands.COUNT; i++) {
            if (bid[i] != null || ask[i] != null) {
                return false;
            }
        }
        return true;
    }
}
