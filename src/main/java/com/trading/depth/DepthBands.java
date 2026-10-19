package com.trading.depth;

import java.util.List;

/**
 * Order book depth bands: cumulative notional resting within N percent of the mid price.
 *
 * The bands mirror data.binance.vision's futures bookDepth archive, which publishes the same
 * figure every ~30 seconds for +-0.2, 1, 2, 3, 4 and 5 percent (negative = bid side). Using
 * the archive's own bands keeps realtime values and backfilled values comparable.
 *
 * Two archive quirks the callers rely on this class to absorb:
 *   - the +-0.2% band only exists from 2026-01-16; older files carry 1..5% only
 *   - percentages are written "-5" in older files and "-5.00" in newer ones
 */
public final class DepthBands {

    /** Band widths in percent of the mid price, in column order. */
    public static final double[] PERCENTS = {0.2, 1, 2, 3, 4, 5};
    public static final int COUNT = PERCENTS.length;

    private static final String[] LABELS = {"0_2", "1", "2", "3", "4", "5"};

    public static final String SAMPLES_COLUMN = "DEPTH_SAMPLES";

    private DepthBands() {
    }

    public static String bidColumn(int band) {
        return "BID_NOTIONAL_" + LABELS[band] + "PCT";
    }

    public static String askColumn(int band) {
        return "ASK_NOTIONAL_" + LABELS[band] + "PCT";
    }

    /** All 13 depth columns, bid/ask interleaved per band, samples last -- the binding order. */
    public static String columnList() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < COUNT; i++) {
            sb.append(bidColumn(i)).append(", ").append(askColumn(i)).append(", ");
        }
        return sb.append(SAMPLES_COLUMN).toString();
    }

    /** Number of values columnList() binds. */
    public static int columnCount() {
        return COUNT * 2 + 1;
    }

    /** Band index for an archive percentage such as 0.2 or 5, or -1 for an unknown band. */
    public static int indexOf(double absPercent) {
        for (int i = 0; i < COUNT; i++) {
            if (Math.abs(absPercent - PERCENTS[i]) < 1e-6) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Computes every band from a live order book snapshot.
     *
     * A band is only reported when the book actually reaches its boundary. /fapi/v1/depth is
     * capped at 1000 levels, which for ETHUSDT covers roughly +-0.45% of the price; summing a
     * 5% band over that would silently under-report it, so such bands come back null instead.
     * A side returning fewer levels than requested is the whole book and is never truncated.
     *
     * @param bids           [price, qty] pairs, best (highest) first
     * @param asks           [price, qty] pairs, best (lowest) first
     * @param requestedLimit the limit the snapshot was requested with
     */
    public static DepthSnapshot fromOrderBook(List<double[]> bids, List<double[]> asks, int requestedLimit) {
        DepthSnapshot snapshot = new DepthSnapshot();
        if (bids.isEmpty() || asks.isEmpty()) {
            return snapshot;
        }
        double mid = (bids.get(0)[0] + asks.get(0)[0]) / 2.0;
        boolean bidsTruncated = bids.size() >= requestedLimit;
        boolean asksTruncated = asks.size() >= requestedLimit;
        double lowestBid = bids.get(bids.size() - 1)[0];
        double highestAsk = asks.get(asks.size() - 1)[0];

        for (int band = 0; band < COUNT; band++) {
            double lower = mid * (1 - PERCENTS[band] / 100.0);
            double upper = mid * (1 + PERCENTS[band] / 100.0);

            if (!bidsTruncated || lowestBid <= lower) {
                double sum = 0;
                for (double[] level : bids) {
                    if (level[0] < lower) break;
                    sum += level[0] * level[1];
                }
                snapshot.bid[band] = sum;
            }
            if (!asksTruncated || highestAsk >= upper) {
                double sum = 0;
                for (double[] level : asks) {
                    if (level[0] > upper) break;
                    sum += level[0] * level[1];
                }
                snapshot.ask[band] = sum;
            }
        }
        return snapshot;
    }
}
