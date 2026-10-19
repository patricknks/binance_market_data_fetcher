package com.trading.depth;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DepthBandsTest {

    /** A side of `levels` price levels, one tick apart from `best`, 1 unit each. */
    private static List<double[]> side(double best, double tick, int levels) {
        List<double[]> out = new ArrayList<>();
        for (int i = 0; i < levels; i++) {
            out.add(new double[]{best + i * tick, 1.0});
        }
        return out;
    }

    @Test
    void columnsAreInBindingOrder() {
        assertEquals("BID_NOTIONAL_0_2PCT, ASK_NOTIONAL_0_2PCT, BID_NOTIONAL_1PCT, ASK_NOTIONAL_1PCT, "
                        + "BID_NOTIONAL_2PCT, ASK_NOTIONAL_2PCT, BID_NOTIONAL_3PCT, ASK_NOTIONAL_3PCT, "
                        + "BID_NOTIONAL_4PCT, ASK_NOTIONAL_4PCT, BID_NOTIONAL_5PCT, ASK_NOTIONAL_5PCT, DEPTH_SAMPLES",
                DepthBands.columnList());
        assertEquals(13, DepthBands.columnCount());
    }

    private static double notionalWithin(List<double[]> levels, double lo, double hi) {
        double sum = 0;
        for (double[] l : levels) {
            if (l[0] >= lo && l[0] <= hi) sum += l[0] * l[1];
        }
        return sum;
    }

    @Test
    void sumsNotionalInsideEachBandAndNullsBandsTheBookDoesNotReach() {
        // mid = 1000.00. A full 1000-level book with a 0.011 step reaches ~989.0 / ~1011.0,
        // i.e. a little past 1% and well short of 2% -- the same shape as ETHUSDT's ~0.45%.
        List<double[]> bids = side(999.99, -0.011, 1000);
        List<double[]> asks = side(1000.01, 0.011, 1000);
        DepthSnapshot s = DepthBands.fromOrderBook(bids, asks, 1000);

        assertEquals(notionalWithin(bids, 998.0, 1000.0), s.bid[0], 1e-6);
        assertEquals(notionalWithin(asks, 1000.0, 1002.0), s.ask[0], 1e-6);
        assertEquals(notionalWithin(bids, 990.0, 1000.0), s.bid[1], 1e-6);
        assertEquals(notionalWithin(asks, 1000.0, 1010.0), s.ask[1], 1e-6);
        // 2%..5% lie beyond the truncated book and must not be reported as partial sums.
        for (int band = 2; band < DepthBands.COUNT; band++) {
            assertNull(s.bid[band], "bid band " + band);
            assertNull(s.ask[band], "ask band " + band);
        }
    }

    @Test
    void aBookShorterThanTheLimitIsCompleteSoEveryBandIsReported() {
        // 50 levels, all within 0.2% of the mid: nothing is cut off, so every band holds the
        // whole side rather than going null.
        List<double[]> bids = side(999.99, -0.01, 50);
        List<double[]> asks = side(1000.01, 0.01, 50);
        DepthSnapshot s = DepthBands.fromOrderBook(bids, asks, 1000);
        double wholeBid = notionalWithin(bids, 0, Double.MAX_VALUE);
        double wholeAsk = notionalWithin(asks, 0, Double.MAX_VALUE);
        for (int band = 0; band < DepthBands.COUNT; band++) {
            assertEquals(wholeBid, s.bid[band], 1e-6, "bid band " + band);
            assertEquals(wholeAsk, s.ask[band], 1e-6, "ask band " + band);
        }
    }

    @Test
    void parsesBothArchivePercentageFormats() {
        assertEquals(0, DepthBands.indexOf(Math.abs(Double.parseDouble("-0.20"))));
        assertEquals(5, DepthBands.indexOf(Math.abs(Double.parseDouble("5.00"))));
        assertEquals(5, DepthBands.indexOf(Math.abs(Double.parseDouble("-5"))));
        assertEquals(-1, DepthBands.indexOf(0.5));
    }

    @Test
    void averagesPerBandIgnoringSnapshotsWithoutThatBand() {
        DepthAggregator agg = new DepthAggregator();
        DepthSnapshot old = new DepthSnapshot(); // archive before 2026-01-16: no 0.2% band
        old.bid[1] = 100.0;
        old.ask[1] = 200.0;
        DepthSnapshot recent = new DepthSnapshot();
        recent.bid[0] = 10.0;
        recent.bid[1] = 300.0;
        recent.ask[1] = 400.0;
        agg.add(old);
        agg.add(recent);
        agg.add(new DepthSnapshot()); // empty: not a sample at all

        DepthStats stats = agg.stats();
        assertEquals(2, stats.samples);
        assertEquals(10.0, stats.bid[0]);   // averaged over the one snapshot that had it
        assertNull(stats.ask[0]);
        assertEquals(200.0, stats.bid[1]);
        assertEquals(300.0, stats.ask[1]);
    }

    @Test
    void bucketsSnapshotsIntoCandlesByTimestamp() {
        long hour = 3_600_000L;
        long open = 1_760_000_000_000L / hour * hour;
        Map<Long, DepthSnapshot> snaps = new TreeMap<>();
        for (long t : new long[]{open, open + 1_000, open + hour - 1, open + hour}) {
            DepthSnapshot s = new DepthSnapshot();
            s.bid[1] = 1.0;
            snaps.put(t, s);
        }
        Map<Long, DepthAggregator> byCandle = new HashMap<>();
        DepthAggregator.addAll(byCandle, snaps, hour);

        assertEquals(3, byCandle.get(open).samples());
        assertEquals(1, byCandle.get(open + hour).samples()); // [open, close): the boundary opens the next
    }

    @Test
    void realtimeBufferKeepsOneSamplePerSlotAndSeparatesCandles() {
        com.trading.config.AppConfig cfg = new com.trading.config.AppConfig();
        cfg.getBinance().setInterval("1h");
        RealtimeDepthBuffer buffer = new RealtimeDepthBuffer(cfg);
        long hour = 3_600_000L;
        long open = 1_760_000_000_000L / hour * hour;

        DepthSnapshot a = new DepthSnapshot();
        a.bid[0] = 10.0;
        a.ask[0] = 20.0;
        DepthSnapshot b = new DepthSnapshot();
        b.bid[0] = 30.0;
        b.ask[0] = 40.0;

        assertEquals(true, buffer.record(open + 1_000, a));
        assertEquals(false, buffer.record(open + 2_000, b)); // same 30s slot: dropped
        assertEquals(true, buffer.record(open + 31_000, b));
        assertEquals(true, buffer.record(open + hour + 1_000, a)); // next candle

        DepthStats first = buffer.statsFor(open);
        assertEquals(2, first.samples);
        assertEquals(20.0, first.bid[0]);
        assertEquals(30.0, first.ask[0]);
        assertEquals(1, buffer.statsFor(open + hour).samples);
        assertNull(buffer.statsFor(open - hour));
    }
}
