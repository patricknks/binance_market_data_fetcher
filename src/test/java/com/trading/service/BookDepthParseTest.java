package com.trading.service;

import com.trading.depth.DepthSnapshot;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Lines copied from real bookDepth archives, old (2023) and new (2026) format. */
class BookDepthParseTest {

    private static long ms(String ts) {
        return LocalDateTime.parse(ts.replace(' ', 'T')).toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    @Test
    void parsesNewFormatWithTheNearBand() {
        Map<Long, DepthSnapshot> out = new TreeMap<>();
        for (String line : new String[]{
                "timestamp,percentage,depth,notional",
                "2026-10-17 00:00:01,-1.00,37836.29000000,91267679.59146000",
                "2026-10-17 00:00:01,-0.20,9836.38400000,23805013.54809000",
                "2026-10-17 00:00:01,0.20,10653.42700000,25836731.15300000",
                "2026-10-17 00:00:01,5.00,113876.71900000,280664106.97819000"}) {
            BinanceVisionService.addBookDepthLine(out, line);
        }
        assertEquals(1, out.size());
        DepthSnapshot s = out.get(ms("2026-10-17 00:00:01"));
        assertEquals(23805013.54809, s.bid[0], 1e-4);
        assertEquals(25836731.15300, s.ask[0], 1e-4);
        assertEquals(91267679.59146, s.bid[1], 1e-4);
        assertEquals(280664106.97819, s.ask[5], 1e-4);
        assertNull(s.ask[1]);
    }

    @Test
    void parsesOldFormatWithoutTheNearBand() {
        Map<Long, DepthSnapshot> out = new TreeMap<>();
        BinanceVisionService.addBookDepthLine(out, "2023-01-01 00:06:05,-5,139098.092,330478341.667");
        BinanceVisionService.addBookDepthLine(out, "2023-01-01 00:06:05,1,34573.148,84105794.664");
        BinanceVisionService.addBookDepthLine(out, "2023-01-01 00:06:32,1,34000.0,84000000.0");
        assertEquals(2, out.size());
        DepthSnapshot first = out.get(ms("2023-01-01 00:06:05"));
        assertEquals(330478341.667, first.bid[5], 1e-3);
        assertEquals(84105794.664, first.ask[1], 1e-3);
        assertNull(first.bid[0]);
    }
}
