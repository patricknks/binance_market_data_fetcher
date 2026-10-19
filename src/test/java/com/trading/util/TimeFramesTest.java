package com.trading.util;

import com.trading.config.AppConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The timeframe rework replaced hour arithmetic with candle arithmetic. These lock in that
 * a 1h run still computes exactly what it used to, and that 15m behaves the same way one
 * candle at a time.
 */
class TimeFramesTest {

    private static AppConfig.Binance binance(String interval) {
        AppConfig.Binance b = new AppConfig.Binance();
        b.setInterval(interval);
        return b;
    }

    @Test
    void parsesBinanceIntervals() {
        assertEquals(Duration.ofHours(1), binance("1h").intervalDuration());
        assertEquals(Duration.ofMinutes(15), binance("15m").intervalDuration());
        assertEquals(Duration.ofHours(4), binance("4h").intervalDuration());
        assertEquals(Duration.ofDays(1), binance("1d").intervalDuration());
        // Whitespace and case are tolerated; the normalised form is what endpoints get.
        assertEquals(Duration.ofMinutes(15), binance(" 15M ").intervalDuration());
        assertEquals("15m", binance(" 15M ").normalisedInterval());
    }

    @Test
    void rejectsUnparseableInterval() {
        assertThrows(IllegalArgumentException.class, () -> binance("15min").intervalDuration());
        assertThrows(IllegalArgumentException.class, () -> binance("0h").intervalDuration());
        assertThrows(IllegalArgumentException.class, () -> binance("").intervalDuration());
    }

    @Test
    void flagsIntervalsTheMetricsEndpointsRefuse() {
        assertTrue(binance("1h").metricsPeriodSupported());
        assertTrue(binance("15m").metricsPeriodSupported());
        assertFalse(binance("1m").metricsPeriodSupported());
        assertFalse(binance("3d").metricsPeriodSupported());
    }

    @Test
    void floorsToCandleBoundary() {
        LocalDateTime t = LocalDateTime.of(2026, 9, 20, 10, 47, 33);
        assertEquals(LocalDateTime.of(2026, 9, 20, 10, 45), TimeFrames.floorTo(t, Duration.ofMinutes(15)));
        assertEquals(LocalDateTime.of(2026, 9, 20, 10, 0), TimeFrames.floorTo(t, Duration.ofHours(1)));
        // Already on a boundary stays put, so the trigger dedupe cannot fire twice.
        LocalDateTime onBoundary = LocalDateTime.of(2026, 9, 20, 10, 45);
        assertEquals(onBoundary, TimeFrames.floorTo(onBoundary, Duration.ofMinutes(15)));
    }

    @Test
    void countsCandlesLikeTheOldHourMath() {
        // The realtime window is [appTime - 1 candle, now]. Up to date -> 1 candle (fetch a
        // single kline); a closed candle -> 2 (the catch-up branch that triggers the indicator).
        LocalDateTime dbTime = LocalDateTime.of(2026, 9, 20, 10, 0);

        LocalDateTime nowSameHour = LocalDateTime.of(2026, 9, 20, 10, 30);
        assertEquals(1, TimeFrames.candlesBetween(dbTime.minusHours(1), nowSameHour, Duration.ofHours(1)));

        LocalDateTime nowNextHour = LocalDateTime.of(2026, 9, 20, 11, 5);
        assertEquals(2, TimeFrames.candlesBetween(dbTime.minusHours(1), nowNextHour, Duration.ofHours(1)));

        // Same shape on 15m: within the candle -> 1, once the next one opens -> 2.
        Duration q = Duration.ofMinutes(15);
        LocalDateTime db15 = LocalDateTime.of(2026, 9, 20, 10, 15);
        assertEquals(1, TimeFrames.candlesBetween(db15.minus(q), LocalDateTime.of(2026, 9, 20, 10, 22), q));
        assertEquals(2, TimeFrames.candlesBetween(db15.minus(q), LocalDateTime.of(2026, 9, 20, 10, 31), q));
    }

    @Test
    void candlesBetweenMatchesToHoursOnHourlyIntervals() {
        LocalDateTime start = LocalDateTime.of(2026, 9, 20, 3, 17);
        for (int minutes = 0; minutes <= 60 * 60; minutes += 7) {
            LocalDateTime end = start.plusMinutes(minutes);
            assertEquals(Duration.between(start, end).toHours(),
                    TimeFrames.candlesBetween(start, end, Duration.ofHours(1)));
        }
    }
}
