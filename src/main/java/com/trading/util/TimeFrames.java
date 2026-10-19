package com.trading.util;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Timeframe-aware arithmetic, so a 1h run and a 15m run share one code path.
 *
 * Everything that used to be expressed in hours -- fetch windows, chunk widths, the warmup
 * resume step, the candle boundary the indicator trigger dedupes on -- is derived from the
 * interval configured in app.binance.interval instead.
 */
public final class TimeFrames {

    private TimeFrames() {
    }

    /**
     * Floors a timestamp to the candle boundary of the given step: 10:47 with a 15m step
     * becomes 10:45, with a 1h step 10:00. Replaces truncatedTo(HOURS), which could only
     * ever produce hourly boundaries.
     *
     * The value is treated as wall-clock time. Both zones in play here (UTC and
     * Asia/Jakarta) have whole-hour offsets and no DST, so flooring the local time and
     * flooring the instant agree for every interval up to a day.
     */
    public static LocalDateTime floorTo(LocalDateTime time, Duration step) {
        long stepSeconds = step.getSeconds();
        if (stepSeconds <= 0) {
            return time;
        }
        long epochSeconds = time.toEpochSecond(ZoneOffset.UTC);
        long floored = Math.floorDiv(epochSeconds, stepSeconds) * stepSeconds;
        return LocalDateTime.ofEpochSecond(floored, 0, ZoneOffset.UTC);
    }

    /**
     * Whole candles that fit between two timestamps. Truncates towards zero, matching the
     * Duration.toHours() this replaced, so the 1h thresholds keep their old meaning.
     */
    public static long candlesBetween(LocalDateTime start, LocalDateTime end, Duration step) {
        long stepSeconds = step.getSeconds();
        if (stepSeconds <= 0) {
            return 0;
        }
        return Duration.between(start, end).getSeconds() / stepSeconds;
    }
}
