package com.trading.depth;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.trading.config.AppConfig;
import com.trading.service.BinanceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Takes one live order book snapshot every 30 seconds, the bookDepth archive's cadence, so
 * a realtime candle averages the same number of observations a backfilled one does.
 *
 * Runs on its own schedule rather than inside the fetch loop: the fetch loop's period is
 * app.refresh-rate-seconds, it skips seconds 56-59, and its retries can stall it for several
 * seconds -- any of which would drop or shift samples. This needs a scheduler pool of at
 * least two threads (spring.task.scheduling.pool.size), or it queues behind the fetch loop.
 *
 * Fires at second 1 and 31 rather than 0 and 30, so the book's transaction time lands
 * safely inside the slot and the candle a sample is counted in matches the archive's
 * (whose snapshots are stamped hh:mm:00-06 and hh:mm:30-32).
 *
 * Keeps sampling during warmup: those samples belong to the candles in progress, which the
 * realtime loop writes once warmup hands over.
 */
@Component
public class DepthSampler {

    private static final Logger logger = LoggerFactory.getLogger(DepthSampler.class);

    /** /fapi/v1/depth maximum; anything higher is rejected with -1130. */
    static final int BOOK_LIMIT = 1000;

    private static final long WARN_EVERY_MS = 3_600_000L;

    private final AppConfig appConfig;
    private final BinanceService binanceService;
    private final RealtimeDepthBuffer buffer;
    private long lastCoverageWarnMs = 0;

    public DepthSampler(AppConfig appConfig, BinanceService binanceService, RealtimeDepthBuffer buffer) {
        this.appConfig = appConfig;
        this.binanceService = binanceService;
        this.buffer = buffer;
    }

    @Scheduled(cron = "1,31 * * * * *", zone = "UTC")
    public void sample() {
        if (!appConfig.getDepth().isEnabled()) {
            return;
        }
        try {
            JsonObject book = binanceService.fetchOrderBook(BOOK_LIMIT);
            if (book == null || !book.has("bids") || !book.has("asks")) {
                return;
            }
            long ts = book.has("T") ? book.get("T").getAsLong() : System.currentTimeMillis();
            DepthSnapshot snapshot = DepthBands.fromOrderBook(
                    levels(book.getAsJsonArray("bids")), levels(book.getAsJsonArray("asks")), BOOK_LIMIT);

            if (snapshot.bid[0] == null || snapshot.ask[0] == null) {
                warnCoverage();
            }
            buffer.record(ts, snapshot);
        } catch (Exception e) {
            logger.warn("[Depth] sample failed: {}", e.getMessage());
        }
    }

    /**
     * The +-0.2% band is the only one the 1000-level book reaches at today's prices. At a
     * high enough price (roughly 5,000 for ETHUSDT) even that stops fitting, and the column
     * would go null -- worth saying so rather than leaving it to be noticed in the data.
     */
    private void warnCoverage() {
        long now = System.currentTimeMillis();
        if (now - lastCoverageWarnMs >= WARN_EVERY_MS) {
            lastCoverageWarnMs = now;
            logger.warn("[Depth] the {}-level order book no longer reaches +-0.2% of the mid price; "
                    + "that band stays null in realtime until the archive sync fills it", BOOK_LIMIT);
        }
    }

    private static List<double[]> levels(JsonArray side) {
        List<double[]> out = new ArrayList<>(side.size());
        for (JsonElement level : side) {
            JsonArray pair = level.getAsJsonArray();
            out.add(new double[]{pair.get(0).getAsDouble(), pair.get(1).getAsDouble()});
        }
        return out;
    }
}
