package com.trading.service;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.trading.config.AppConfig;
import com.trading.depth.DepthBands;
import com.trading.depth.DepthStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

@Service
public class DatabaseService {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseService.class);
    private static final ZoneId UTC = ZoneId.of("UTC");
    private static final ZoneId JAKARTA = ZoneId.of("Asia/Jakarta");
    private final JdbcTemplate jdbcTemplate;
    private final AppConfig.Database db;
    /**
     * Stored in ticker_name as configured, so a ".P" row is distinguishable from a spot row
     * of the same pair.
     */
    private final String symbol;
    /**
     * Whether the market-data table has the depth columns. When false, no statement
     * mentions them, so tables created before depth existed keep working unchanged.
     */
    private final boolean depthEnabled;

    public DatabaseService(JdbcTemplate jdbcTemplate, AppConfig appConfig) {
        this.jdbcTemplate = jdbcTemplate;
        this.db = appConfig.getDatabase();
        this.symbol = appConfig.getBinance().getSymbol();
        this.depthEnabled = appConfig.getDepth().isEnabled();
        logger.info("Using tables - market data: {}, application time: {}",
                db.qualifiedMarketData(), db.qualifiedApplicationTime());
        logger.info("Order book depth columns: {}", depthEnabled ? "enabled" : "disabled");
        logger.info("symbol {} -> {} market, ticker_name written as {}",
                symbol,
                appConfig.getBinance().perpMarket() ? "USD-M perpetual" : "spot",
                symbol);
    }

    public LocalDateTime getCurrentApplicationTime() {
        try {
            String sql = "SELECT UTC_DATE FROM " + db.qualifiedApplicationTime() + " WHERE ID = 1";
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                java.sql.Timestamp ts = rs.getTimestamp("UTC_DATE");
                // LocalDateTime utcDateTime = ts != null ? ts.toLocalDateTime() : LocalDateTime.now(UTC);
                LocalDateTime utcDateTime = ts.toLocalDateTime();
                LocalDateTime businessDate = utcDateTime.atZone(UTC).withZoneSameInstant(JAKARTA).toLocalDateTime();
                long unixDate = utcDateTime.atZone(UTC).toEpochSecond();
                logger.info("Current App time - {}", LocalDateTime.now(UTC));
                logger.info("Current DB time - BUSINESS_DATE: {}, UTC_DATE: {}, UNIX_DATE: {}", businessDate, utcDateTime, unixDate);
                return utcDateTime;
            });
        } catch (Exception e) {
            logger.error("Error fetching application time: {}", e.getMessage());
            return LocalDateTime.now(UTC);
        }
    }

    public void updateApplicationTime(long unixTimeMs) {
        try {
            logger.info("Updating application time record from Binance API...");
            long unixTime = unixTimeMs / 1000;
            Instant instant = Instant.ofEpochSecond(unixTime);
            LocalDateTime utcDateTime = LocalDateTime.ofInstant(instant, UTC);
            LocalDateTime businessDate = utcDateTime.atZone(UTC).withZoneSameInstant(JAKARTA).toLocalDateTime();
            LocalDateTime now = LocalDateTime.now(JAKARTA);

            logger.info("BINANCE time now: {}, Updating APPLICATION_TIME to: {}", businessDate, businessDate);

            String sql = "UPDATE " + db.qualifiedApplicationTime() + " SET BUSINESS_DATE = ?, UTC_DATE = ?, UNIX_DATE = ?, INSERT_DATE = ? WHERE ID = 1";
            jdbcTemplate.update(sql, businessDate, utcDateTime, unixTime, now);
            logger.info("APPLICATION_TIME has been synced with Binance open time : {}", businessDate);
        } catch (Exception e) {
            logger.error("Error updating application_time: {}", e.getMessage());
        }
    }

    public void upsertMarketData(List<KlineRecord> records, String partition) {
        try {
            int insertCount = 0;
            int updateCount = 0;
            
            for (KlineRecord record : records) {
                boolean exists = checkExists(record.businessDate, record.unixDate, record.utcDateTime);
                if (exists) {
                    updateRecord(record, partition);
                    updateCount++;
                } else {
                    insertRecord(record, partition);
                    insertCount++;
                }
            }
            
            logger.info("Committed {} rows (inserted: {}, updated: {}) to partition {}", 
                    records.size(), insertCount, updateCount, partition);
        } catch (Exception e) {
            logger.error("Error upserting market data: {}", e.getMessage());
        }
    }

    private boolean checkExists(LocalDateTime businessDate, long unixDate, LocalDateTime utcDateTime) {
        try {
            String sql = "SELECT 1 FROM " + db.qualifiedMarketData() + " WHERE business_date = ? AND unix_date = ? AND utc_date = ? LIMIT 1";
            return jdbcTemplate.queryForObject(sql, Integer.class, businessDate, unixDate, utcDateTime) != null;
        } catch (Exception e) {
            return false;
        }
    }

    private void insertRecord(KlineRecord record, String partition) {
        try {
            String sql = "INSERT INTO " + db.qualifiedMarketData() + " (" +
                    "BUSINESS_DATE, UNIX_DATE, UTC_DATE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, CLOSE_PRICE_ADJUSTED, " +
                    "VOLUME, TICKER_NAME, INSERT_DATE, KLINE_CLOSE_TIME, QUOTE_ASSET_VOLUME, " +
                    "NUMBER_OF_TRADES, TAKER_BUY_BASE_ASSET_VOLUME, TAKER_BUY_QUOTE_ASSET_VOLUME, " +
                    "LAST_TXN_CHECK, AVG_TRADE_SIZE, AGGRESSIVE_BUY_RATIO, PRICE_EFFICIENCY, " +
                    "TRADE_IMBALANCE, ICEBERG_SCORE, UPSIDE_PROBABILITY, PRICE_CHANGE, SUM_OPEN_INTEREST, " +
                    "SUM_OPEN_INTEREST_VALUE, CMC_CIRCULATING_SPLY, LONG_ACCOUNT, LS_RATIO, SHORT_ACCOUNT, " +
                    "FUNDING_RATE" + depthColumnsSuffix() + ") " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?" +
                    depthPlaceholdersSuffix() + ")";

            List<Object> args = new ArrayList<>(Arrays.asList(
                    record.businessDate, record.unixDate, record.utcDateTime,
                    record.openPrice, record.highPrice, record.lowPrice, record.closePrice, record.closePriceAdjusted,
                    record.volume, symbol, record.insertDate, record.kloseTime,
                    record.quoteAssetVolume, record.numberOfTrades,
                    record.takerBuyBaseVolume, record.takerBuyQuoteVolume, 0,
                    record.avgTradeSize, record.aggressiveBuyRatio, record.priceEfficiency,
                    record.tradeImbalance, record.icebergScore, record.upsideProbability, record.priceChange,
                    record.sumOpenInterest, record.sumOpenInterestValue, record.cmcCirculatingSupply,
                    record.longAccount, record.lsRatio, record.shortAccount,
                    record.fundingRate));
            if (depthEnabled) {
                args.addAll(depthValues(record.depth));
            }
            jdbcTemplate.update(sql, args.toArray());
        } catch (Exception e) {
            logger.error("Error inserting record: {}", e.getMessage());
        }
    }

    private void updateRecord(KlineRecord record, String partition) {
        try {
            String sql = "UPDATE " + db.qualifiedMarketData() + " SET " +
                    "OPEN_PRICE = ?, HIGH_PRICE = ?, LOW_PRICE = ?, CLOSE_PRICE = ?, CLOSE_PRICE_ADJUSTED = ?, " +
                    "VOLUME = ?, TICKER_NAME = ?, INSERT_DATE = ?, KLINE_CLOSE_TIME = ?, " +
                    "QUOTE_ASSET_VOLUME = ?, NUMBER_OF_TRADES = ?, " +
                    "TAKER_BUY_BASE_ASSET_VOLUME = ?, TAKER_BUY_QUOTE_ASSET_VOLUME = ?, " +
                    "LAST_TXN_CHECK = 0, AVG_TRADE_SIZE = ?, AGGRESSIVE_BUY_RATIO = ?, PRICE_EFFICIENCY = ?, " +
                    "TRADE_IMBALANCE = ?, ICEBERG_SCORE = ?, UPSIDE_PROBABILITY = ?, PRICE_CHANGE = ?, " +
                    "SUM_OPEN_INTEREST = ?, SUM_OPEN_INTEREST_VALUE = ?, CMC_CIRCULATING_SPLY = ?, " +
                    "LONG_ACCOUNT = ?, LS_RATIO = ?, SHORT_ACCOUNT = ?, FUNDING_RATE = ?" +
                    (depthEnabled ? ", " + depthKeepExistingAssignments() : "") + " " +
                    "WHERE business_date = ? AND unix_date = ? AND utc_date = ? AND LAST_TXN_CHECK = 1";

            List<Object> args = new ArrayList<>(Arrays.asList(
                    record.openPrice, record.highPrice, record.lowPrice, record.closePrice, record.closePriceAdjusted,
                    record.volume, symbol, record.insertDate, record.kloseTime,
                    record.quoteAssetVolume, record.numberOfTrades,
                    record.takerBuyBaseVolume, record.takerBuyQuoteVolume,
                    record.avgTradeSize, record.aggressiveBuyRatio, record.priceEfficiency,
                    record.tradeImbalance, record.icebergScore, record.upsideProbability, record.priceChange,
                    record.sumOpenInterest, record.sumOpenInterestValue, record.cmcCirculatingSupply,
                    record.longAccount, record.lsRatio, record.shortAccount,
                    record.fundingRate));
            if (depthEnabled) {
                args.addAll(depthValues(record.depth));
            }
            args.addAll(Arrays.asList(record.businessDate, record.unixDate, record.utcDateTime));
            jdbcTemplate.update(sql, args.toArray());
        } catch (Exception e) {
            logger.error("Error updating record: {}", e.getMessage());
        }
    }

    public void updateLastTransaction(int limit, String partition) {
        String tableName = db.qualifiedMarketDataPartition(partition);
        String mainTable = db.qualifiedMarketData();
        logger.info("In updateLastTransaction LIMIT used: {} and Partition in: {}", limit,partition);
        try {
            String sql = String.format(
                "WITH latest_row AS (" +
                "SELECT id FROM %s " +
                "WHERE insert_date IS NOT NULL " +
                "ORDER BY business_date DESC LIMIT 1) " +
                "UPDATE %s AS tgt " +
                "SET last_txn_check = 1 FROM latest_row src WHERE tgt.id = src.id",
                mainTable,
                mainTable
            );
            String sql2 = String.format(
                "WITH latest_row AS (" +
                "SELECT id FROM %s " +
                "WHERE insert_date IS NOT NULL " +
                "ORDER BY business_date DESC LIMIT 1) " +
                "UPDATE %s AS tgt " +
                "SET last_txn_check = 1 FROM latest_row src WHERE tgt.id = src.id",
                tableName,
                tableName
            );
            if (limit == 1) {
                logger.info("Updating last transaction in partition {}", partition);
                jdbcTemplate.update(sql2);
            } else {
                logger.info("Updating last transaction in main table {}", mainTable);
                jdbcTemplate.update(sql);
            }
            
            // if (partition.isEmpty()) {
            //     logger.info("Updated Last Kline in partition eth_market_data");
            // } else {
            //     logger.info("Updated Last Kline in partition eth_market_data_{}", partition);
            // }
        } catch (Exception e) {
            logger.error("Updating last txn failed, because of: {}", e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // Warmup support. Added for historical backfill; the realtime methods
    // above are untouched and keep their per-row behaviour.
    // ------------------------------------------------------------------

    /** Latest business_date already stored, or null when the table is empty. */
    public LocalDateTime getMaxBusinessDate() {
        try {
            String sql = "SELECT max(business_date) FROM " + db.qualifiedMarketData();
            return jdbcTemplate.queryForObject(sql, LocalDateTime.class);
        } catch (Exception e) {
            logger.error("Error reading max(business_date): {}", e.getMessage());
            return null;
        }
    }

    /**
     * The realtime code reads and writes application time with WHERE ID = 1, so a freshly
     * created table needs that row to exist or every update silently affects zero rows.
     */
    public void ensureApplicationTimeSeed() {
        try {
            String table = db.qualifiedApplicationTime();
            Integer count = jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM " + table + " WHERE ID = 1", Integer.class);
            if (count != null && count > 0) {
                return;
            }
            LocalDateTime now = LocalDateTime.now(UTC);
            jdbcTemplate.update(
                    "INSERT INTO " + table + " (ID, BUSINESS_DATE, UTC_DATE, UNIX_DATE, INSERT_DATE) "
                            + "VALUES (1, ?, ?, ?, ?)",
                    now.atZone(UTC).withZoneSameInstant(JAKARTA).toLocalDateTime(),
                    now,
                    now.atZone(UTC).toEpochSecond(),
                    LocalDateTime.now(JAKARTA));
            logger.info("Seeded {} with ID = 1", table);
        } catch (Exception e) {
            logger.error("Error seeding application time row: {}", e.getMessage());
        }
    }

    /**
     * Creates the monthly partition for the given month if it is missing, reusing the
     * project's own create_partition_table_with_indexes procedure.
     *
     * Two details matter: the procedure builds identifiers with format('%I', ...), so the
     * table name must be lower case or it ends up quoted and unresolvable; and its inner
     * CREATE statements are unqualified, so search_path must point at the schema.
     */
    public void ensurePartition(YearMonth month) {
        String schema = db.getSchema();
        String table = db.getTable().getMarketData().toLowerCase();
        String from = month.atDay(1).toString();
        String to = month.plusMonths(1).atDay(1).toString();

        try {
            jdbcTemplate.execute((org.springframework.jdbc.core.ConnectionCallback<Void>) con -> {
                try (Statement st = con.createStatement()) {
                    if (schema != null && !schema.isBlank()) {
                        st.execute("SET search_path TO " + schema + ", public");
                    }
                    st.execute(String.format(
                            "CALL %screate_partition_table_with_indexes('%s', '%s', '%s')",
                            (schema == null || schema.isBlank()) ? "" : schema + ".",
                            table, from, to));
                    st.execute("RESET search_path");
                }
                return null;
            });
        } catch (Exception e) {
            logger.warn("Partition ensure for {} failed (may already exist): {}", month, e.getMessage());
        }
    }

    /**
     * Bulk upsert used by warmup. Relies on the UNIQUE (business_date, unix_date, utc_date)
     * constraint. If the batch fails for any reason the rows are retried one by one so a
     * single bad row cannot discard a whole month.
     */
    public int batchUpsertMarketData(List<KlineRecord> records) {
        if (records == null || records.isEmpty()) {
            return 0;
        }
        String sql = upsertSql();
        try {
            jdbcTemplate.batchUpdate(sql, new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    bindUpsert(ps, records.get(i));
                }

                @Override
                public int getBatchSize() {
                    return records.size();
                }
            });
            return records.size();
        } catch (Exception e) {
            logger.warn("Batch upsert of {} rows failed ({}), retrying row by row",
                    records.size(), e.getMessage());
            int ok = 0;
            for (KlineRecord record : records) {
                try {
                    jdbcTemplate.update(sql, ps -> bindUpsert(ps, record));
                    ok++;
                } catch (Exception rowError) {
                    logger.error("Skipping row utc_date={}: {}", record.utcDateTime, rowError.getMessage());
                }
            }
            return ok;
        }
    }

    private String upsertSql() {
        // Aliased as tgt so the depth columns can fall back to the stored value: re-running
        // warmup over a range must not erase depth that only the archive sync could provide.
        return "INSERT INTO " + db.qualifiedMarketData() + " AS tgt ("
                + "BUSINESS_DATE, UNIX_DATE, UTC_DATE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, "
                + "CLOSE_PRICE_ADJUSTED, VOLUME, TICKER_NAME, INSERT_DATE, KLINE_CLOSE_TIME, "
                + "QUOTE_ASSET_VOLUME, NUMBER_OF_TRADES, TAKER_BUY_BASE_ASSET_VOLUME, "
                + "TAKER_BUY_QUOTE_ASSET_VOLUME, LAST_TXN_CHECK, AVG_TRADE_SIZE, AGGRESSIVE_BUY_RATIO, "
                + "PRICE_EFFICIENCY, TRADE_IMBALANCE, ICEBERG_SCORE, UPSIDE_PROBABILITY, PRICE_CHANGE, "
                + "SUM_OPEN_INTEREST, SUM_OPEN_INTEREST_VALUE, CMC_CIRCULATING_SPLY, "
                + "LONG_ACCOUNT, LS_RATIO, SHORT_ACCOUNT, FUNDING_RATE" + depthColumnsSuffix() + ") "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
                + depthPlaceholdersSuffix() + ") "
                + "ON CONFLICT (BUSINESS_DATE, UNIX_DATE, UTC_DATE) DO UPDATE SET "
                + (depthEnabled ? depthExcludedOrExistingAssignments() + ", " : "")
                + "OPEN_PRICE = EXCLUDED.OPEN_PRICE, HIGH_PRICE = EXCLUDED.HIGH_PRICE, "
                + "LOW_PRICE = EXCLUDED.LOW_PRICE, CLOSE_PRICE = EXCLUDED.CLOSE_PRICE, "
                + "CLOSE_PRICE_ADJUSTED = EXCLUDED.CLOSE_PRICE_ADJUSTED, VOLUME = EXCLUDED.VOLUME, "
                + "TICKER_NAME = EXCLUDED.TICKER_NAME, INSERT_DATE = EXCLUDED.INSERT_DATE, "
                + "KLINE_CLOSE_TIME = EXCLUDED.KLINE_CLOSE_TIME, "
                + "QUOTE_ASSET_VOLUME = EXCLUDED.QUOTE_ASSET_VOLUME, "
                + "NUMBER_OF_TRADES = EXCLUDED.NUMBER_OF_TRADES, "
                + "TAKER_BUY_BASE_ASSET_VOLUME = EXCLUDED.TAKER_BUY_BASE_ASSET_VOLUME, "
                + "TAKER_BUY_QUOTE_ASSET_VOLUME = EXCLUDED.TAKER_BUY_QUOTE_ASSET_VOLUME, "
                + "AVG_TRADE_SIZE = EXCLUDED.AVG_TRADE_SIZE, "
                + "AGGRESSIVE_BUY_RATIO = EXCLUDED.AGGRESSIVE_BUY_RATIO, "
                + "PRICE_EFFICIENCY = EXCLUDED.PRICE_EFFICIENCY, "
                + "TRADE_IMBALANCE = EXCLUDED.TRADE_IMBALANCE, "
                + "ICEBERG_SCORE = EXCLUDED.ICEBERG_SCORE, "
                + "UPSIDE_PROBABILITY = EXCLUDED.UPSIDE_PROBABILITY, "
                + "PRICE_CHANGE = EXCLUDED.PRICE_CHANGE, "
                + "SUM_OPEN_INTEREST = EXCLUDED.SUM_OPEN_INTEREST, "
                + "SUM_OPEN_INTEREST_VALUE = EXCLUDED.SUM_OPEN_INTEREST_VALUE, "
                + "CMC_CIRCULATING_SPLY = EXCLUDED.CMC_CIRCULATING_SPLY, "
                + "LONG_ACCOUNT = EXCLUDED.LONG_ACCOUNT, LS_RATIO = EXCLUDED.LS_RATIO, "
                + "SHORT_ACCOUNT = EXCLUDED.SHORT_ACCOUNT, FUNDING_RATE = EXCLUDED.FUNDING_RATE";
    }

    private void bindUpsert(PreparedStatement ps, KlineRecord r) throws SQLException {
        int i = 1;
        ps.setObject(i++, r.businessDate);
        ps.setLong(i++, r.unixDate);
        ps.setObject(i++, r.utcDateTime);
        ps.setDouble(i++, r.openPrice);
        ps.setDouble(i++, r.highPrice);
        ps.setDouble(i++, r.lowPrice);
        ps.setDouble(i++, r.closePrice);
        ps.setDouble(i++, r.closePriceAdjusted);
        ps.setDouble(i++, r.volume);
        ps.setString(i++, symbol);
        ps.setObject(i++, r.insertDate);
        ps.setLong(i++, r.kloseTime);
        ps.setDouble(i++, r.quoteAssetVolume);
        ps.setLong(i++, r.numberOfTrades);
        ps.setDouble(i++, r.takerBuyBaseVolume);
        ps.setDouble(i++, r.takerBuyQuoteVolume);
        ps.setInt(i++, 0); // LAST_TXN_CHECK, same as insertRecord
        ps.setDouble(i++, r.avgTradeSize);
        ps.setDouble(i++, r.aggressiveBuyRatio);
        ps.setDouble(i++, r.priceEfficiency);
        ps.setDouble(i++, r.tradeImbalance);
        ps.setDouble(i++, r.icebergScore);
        ps.setString(i++, r.upsideProbability);
        ps.setDouble(i++, r.priceChange);
        setNullableDouble(ps, i++, r.sumOpenInterest);
        setNullableDouble(ps, i++, r.sumOpenInterestValue);
        setNullableDouble(ps, i++, r.cmcCirculatingSupply);
        setNullableDouble(ps, i++, r.longAccount);
        setNullableDouble(ps, i++, r.lsRatio);
        setNullableDouble(ps, i++, r.shortAccount);
        setNullableDouble(ps, i++, r.fundingRate);
        if (depthEnabled) {
            bindDepth(ps, i, r.depth);
        }
    }

    // ------------------------------------------------------------------
    // Order book depth (see com.trading.depth.DepthBands).
    // ------------------------------------------------------------------

    /** ", BID_NOTIONAL_0_2PCT, ..." to append to a column list, or "" when depth is off. */
    private String depthColumnsSuffix() {
        return depthEnabled ? ", " + DepthBands.columnList() : "";
    }

    /** ", ?, ?, ..." matching depthColumnsSuffix(), or "" when depth is off. */
    private String depthPlaceholdersSuffix() {
        if (!depthEnabled) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < DepthBands.columnCount(); i++) {
            sb.append(", ?");
        }
        return sb.toString();
    }

    private static List<String> depthColumns() {
        return Arrays.asList(DepthBands.columnList().split(", "));
    }

    /**
     * Realtime update: "COL = COALESCE(?, COL)" for every depth column. The sampler only
     * knows the bands the live book reached and only the candles it has observed since
     * startup, so a null must never overwrite a value that is already there.
     */
    private static String depthKeepExistingAssignments() {
        StringBuilder sb = new StringBuilder();
        for (String col : depthColumns()) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(col).append(" = COALESCE(?, ").append(col).append(")");
        }
        return sb.toString();
    }

    /** Warmup upsert: take the new value, but keep the stored one where the new one is null. */
    private static String depthExcludedOrExistingAssignments() {
        StringBuilder sb = new StringBuilder();
        for (String col : depthColumns()) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(col).append(" = COALESCE(EXCLUDED.").append(col).append(", tgt.").append(col).append(")");
        }
        return sb.toString();
    }

    /** Depth values in DepthBands.columnList() order; all null when there is no depth. */
    private static List<Object> depthValues(DepthStats depth) {
        List<Object> values = new ArrayList<>(DepthBands.columnCount());
        for (int i = 0; i < DepthBands.COUNT; i++) {
            values.add(depth == null ? null : depth.bid[i]);
            values.add(depth == null ? null : depth.ask[i]);
        }
        values.add(depth == null ? null : depth.samples);
        return values;
    }

    private static void bindDepth(PreparedStatement ps, int start, DepthStats depth) throws SQLException {
        int i = start;
        for (int band = 0; band < DepthBands.COUNT; band++) {
            setNullableDouble(ps, i++, depth == null ? null : depth.bid[band]);
            setNullableDouble(ps, i++, depth == null ? null : depth.ask[band]);
        }
        if (depth == null || depth.samples == null) {
            ps.setNull(i, Types.INTEGER);
        } else {
            ps.setInt(i, depth.samples);
        }
    }

    /**
     * Rows of one UTC day that still lack the archive-only bands, and how many rows the day
     * has at all. The archive sync uses both: zero rows means the realtime loop has not
     * written that day yet, which is different from "already complete".
     *
     * The business_date range is added so the planner prunes to one or two partitions;
     * business_date is utc_date shifted to Asia/Jakarta.
     */
    public long[] countDepthCoverage(LocalDateTime dayStartUtc, LocalDateTime dayEndUtc) {
        String sql = "SELECT count(*), count(*) FILTER (WHERE " + DepthBands.askColumn(1) + " IS NULL) FROM "
                + db.qualifiedMarketData()
                + " WHERE utc_date >= ? AND utc_date < ? AND business_date >= ? AND business_date < ?";
        return jdbcTemplate.queryForObject(sql, (rs, n) -> new long[]{rs.getLong(1), rs.getLong(2)},
                dayStartUtc, dayEndUtc, toBusiness(dayStartUtc), toBusiness(dayEndUtc));
    }

    /**
     * The provisional realtime +-0.2% values of one UTC day, keyed by candle open (epoch ms).
     * Read before the archive overwrites them, to log how far apart the two sources are.
     *
     * Realtime rows are told apart by their empty 1% band: only the archive ever fills it,
     * so a row that has the 0.2% band without it got that value from the live sampler.
     */
    public Map<Long, double[]> loadRealtimeNearBand(LocalDateTime dayStartUtc, LocalDateTime dayEndUtc) {
        String bid = DepthBands.bidColumn(0);
        String ask = DepthBands.askColumn(0);
        String sql = "SELECT utc_date, " + bid + ", " + ask + " FROM " + db.qualifiedMarketData()
                + " WHERE utc_date >= ? AND utc_date < ? AND business_date >= ? AND business_date < ?"
                + " AND " + bid + " IS NOT NULL AND " + ask + " IS NOT NULL"
                + " AND " + DepthBands.askColumn(1) + " IS NULL";
        Map<Long, double[]> out = new HashMap<>();
        jdbcTemplate.query(sql, rs -> {
            long openMs = rs.getTimestamp(1).toLocalDateTime().toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
            out.put(openMs, new double[]{rs.getDouble(2), rs.getDouble(3)});
        }, dayStartUtc, dayEndUtc, toBusiness(dayStartUtc), toBusiness(dayEndUtc));
        return out;
    }

    /**
     * Writes archive depth onto rows that do not have it yet, keyed by candle open (epoch ms,
     * UTC). Archive values replace the provisional realtime ones; a band the archive lacks
     * keeps whatever is stored. Rows that already carry archive bands (warmup wrote them) are
     * left alone. Only the depth columns are touched -- LAST_TXN_CHECK and the price columns
     * stay as the realtime loop left them.
     */
    public int updateDepth(Map<Long, DepthStats> byCandleOpen) {
        if (byCandleOpen.isEmpty()) {
            return 0;
        }
        String sql = "UPDATE " + db.qualifiedMarketData() + " SET " + depthKeepExistingAssignments()
                + " WHERE business_date = ? AND utc_date = ? AND " + DepthBands.askColumn(1) + " IS NULL";
        List<Map.Entry<Long, DepthStats>> entries = new ArrayList<>(byCandleOpen.entrySet());
        int[][] counts = jdbcTemplate.batchUpdate(sql, entries, 500, (ps, e) -> {
            bindDepth(ps, 1, e.getValue());
            LocalDateTime utc = LocalDateTime.ofInstant(Instant.ofEpochMilli(e.getKey()), UTC);
            ps.setObject(DepthBands.columnCount() + 1, toBusiness(utc));
            ps.setObject(DepthBands.columnCount() + 2, utc);
        });
        int updated = 0;
        for (int[] batch : counts) {
            for (int c : batch) {
                updated += Math.max(c, 0);
            }
        }
        return updated;
    }

    private static LocalDateTime toBusiness(LocalDateTime utc) {
        return utc.atZone(UTC).withZoneSameInstant(JAKARTA).toLocalDateTime();
    }

    private static void setNullableDouble(PreparedStatement ps, int index, Double value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.NUMERIC);
        } else {
            ps.setDouble(index, value);
        }
    }

    // Inner class for kline record
    public static class KlineRecord {
        public LocalDateTime businessDate;
        public long unixDate;
        public LocalDateTime utcDateTime;
        public double openPrice, highPrice, lowPrice, closePrice, closePriceAdjusted;
        public double volume, quoteAssetVolume, takerBuyBaseVolume, takerBuyQuoteVolume;
        public long kloseTime, numberOfTrades;
        public LocalDateTime insertDate;
        public double avgTradeSize, aggressiveBuyRatio, priceEfficiency, tradeImbalance, icebergScore, priceChange;
        public String upsideProbability;
        public Double sumOpenInterest, sumOpenInterestValue, cmcCirculatingSupply, longAccount, lsRatio, shortAccount;
        /** Forward-filled: the funding rate in effect for this hour. */
        public Double fundingRate;
        /** Order book depth averaged over the candle, or null when none was observed. */
        public DepthStats depth;
    }
}
