package com.trading.config;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
@ConfigurationProperties(prefix = "app")
public class AppConfig {

    private static final Logger logger = LoggerFactory.getLogger(AppConfig.class);

    private Binance binance = new Binance();
    private Database database = new Database();
    private Warmup warmup = new Warmup();
    private Depth depth = new Depth();
    private int refreshRateSeconds;  // Will be injected from application.properties
    private int memoryLimitMb;
    private int gcIntervalLoops;
    private Healthcheck healthcheck = new Healthcheck();
    private Trigger trigger = new Trigger();

    /**
     * Market selection is driven by the symbol: a ".P" suffix (e.g. ETHUSDT.P) means the
     * USD-M perpetual, anything else means spot. Only OHLCV follows that switch -- open
     * interest, long/short, funding and CMC supply are perpetual-only figures and always
     * come from the futures endpoints regardless.
     *
     * The suffix is a charting convention, not a Binance symbol: every API call and archive
     * path must use the base symbol, which is why baseSymbol() exists.
     *
     * The interval drives more than the API parameter: fetch windows, chunk widths and the
     * warmup resume step are all derived from intervalDuration(), so switching between 1h
     * and 15m is a config change.
     */
    public static class Binance {
        private String symbol = "ETHUSDT";
        private String interval = "1h";
        /** Spot klines endpoint, used when the symbol has no ".P" suffix. */
        private String klinesUrl;
        /** USD-M perpetual klines endpoint, used when the symbol ends with ".P". */
        private String perpKlinesUrl = "https://fapi.binance.com/fapi/v1/klines";
        private String oiUrl;
        private String lsUrl;
        /** Unlike the /futures/data/ endpoints, this one serves the full history, not just 30 days. */
        private String fundingUrl = "https://fapi.binance.com/fapi/v1/fundingRate";
        /** Live order book snapshot. Futures, like OI: the depth archive only exists for USD-M. */
        private String depthUrl = "https://fapi.binance.com/fapi/v1/depth";
        private int timeoutSeconds = 15;

        public String getSymbol() { return symbol; }
        public void setSymbol(String symbol) { this.symbol = symbol; }
        public String getInterval() { return interval; }
        public void setInterval(String interval) { this.interval = interval; }
        public String getKlinesUrl() { return klinesUrl; }
        public void setKlinesUrl(String klinesUrl) { this.klinesUrl = klinesUrl; }
        public String getPerpKlinesUrl() { return perpKlinesUrl; }
        public void setPerpKlinesUrl(String perpKlinesUrl) { this.perpKlinesUrl = perpKlinesUrl; }
        public String getOiUrl() { return oiUrl; }
        public void setOiUrl(String oiUrl) { this.oiUrl = oiUrl; }
        public String getLsUrl() { return lsUrl; }
        public void setLsUrl(String lsUrl) { this.lsUrl = lsUrl; }
        public String getFundingUrl() { return fundingUrl; }
        public void setFundingUrl(String fundingUrl) { this.fundingUrl = fundingUrl; }
        public String getDepthUrl() { return depthUrl; }
        public void setDepthUrl(String depthUrl) { this.depthUrl = depthUrl; }
        public int getTimeoutSeconds() { return timeoutSeconds; }
        public void setTimeoutSeconds(int timeoutSeconds) { this.timeoutSeconds = timeoutSeconds; }

        private static final String PERP_SUFFIX = ".P";

        /** Binance interval syntax: a count plus a unit, e.g. 15m, 1h, 4h, 1d. */
        private static final Pattern INTERVAL_PATTERN = Pattern.compile("^(\\d+)([mhdw])$");

        /**
         * Periods the /futures/data/ endpoints accept. A klines interval outside this set is
         * still fine for OHLCV, but open interest and long/short cannot be requested for it.
         */
        private static final Set<String> METRICS_PERIODS = Collections.unmodifiableSet(
                new HashSet<>(Arrays.asList("5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d")));

        // Plain methods rather than getters, so the properties binder ignores them.

        /** Lower-cased, trimmed interval, which is the form every Binance endpoint expects. */
        public String normalisedInterval() {
            return interval == null ? "" : interval.trim().toLowerCase();
        }

        /**
         * The configured interval as a Duration: 15m -> PT15M, 1h -> PT1H.
         *
         * Every window and step in the fetch path is a multiple of this, which is what makes
         * the timeframe configurable. Throws on an unparseable value so a typo fails at
         * startup rather than producing silently wrong windows.
         */
        public Duration intervalDuration() {
            String raw = normalisedInterval();
            Matcher matcher = INTERVAL_PATTERN.matcher(raw);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Unsupported app.binance.interval '" + interval
                        + "'. Expected a Binance interval such as 15m, 1h or 4h.");
            }
            long count = Long.parseLong(matcher.group(1));
            if (count <= 0) {
                throw new IllegalArgumentException("app.binance.interval must be positive, got '" + interval + "'");
            }
            String unit = matcher.group(2);
            if ("m".equals(unit)) {
                return Duration.ofMinutes(count);
            }
            if ("h".equals(unit)) {
                return Duration.ofHours(count);
            }
            if ("d".equals(unit)) {
                return Duration.ofDays(count);
            }
            return Duration.ofDays(7 * count);
        }

        /** True when open interest / long-short can be requested for the configured interval. */
        public boolean metricsPeriodSupported() {
            return METRICS_PERIODS.contains(normalisedInterval());
        }

        /** True when the configured symbol selects the USD-M perpetual market. */
        public boolean perpMarket() {
            return symbol != null && symbol.toUpperCase().endsWith(PERP_SUFFIX);
        }

        /** The symbol Binance actually accepts: the configured one minus the ".P" suffix. */
        public String baseSymbol() {
            return perpMarket() ? symbol.substring(0, symbol.length() - PERP_SUFFIX.length()) : symbol;
        }

        /** Klines endpoint for the selected market. */
        public String klinesUrlForMarket() {
            return perpMarket() ? perpKlinesUrl : klinesUrl;
        }
    }

    /**
     * Schema and table names, so they are not hardcoded in the SQL statements.
     * The qualified* helpers are plain methods (not getters) so the binder ignores them.
     */
    public static class Database {
        private String schema = "algo_oltp";
        private Table table = new Table();

        public static class Table {
            private String marketData = "eth_market_data";
            private String applicationTime = "application_time";

            public String getMarketData() { return marketData; }
            public void setMarketData(String marketData) { this.marketData = marketData; }
            public String getApplicationTime() { return applicationTime; }
            public void setApplicationTime(String applicationTime) { this.applicationTime = applicationTime; }
        }

        public String getSchema() { return schema; }
        public void setSchema(String schema) { this.schema = schema; }
        public Table getTable() { return table; }
        public void setTable(Table table) { this.table = table; }

        /** Prefixes a table with the configured schema; a blank schema yields the bare name. */
        private String qualify(String tableName) {
            return (schema == null || schema.isBlank()) ? tableName : schema + "." + tableName;
        }

        public String qualifiedMarketData() {
            return qualify(table.getMarketData());
        }

        public String qualifiedApplicationTime() {
            return qualify(table.getApplicationTime());
        }

        /** Partitioned variant, e.g. algo_oltp.eth_market_data_202608. */
        public String qualifiedMarketDataPartition(String partition) {
            return qualify(table.getMarketData() + "_" + partition);
        }
    }

    /**
     * Historical backfill from data.binance.vision. Warmup runs instead of the realtime
     * loop while the table is empty or older than stale-threshold-days.
     */
    public static class Warmup {
        private boolean enabled = true;
        /** Earliest spot kline archive for ETHUSDT/1h is 2017-08. */
        private LocalDate startDate = LocalDate.of(2017, 8, 1);
        /** Warmup fills up to now minus this many days; realtime covers the rest via REST. */
        private int staleThresholdDays = 14;
        private String baseUrl = "https://data.binance.vision";
        /** Futures metrics archives (open interest / long-short) do not exist before this date. */
        private LocalDate metricsStartDate = LocalDate.of(2021, 12, 1);
        /**
         * How far before a candle's open the matching metrics sample sits. The archives are
         * sampled every 5 minutes and, for 1h candles, the value the REST endpoint reports for
         * hour H is the sample taken at H minus 5 minutes -- verified against live data.
         * Exposed as a setting so another timeframe can be re-aligned without a code change;
         * a wrong value shows up as null open-interest columns on warmup rows.
         */
        private int metricsOffsetMinutes = 5;
        private boolean autoCreatePartition = true;
        private int httpTimeoutSeconds = 120;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public LocalDate getStartDate() { return startDate; }
        public void setStartDate(LocalDate startDate) { this.startDate = startDate; }
        public int getStaleThresholdDays() { return staleThresholdDays; }
        public void setStaleThresholdDays(int staleThresholdDays) { this.staleThresholdDays = staleThresholdDays; }
        public String getBaseUrl() { return baseUrl; }
        public void setBaseUrl(String baseUrl) { this.baseUrl = baseUrl; }
        public LocalDate getMetricsStartDate() { return metricsStartDate; }
        public void setMetricsStartDate(LocalDate metricsStartDate) { this.metricsStartDate = metricsStartDate; }
        public int getMetricsOffsetMinutes() { return metricsOffsetMinutes; }
        public void setMetricsOffsetMinutes(int metricsOffsetMinutes) { this.metricsOffsetMinutes = metricsOffsetMinutes; }
        public boolean isAutoCreatePartition() { return autoCreatePartition; }
        public void setAutoCreatePartition(boolean autoCreatePartition) { this.autoCreatePartition = autoCreatePartition; }
        public int getHttpTimeoutSeconds() { return httpTimeoutSeconds; }
        public void setHttpTimeoutSeconds(int httpTimeoutSeconds) { this.httpTimeoutSeconds = httpTimeoutSeconds; }
    }

    /**
     * Order book depth bands (see DepthBands).
     *
     * Three sources fill the same columns:
     *   - realtime: a 30-second sampler on /fapi/v1/depth, averaged per candle. The REST book
     *     is capped at 1000 levels (~+-0.45% for ETHUSDT), so only the +-0.2% band comes from it
     *   - warmup: the bookDepth archive, for every candle warmup writes
     *   - archive sync: once a day's archive is published (the day after), it overwrites the
     *     provisional realtime values and fills the 1..5% bands realtime cannot see
     */
    public static class Depth {
        /**
         * Off by default: the depth columns only exist in tables created for them, and with
         * this off every statement is exactly the pre-depth SQL, so older tables keep working.
         */
        private boolean enabled = false;
        /** First day of the bookDepth archive for ETHUSDT. Earlier candles keep null depth. */
        private LocalDate startDate = LocalDate.of(2023, 1, 1);
        /**
         * How many past days the archive sync re-checks. Must exceed warmup's
         * stale-threshold-days: the candles between warmup's cutoff and now are written by
         * the realtime loop without archive depth, and only the sync fills them in.
         */
        private int syncLookbackDays = 30;
        private int syncIntervalMinutes = 60;
        private int syncInitialDelaySeconds = 120;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public LocalDate getStartDate() { return startDate; }
        public void setStartDate(LocalDate startDate) { this.startDate = startDate; }
        public int getSyncLookbackDays() { return syncLookbackDays; }
        public void setSyncLookbackDays(int syncLookbackDays) { this.syncLookbackDays = syncLookbackDays; }
        public int getSyncIntervalMinutes() { return syncIntervalMinutes; }
        public void setSyncIntervalMinutes(int syncIntervalMinutes) { this.syncIntervalMinutes = syncIntervalMinutes; }
        public int getSyncInitialDelaySeconds() { return syncInitialDelaySeconds; }
        public void setSyncInitialDelaySeconds(int syncInitialDelaySeconds) { this.syncInitialDelaySeconds = syncInitialDelaySeconds; }
    }

    public static class Healthcheck {
        private int port = 8061;

        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
    }

    public static class Trigger {
        private String host = "127.0.0.1";
        private int port = 8070;

        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
    }

    /**
     * Resolves the timeframe once at startup so a typo in app.binance.interval fails here,
     * loudly, instead of quietly producing windows of the wrong width later on.
     */
    @PostConstruct
    void logResolvedTimeframe() {
        Duration step = binance.intervalDuration();
        logger.info("Timeframe: symbol={} ({} market), interval={} ({} minutes per candle)",
                binance.getSymbol(),
                binance.perpMarket() ? "USD-M perpetual" : "spot",
                binance.normalisedInterval(),
                step.toMinutes());
        if (!binance.metricsPeriodSupported()) {
            logger.warn("interval {} is not a period the open-interest / long-short endpoints accept; "
                            + "those columns will stay null. Supported: 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d",
                    binance.normalisedInterval());
        }
        if (depth.isEnabled() && depth.getSyncLookbackDays() <= warmup.getStaleThresholdDays() + 1) {
            logger.warn("app.depth.sync-lookback-days={} does not reach past app.warmup.stale-threshold-days={}; "
                            + "candles between the warmup cutoff and now will keep null depth bands",
                    depth.getSyncLookbackDays(), warmup.getStaleThresholdDays());
        }
    }

    public Binance getBinance() { return binance; }
    public void setBinance(Binance binance) { this.binance = binance; }
    public Database getDatabase() { return database; }
    public void setDatabase(Database database) { this.database = database; }
    public Warmup getWarmup() { return warmup; }
    public void setWarmup(Warmup warmup) { this.warmup = warmup; }
    public Depth getDepth() { return depth; }
    public void setDepth(Depth depth) { this.depth = depth; }
    public int getRefreshRateSeconds() { return refreshRateSeconds; }
    public void setRefreshRateSeconds(int refreshRateSeconds) { this.refreshRateSeconds = refreshRateSeconds; }
    public int getMemoryLimitMb() { return memoryLimitMb; }
    public void setMemoryLimitMb(int memoryLimitMb) { this.memoryLimitMb = memoryLimitMb; }
    public int getGcIntervalLoops() { return gcIntervalLoops; }
    public void setGcIntervalLoops(int gcIntervalLoops) { this.gcIntervalLoops = gcIntervalLoops; }
    public Healthcheck getHealthcheck() { return healthcheck; }
    public void setHealthcheck(Healthcheck healthcheck) { this.healthcheck = healthcheck; }
    public Trigger getTrigger() { return trigger; }
    public void setTrigger(Trigger trigger) { this.trigger = trigger; }
}
