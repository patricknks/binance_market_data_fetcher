# binance_market_data_fetcher

Spring Boot service that collects Binance market data for one symbol and timeframe and stores
it in PostgreSQL, one row per candle. Each row joins:

- **OHLCV klines**: spot, or USD-M perpetual when the symbol ends with `.P`. Derived columns
  are computed from them (avg trade size, aggressive buy ratio, price efficiency, trade
  imbalance, iceberg score, price change, upside probability).
- **Open interest**, **top long/short position ratio** and **CMC circulating supply** from
  the futures endpoints.
- **Funding rate**, forward-filled so every candle carries the rate in effect.
- **Order book depth** (optional): average cumulative bid/ask notional within
  0.2 / 1 / 2 / 3 / 4 / 5 % of the mid price, plus the number of snapshots averaged.

After each new candle it can notify a downstream indicator service over TCP.

Current version: **2.1.0** (`target/binance_data_fetcher-2.1.0.jar`).

## How it works

| Component | Schedule | What it does |
|-----------|----------|--------------|
| `WarmupService` | inside the fetch loop | Backfills history from the `data.binance.vision` archives while the table is empty or its newest row is older than `app.warmup.stale-threshold-days`. Creates missing monthly partitions. |
| `DataFetcherScheduler` | every `app.refresh-rate-seconds` (skips seconds 56–59) | Realtime loop: fetches klines, OI, long/short and funding from REST since the last stored candle, upserts them, and sends `ping` to the indicator service when a new candle closes. |
| `DepthSampler` | seconds 1 and 31 of every minute (UTC) | Takes a 1000-level snapshot from `/fapi/v1/depth`. That depth only reaches about ±0.45%, so realtime rows get only the 0.2% band. |
| `DepthArchiveSyncService` | checks every minute, runs every `app.depth.sync-interval-minutes` | When a day's `bookDepth` archive is published (the next day), it overwrites the realtime 0.2% values, fills the 1–5% bands, and logs how far the realtime values were from the archive. It waits while warmup or the realtime catch-up is still running. |
| `HealthCheckListener` | always | TCP listener on `app.healthcheck.port`. Replies `OK` when the main loop's last heartbeat was within the last 10 s, otherwise `FAIL`. |
| `ResourceMonitor` | after each loop | Forces GC every `app.gc-interval-loops` loops. Exits with code **99** when the heap exceeds `app.memory-limit-mb`. |

Rows are upserted on `UNIQUE (BUSINESS_DATE, UNIX_DATE, UTC_DATE)`. `BUSINESS_DATE` is the
candle open time in Asia/Jakarta, and `UTC_DATE` / `UNIX_DATE` are the same instant in UTC.

### Data availability

- USD-M perpetual klines start in 2020-01. ETHUSDT spot klines start in 2017-08.
- Open interest and long/short archives start on 2021-12-01. REST serves only the last 30 days, so earlier rows stay null. CMC supply has no archive at all.
- The `bookDepth` archive starts on 2023-01-01, and its 0.2% band starts on 2026-01-16. Some days are missing or only partial.

## Project structure

```
config/
  application.properties        # runtime config used by start.bat (edit this, no rebuild)
  ddl/
    create_partition_table_with_indexes.ddl   # monthly partition + index procedure
    tables_perp_1h_new.ddl      # perp 1h tables WITH the 13 depth columns (current)
    tables_perp.ddl             # perp tables without depth (legacy)
    tables.ddl                  # spot tables without depth (legacy)
src/main/java/com/trading/
  DataFetcherApplication.java   # entry point, sets app.home (base folder for logs/)
  config/AppConfig.java         # all app.* properties
  scheduler/DataFetcherScheduler.java
  service/
    BinanceService.java         # REST calls with retry + exponential backoff
    BinanceVisionService.java   # archive (zip/csv) downloads
    WarmupService.java          # historical backfill
    DatabaseService.java        # PostgreSQL upserts, partitions, application time
    HealthCheckListener.java
    ResourceMonitor.java
  depth/                        # depth sampler, aggregation, archive sync
  util/TimeFrames.java          # interval maths
src/main/resources/
  application.properties        # bundled defaults (depth off, legacy tables)
  logback-spring.xml            # logs/application.log, daily + 10 MB rolling
start.bat / stop.bat            # start in background (PID in run.pid) / stop the process tree
```

## Prerequisites

- JDK 11 or newer to build (the code targets Java 11). JDK 21 is fine to run it.
- Maven 3.6+
- PostgreSQL 12+ with the `algo_trade` database and the `algo_oltp` schema
- Network access to `api.binance.com`, `fapi.binance.com` and `data.binance.vision`

## Setup

### 1. Database password

The password is read from the `DB_PASSWORD` environment variable and is never committed.
Set it once for your Windows user, then open a new terminal:

```powershell
[Environment]::SetEnvironmentVariable('DB_PASSWORD', '<your password>', 'User')
```

The URL and username are set in `config/application.properties`
(`jdbc:postgresql://127.0.0.1:5432/algo_trade`, user `postgres`).

### 2. Create the tables

Run these in order with psql or any SQL client:

1. `config/ddl/create_partition_table_with_indexes.ddl` creates the procedure, which is shared by every table.
2. `config/ddl/tables_perp_1h_new.ddl` creates the market-data table (with depth columns) and its application-time table, plus monthly partitions from 2020-01 to 2027-12.

Every DDL starts with `SET search_path TO algo_oltp, public`. Table names passed to the
procedure must be lower case.

> Never `ALTER` an existing table to add the depth columns. Create a new set of tables and
> point `app.database.table.*` at them. The DDLs drop and recreate their own tables, so you
> can rerun them on a test table.

### 3. Configure

Edit `config/application.properties`. The settings that matter most:

| Property | Current value | Notes |
|----------|---------------|-------|
| `app.binance.symbol` | `ETHUSDT.P` | `.P` selects the USD-M perpetual for OHLCV. Without it, spot. |
| `app.binance.interval` | `1h` | Valid values: 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d. Use a separate set of tables per timeframe. |
| `app.database.schema` | `algo_oltp` | |
| `app.database.table.market-data` | `market_data_ethusdtp_1h` | Must have the depth columns if depth is enabled. |
| `app.database.table.application-time` | `application_time_ethusdtp_1h` | |
| `app.warmup.start-date` | `2020-01-01` | Earliest candle to backfill into an empty table. |
| `app.warmup.stale-threshold-days` | `14` | Warmup runs when the newest row is older than this. |
| `app.depth.enabled` | `true` | Keep it `false` for tables without depth columns. |
| `app.depth.sync-lookback-days` | `30` | Must be larger than `stale-threshold-days`. |
| `spring.task.scheduling.pool.size` | `3` | Needed so the depth jobs do not queue behind the fetch loop. |
| `server.port` / `app.healthcheck.port` | `8090` / `8091` | Must be unique per instance. |
| `app.trigger.host` / `app.trigger.port` | `127.0.0.1:8070` | Indicator service this app connects to. It does not bind this port. |
| `app.refresh-rate-seconds` | `30` | Realtime loop period. |
| `app.memory-limit-mb` | `1024` | Heap limit before exit code 99. `0` disables it. |

`start.bat` passes `--spring.config.location=file:./config/application.properties`. That
file *replaces* the bundled `src/main/resources/application.properties` instead of being
merged with it, so it has to contain every property.

## Build

```powershell
mvn clean package
```

This runs the unit tests. Add `-DskipTests` to skip them. The output is
`target/binance_data_fetcher-<version>.jar`.

## Run

```powershell
start.bat     # starts in the background, writes the PID to run.pid, stdout/stderr to logs\
stop.bat      # taskkill /T on that PID
```

`start.bat` uses the newest `binance_data_fetcher-*.jar`, looking in `target\` first and
then in the script folder. You never need to edit it when the version changes.

To run in the foreground instead:

```powershell
java -jar target/binance_data_fetcher-2.1.0.jar --spring.config.location=file:./config/application.properties
```

Stop it with `stop.bat`, or with `taskkill /PID <pid> /T /F`. The `java` on PATH (Oracle
`javapath`) is a launcher that starts a child JVM. Killing only the parent PID leaves the
app running. Do not use `taskkill /IM java.exe`, because that also kills every other Java
instance on the machine.

### Running several instances

One instance handles one symbol and one timeframe. For another pair or timeframe, use a
separate folder with its own `config/application.properties`, its own tables, and its own
`server.port` / `app.healthcheck.port`. Keep `app.trigger.port` the same for all of them.

### Deploy

Copy the JAR into the deploy folder, next to its `config\` and `start.bat`, then restart
with `stop.bat` and `start.bat`.

### Fill depth for a table right away

The archive sync first runs `app.depth.sync-initial-delay-seconds` after startup. To run it
once against a table without disturbing a running instance, start a second copy briefly on
other ports:

```powershell
java -jar target/binance_data_fetcher-2.1.0.jar --spring.config.location=file:./config/application.properties `
  --app.trigger.port=1 --server.port=18090 --app.healthcheck.port=18091 --app.depth.sync-initial-delay-seconds=5
```

The second copy overwrites the current candle's provisional 0.2% value with a value based
on a single sample.

## Monitoring

- Logs go to `logs/application.log` in the folder `start.bat` runs from, rolled daily and at 10 MB (3 days, 100 MB cap). Console output from `start.bat` goes to `logs/stdout.log`. `stdout.log` is not rotated, so it keeps growing.
- Useful log prefixes: `[Warmup]`, `[DepthSync]`, `[HealthCheck]`, `[TriggerClient]`, `START LOOP`.
- To test the health check: `Test-NetConnection 127.0.0.1 -Port 8091`, or open a TCP connection and read `OK` or `FAIL`.

## Troubleshooting

| Symptom | Check |
|---------|-------|
| `Could not resolve placeholder 'DB_PASSWORD'` | The environment variable is not set in the shell that ran `start.bat`. Set it and open a new terminal. |
| `Address already in use` | `server.port` or `app.healthcheck.port` is already taken by another instance. |
| Depth columns stay null | Check that `app.depth.enabled=true`, that the table has the depth columns, and the `[DepthSync]` log lines. The 1–5% bands only appear the day after. |
| Open-interest columns null on warmup rows | Either the row is before 2021-12-01, or `app.warmup.metrics-offset-minutes` does not match the timeframe. The warmup log warns about this. |
| Process exits with code 99 | The heap went over `app.memory-limit-mb`. |
| `mvn clean` fails because the JAR is locked | An instance started from `target\` is still running. Stop it with `stop.bat`. |
