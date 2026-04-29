# QuantaCandle

QuantaCandle is trade-first infrastructure.

- `QuantaCandle.TradeRecorder` / `tr` records raw exchange trades into JSONL.
- `QuantaCandle.CLI` / `qc` scans and heals local trade files, then generates OHLCV candles from those trades.

It does not record raw OHLCV candles from the exchange.

## Repository layout

- Source: `src\`
- Trade Recorder project: `src\QuantaCandle.TradeRecorder\`
- CLI project: `src\QuantaCandle.CLI\`
- Docker build file: `DOCKERFILE`
- GitHub Actions workflows:
  - `.github\workflows\docker2ghcr.yml`
  - `.github\workflows\docker2ecr.yml`
  - `.github\workflows\file-structure-validation.yml`

## Developer setup

After cloning, run once:

```bash
bash setup-hooks.sh
```

See `GIT_HOOKS.md` for hook behavior.

## Executables

### `QuantaCandle.TradeRecorder` / `tr`

Help:

```cmd
dotnet run -c Release --project src\QuantaCandle.TradeRecorder -- --help
```

Current command shape:

```text
BTCUSDT --exchange Binance|-x Binance [--duration 10m] [--capacity 10000] [--batchSize 500] [--flushInterval 1s] [--checkpointInterval 1h] [--cacheSize 1024] [--sink file|s3|null|-to file|s3|null] [--outDir trade-data]
```

Operational notes:

- Instrument is the first positional argument.
- Exchange is required and selected with `--exchange` or `-x`.
- Sink is selected with `--sink` or `-to`.
- Default sink is `file`.
- Supported sinks are `file`, `s3`, and `null`.
- Omit `--duration` for a long-running recorder.
- Use `--duration` only for bounded smoke tests.
- `--checkpointInterval` controls scratch checkpoint cadence.
- `--cacheSize` controls the in-memory deduplication cache size.
- `--binanceWsBase` overrides the Binance websocket base URL.
- Default Binance global websocket base is `wss://stream.binance.com:9443`.
- Binance US websocket base is `wss://stream.binance.us:9443`.
- `--rate` is a Stub/testing source setting; it does not throttle Binance websocket ingestion.

Examples:

Long-running local file sink:

```cmd
dotnet run -c Release --project src\QuantaCandle.TradeRecorder -- BTCUSDT --exchange Binance --sink file --outDir trade-data --checkpointInterval 1h --cacheSize 1024
```

Bounded smoke test:

```cmd
dotnet run -c Release --project src\QuantaCandle.TradeRecorder -- BTCUSDT --exchange Binance --sink file --outDir trade-data --duration 30s --checkpointInterval 30s --cacheSize 256
```

S3 sink using environment variables:

```cmd
set QUANTA_CANDLE_S3_BUCKET=my-trade-bucket
set QUANTA_CANDLE_S3_PREFIX=trade-data/
dotnet run -c Release --project src\QuantaCandle.TradeRecorder -- BTCUSDT --exchange Binance --sink s3 --outDir trade-data --checkpointInterval 1h --cacheSize 1024
```

Binance US websocket endpoint:

```cmd
dotnet run -c Release --project src\QuantaCandle.TradeRecorder -- BTCUSDT -x Binance -to file --outDir trade-data --binanceWsBase wss://stream.binance.us:9443
```

S3 settings currently supported:

- `QUANTA_CANDLE_S3_BUCKET`
- `QUANTA_CANDLE_S3_PREFIX`

The recorder also accepts `--s3Bucket` and `--s3Prefix` explicitly.

### `QuantaCandle.CLI` / `qc`

Help:

```cmd
dotnet run -c Release --project src\QuantaCandle.CLI -- --help
```

Current subcommands:

- `qc candlize <instrument>`
- `qc scan <instrument>`
- `qc heal <instrument>`

Aliases:

- `--exchange` / `-x`
- `--workDir` / `-dir`
- `--dates` / `-on`
- `--begin` / `-from`
- `--end` / `-to`
- `--timeFrame` / `-time`

`candlize` requires `-time` or `--timeFrame`.

Examples:

```cmd
dotnet run -c Release --project src\QuantaCandle.CLI -- candlize BTC-USDT -x Binance -dir . -time 10s
dotnet run -c Release --project src\QuantaCandle.CLI -- candlize BTC-USDT -x Binance -dir . -time 1m -from 2026-04-10 -to 2026-04-11
dotnet run -c Release --project src\QuantaCandle.CLI -- scan BTC-USDT -x Binance -dir . -on 2026-04-10
dotnet run -c Release --project src\QuantaCandle.CLI -- heal BTC-USDT -x Binance -dir . -on 2026-04-10
```

Notes:

- `scan` reports gaps and does not modify files.
- `heal` is currently implemented only for `Binance`.
- `heal` can bootstrap explicitly requested missing UTC day files before scanning/healing them.
- The CLI always runs `candlize` with CSV output today.
- The generator supports JSONL internally, but the current CLI does not expose a format option.

## File layout

Trade Recorder runtime files under `<workDir>`:

- Scratch checkpoint: `<workDir>\trade-data\<exchange>\<instrument>\qc-scratch.jsonl`
- Finalized day file: `<workDir>\trade-data\<exchange>\<instrument>\yyyy-MM-dd.jsonl`
- Partial finalized day file after incomplete rollover healing: `<workDir>\trade-data\<exchange>\<instrument>\yyyy-MM-dd.partial.jsonl`
- Snapshot checkpoint export: `<workDir>\trade-data\<exchange>\<instrument>\yyyy-MM-dd.HHmmssfff.jsonl`

`qc` candle output under `<workDir>`:

- CSV output directory: `<workDir>\candle-data\<exchange>\<instrument>\<timeframe>\`
- Current CSV file naming: `yyyyMMdd-yyyyMMdd.csv`

Examples:

- Single-day output: `<workDir>\candle-data\binance\BTC-USDT\1m\20260410-20260410.csv`
- Multi-day output: `<workDir>\candle-data\binance\BTC-USDT\1m\20260410-20260411.csv`

`candlize` writes one combined CSV file for the resolved date scope, not one CSV file per day.

The CLI reads trades from:

```text
<workDir>\trade-data\<exchange>\<instrument>\yyyy-MM-dd.jsonl
```

The candlize date-range resolver ignores `qc-scratch.jsonl` and `.partial.jsonl` files.

## Candle output

Current CSV header emitted by the generator:

```text
OpenTimeUtc,Instrument,Open,High,Low,Close,BaseVolume,QuoteVolume,BuyQuoteVolume,SellQuoteVolume,TradeCount
```

Gap buckets with no trades are emitted with:

- Empty `Open`, `High`, `Low`, `Close`
- `BaseVolume=0`
- `QuoteVolume=0`
- `BuyQuoteVolume=0`
- `SellQuoteVolume=0`
- `TradeCount=0`

## Local workflows

### Local console smoke test

Run the recorder for a short bounded sample:

```cmd
dotnet run -c Release --project src\QuantaCandle.TradeRecorder -- BTCUSDT --exchange Binance --sink file --outDir trade-data --duration 30s --checkpointInterval 30s --cacheSize 256
```

Then scan and generate candles from the current directory:

```cmd
dotnet run -c Release --project src\QuantaCandle.CLI -- scan BTC-USDT -x Binance -dir . -on 2026-04-10
dotnet run -c Release --project src\QuantaCandle.CLI -- candlize BTC-USDT -x Binance -dir . -time 1m -from 2026-04-10 -to 2026-04-10
```

### Local Trade Recorder service setup

The current app uses console hosting. It does not implement native Windows Service hosting, so do not install it with `sc.exe` as a real Windows Service.

For long-running local Windows operation, use Task Scheduler.

Publish the recorder:

```cmd
dotnet publish src\QuantaCandle.TradeRecorder\QuantaCandle.TradeRecorder.csproj -c Release -r win-x64 --self-contained false -o build-output\tr
```

Example local data directory:

```cmd
mkdir C:\QuantaCandle\data
```

Example launcher file `C:\QuantaCandle\run-trade-recorder.cmd`:

```cmd
@echo off
setlocal
set QUANTA_CANDLE_S3_BUCKET=
set QUANTA_CANDLE_S3_PREFIX=
cd /d C:\QuantaCandle\data
"W:\github-repos\QuantaCandle\build-output\tr\tr.exe" BTCUSDT --exchange Binance --sink file --outDir trade-data --checkpointInterval 1h --cacheSize 1024
```

Create the scheduled task:

```cmd
schtasks /Create /TN "QuantaCandle Trade Recorder BTCUSDT" /SC ONSTART /RU "%USERNAME%" /TR "C:\QuantaCandle\run-trade-recorder.cmd"
```

Start, stop, and rerun the task:

```cmd
schtasks /Run /TN "QuantaCandle Trade Recorder BTCUSDT"
schtasks /End /TN "QuantaCandle Trade Recorder BTCUSDT"
schtasks /Run /TN "QuantaCandle Trade Recorder BTCUSDT"
```

Trade files will appear under:

```text
C:\QuantaCandle\data\trade-data\Binance\BTC-USDT\
```

### Docker image build and run

The repository uses `DOCKERFILE` and publishes `QuantaCandle.TradeRecorder`. The image entrypoint is:

```text
dotnet tr.dll
```

Build:

```cmd
docker build -f DOCKERFILE -t quantacandle-trade-recorder .
```

Run with local bind mount:

```cmd
docker run --rm -it -v %cd%\docker-data:/data -w /data quantacandle-trade-recorder BTCUSDT --exchange Binance --sink file --outDir trade-data --checkpointInterval 1h --cacheSize 1024
```

Run with S3 sink:

```cmd
docker run --rm -it -e QUANTA_CANDLE_S3_BUCKET=my-trade-bucket -e QUANTA_CANDLE_S3_PREFIX=trade-data/ -w /data quantacandle-trade-recorder BTCUSDT --exchange Binance --sink s3 --outDir trade-data --checkpointInterval 1h --cacheSize 1024
```

### AWS ECS / Fargate with S3 sink

Typical task command arguments:

```text
BTCUSDT --exchange Binance --sink s3 --s3Bucket <bucket> --s3Prefix trade-data/
```

Recommended additions:

```text
--outDir trade-data --checkpointInterval 1h --cacheSize 1024
```

High-level AWS permissions required by the task role:

- `s3:PutObject` on the target bucket and prefix

Behavior notes:

- Finalized day files are uploaded to `<prefix>/<exchange>/<instrument>/yyyy-MM-dd.jsonl` and deleted locally after successful upload.
- Partial finalized day files can also be uploaded when rollover healing remains incomplete.
- Snapshot checkpoint files can be uploaded without deleting the local snapshot artifact.

## GitHub Actions container publishing

GHCR workflow:

- File: `.github\workflows\docker2ghcr.yml`
- Trigger: `push` to `master` and `workflow_dispatch`
- Builds `DOCKERFILE`
- Publishes `ghcr.io/<owner-lowercase>/quantacandle:latest` on the default branch
- Publishes `ghcr.io/<owner-lowercase>/quantacandle:<full-commit-sha>`

ECR workflow:

- File: `.github\workflows\docker2ecr.yml`
- Trigger: `push` to `master` and `workflow_dispatch`
- Builds `DOCKERFILE`
- Publishes `latest` and `<commit-sha>` tags to the configured ECR repository

## License

MIT
