# Quanta Candle

Deterministic, validation-first crypto OHLCV data collector built with .NET and Docker.

QuantaCandle is a production-minded market data pipeline designed to collect, validate, and store cryptocurrency candle data with reproducibility and operational simplicity in mind.

This is not a trading bot.
This is infrastructure.

## Purpose

Public exchange exports and third-party datasets often contain gaps, inconsistencies, rounding issues, or silent data corruption.

Quanta Candle aims to:
- Collect raw OHLCV candles directly from exchanges
- Validate structural integrity (gaps, overlaps, ordering, duplicates)
- Normalize and store clean datasets
- Support reproducible research and quantitative workflows
- Run reliably in containerized Linux environments (AWS-ready)

## Design Principles

- Deterministic processing: same input -> same output
- Validation-first: don't trust upstream data blindly
- Minimal moving parts: simple deployment, predictable behavior
- Infrastructure over hype: correctness and reliability

## Tech Stack

- .NET (worker/service architecture)
- Docker / Docker Compose
- Linux-first runtime
- GitHub Actions (CI)
- AWS ECS/Fargate (deployment target)

## Developer Setup

After cloning this repository, run once:

```bash
bash setup-hooks.sh
```

This enables mandatory file structure validation on every commit.

See `GIT_HOOKS.md` for details, including GitHub Actions enforcement (recommended as the mandatory gate) and the `post-checkout` recovery hook.

## Executable usage

### QuantaCandle.TradeRecorder

Trade Recorder has no subcommands. The instrument is the first positional argument, the exchange is selected with `--exchange` or `-x`, and the sink is selected with `--sink` or `-to`.

Record trades to a local directory and keep running until you stop the process:

```powershell
dotnet run -c Release --project src\QuantaCandle.TradeRecorder -- BTCUSDT --exchange Binance --sink file --outDir trade-data
```

Run a bounded local collection that exits on its own after 30 seconds:

```powershell
dotnet run -c Release --project src\QuantaCandle.TradeRecorder -- BTCUSDT --exchange Binance --duration 30s --sink file --outDir trade-data
```

Record trades and dispatch finalized files to S3:

```powershell
dotnet run -c Release --project src\QuantaCandle.TradeRecorder -- BTCUSDT --exchange Binance --sink s3 --s3Bucket quanta-candle-bucket-eu-north-1 --s3Prefix trade-data/
```

Use the sink alias and Binance US websocket endpoint for a local US smoke test:

```powershell
dotnet run -c Release --project src\QuantaCandle.TradeRecorder -- BTCUSDT -x Binance -to file --outDir trade-data --binanceWsBase wss://stream.binance.us:9443
```

Working directory structure:
- Trade inputs and recorder output: `<working-dir>\trade-data\<exchange>\<instrument>\<yyyy-MM-dd>.jsonl`
- Trade scratch checkpoint: `<working-dir>\trade-data\<exchange>\<instrument>\qc-scratch.jsonl`
- Candle outputs: `<working-dir>\candle-data\<exchange>\<instrument>\<yyyy-MM-dd>.csv`

### QuantaCandle.CLI

The CLI executable is `qc`. It uses positional subcommands:
- `qc candlize <instrument>`
- `qc scan <instrument>`
- `qc heal <instrument>`

Generate deterministic 1-minute candles from collected trades (CSV output):

```powershell
dotnet run -c Release --project src\QuantaCandle.CLI -- candlize BTC-USDT --exchange Binance --workDir .
```

Scan locally recorded trade files for numeric trade ID gaps without modifying any files:

```powershell
dotnet run -c Release --project src\QuantaCandle.CLI -- scan BTC-USDT --exchange Binance --workDir .
```

Scan a specific work directory and review the per-gap summary output:

```powershell
$workDir = Join-Path (Get-Location) "tmp-e2e"
dotnet run -c Release --project src\QuantaCandle.CLI -- scan BTC-USDT --exchange Binance --workDir $workDir
```

Heal bounded trade ID gaps for a specific date:

```powershell
dotnet run -c Release --project src\QuantaCandle.CLI -- heal BTC-USDT --exchange Binance --workDir . --dates 2026-03-28
```

Trade input layout used by `qc`: `<workDir>\trade-data\<exchange>\<INSTRUMENT>\<yyyy-MM-dd>.jsonl`

Default candle output format (CSV): `<workDir>\candle-data\<exchange>\<INSTRUMENT>\<yyyy-MM-dd>.csv`

Current output formats:
- CSV is currently the default and only documented workflow for `qc`.
- JSONL generation is not exposed by the current `qc` command surface.

CSV header columns:
- `OpenTimeUtc,Instrument,Open,High,Low,Close,Volume,TradeCount`

Gap minutes (no trades) are emitted as:
- CSV: `Open,High,Low,Close` are empty; `Volume=0`; `TradeCount=0`
- JSONL: `open/high/low/close = null`; `volume = 0`; `tradeCount = 0`

## Local end-to-end validation

From the project root (PowerShell), run:

```powershell
$root = Join-Path (Get-Location) "tmp-e2e"
$workDir = $root
New-Item -ItemType Directory -Force -Path $workDir | Out-Null

dotnet run -c Release --project src\QuantaCandle.TradeRecorder -- BTCUSDT --exchange Binance --sink file --outDir (Join-Path $workDir "trade-data") --binanceWsBase wss://stream.binance.us:9443

# Stop it manually after a short sample window, then generate candles from the recorded trades.

dotnet run -c Release --project src\QuantaCandle.CLI -- scan BTC-USDT --exchange Binance --workDir $workDir
dotnet run -c Release --project src\QuantaCandle.CLI -- candlize BTC-USDT --exchange Binance --workDir $workDir

Get-ChildItem -Recurse (Join-Path $workDir "candle-data") -Filter *.csv | Sort-Object FullName
$firstCandleFile = Get-ChildItem -Recurse (Join-Path $workDir "candle-data\binance\BTC-USDT") -Filter *.csv | Sort-Object FullName | Select-Object -First 1
Get-Content $firstCandleFile.FullName | Select-Object -First 20
```

Use `wss://stream.binance.com:9443` instead of `wss://stream.binance.us:9443` if you are outside the US.

## AWS Deployment (Manual Infra)

Runtime deployment flow:
1. Push code to GitHub.
2. GitHub Actions builds and pushes the container image to the existing ECR repository (`.github/workflows/container-ecr.yml`).
3. In the AWS ECS web console, force a new deployment on the existing Trade Recorder service when rollout is needed.

The Trade Recorder process is intended to run as a long-lived ECS Service. It keeps collecting until ECS stops or replaces the task, at which point the host shuts down gracefully.

The ECS runtime remains S3-based for trade collection output (`--sink s3` with bucket/prefix configured in the task definition/runtime settings).
Use `--duration` only for explicitly bounded runs such as local smoke tests.

## CI: Build and publish to GHCR

Workflow: `.github/workflows/docker-ghcr.yml`

What it does:
- Triggers on `push` to `master` and `workflow_dispatch`
- Builds the root `Dockerfile`
- Publishes `ghcr.io/<owner-lowercase>/quantacandle:latest` (default branch only)
- Publishes `ghcr.io/<owner-lowercase>/quantacandle:<full-commit-sha>`

GHCR settings:
- The workflow uses `GITHUB_TOKEN` with `packages: write` permission.
- `<owner-lowercase>` is the lowercase form of `github.repository_owner`.
- Package visibility and pull access are managed in GitHub package settings (`Packages` -> `quantacandle`).
- For public pulls, set the package visibility to public.

## License

MIT

