# QuantaCandle

Deterministic, validation-first crypto OHLCV data collector built with .NET and Docker.

QuantaCandle is a production-minded market data pipeline designed to collect, validate, and store cryptocurrency candle data with reproducibility and operational simplicity in mind.

This is not a trading bot.
This is infrastructure.

## Purpose

Public exchange exports and third-party datasets often contain gaps, inconsistencies, rounding issues, or silent data corruption.

QuantaCandle aims to:
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

## Docker (CLI collector)

Build:

```bash
docker build -t quantacandle .
```

Run (writes trade output to `/data` inside the container):

```bash
docker run --rm -v $(pwd)/trades-out:/data quantacandle collect-trades --source binance --instrument BTCUSDT --duration 30s --sink file --outDir /data
```

Generate deterministic 1-minute candles from collected trades (CSV default):

```bash
docker run --rm -v $(pwd)/trades-out:/trades -v $(pwd)/candles-out:/candles quantacandle generate-candles --source binance --timeframe 1m --inDir /trades --outDir /candles
```

Generate candles as JSONL (optional):

```bash
docker run --rm -v $(pwd)/trades-out:/trades -v $(pwd)/candles-out:/candles quantacandle generate-candles --source binance --timeframe 1m --format jsonl --inDir /trades --outDir /candles
```

Input format (trade files): `<inDir>/<INSTRUMENT>/<yyyy-MM-dd>.jsonl`

Default candle output format (CSV): `<outDir>/binance/1m/<INSTRUMENT>/<yyyy-MM-dd>.csv`

JSONL candle output format (when `--format jsonl`): `<outDir>/binance/1m/<INSTRUMENT>/<yyyy-MM-dd>.jsonl`

CSV header columns:
- `OpenTimeUtc,Instrument,Open,High,Low,Close,Volume,TradeCount`

Gap minutes (no trades) are emitted as:
- CSV: `Open,High,Low,Close` are empty; `Volume=0`; `TradeCount=0`
- JSONL: `open/high/low/close = null`; `volume = 0`; `tradeCount = 0`

## Local end-to-end validation

From the project root (PowerShell), run:

```powershell
$root = Join-Path (Get-Location) "tmp-e2e"
$tradeDir = Join-Path $root "trades"
$candleDir = Join-Path $root "candles"
New-Item -ItemType Directory -Force -Path $tradeDir,$candleDir | Out-Null

dotnet run -c Release --project src\QuantaCandle.CLI -- collect-trades --source binance --instrument BTCUSDT --duration 30s --sink file --outDir $tradeDir --binanceWsBase wss://stream.binance.us:9443

dotnet run -c Release --project src\QuantaCandle.CLI -- generate-candles --source binance --timeframe 1m --inDir $tradeDir --outDir $candleDir
dotnet run -c Release --project src\QuantaCandle.CLI -- generate-candles --source binance --timeframe 1m --format jsonl --inDir $tradeDir --outDir $candleDir

Get-ChildItem -Recurse $candleDir -Filter *.csv | Sort-Object FullName
$firstCandleFile = Get-ChildItem -Recurse (Join-Path $candleDir "binance\1m\BTC-USDT") -Filter *.csv | Sort-Object FullName | Select-Object -First 1
Get-Content $firstCandleFile.FullName | Select-Object -First 20
```

Use `wss://stream.binance.com:9443` instead of `wss://stream.binance.us:9443` if you are outside the US.

## AWS Deployment (Manual Infra)

Terraform is retired for this project. AWS infrastructure is managed manually in AWS.

Runtime deployment flow:
1. Push code to GitHub.
2. GitHub Actions builds and pushes the container image to the existing ECR repository (`.github/workflows/container-ecr.yml`).
3. In the AWS ECS web console, force a new deployment on the existing Quanta Candle service when rollout is needed.

The ECS task runtime remains S3-based for trade collection output (`--sink s3` with bucket/prefix configured in the task definition/runtime settings).

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

## CI: Validation (no publish)

Workflow: `.github/workflows/ci-workflow.yml`

What it does:
- Triggers on `push` and `pull_request`
- Restores dependencies and builds `src/QuantaCandle.slnx` in `Release`
- Runs tests in `Release` with `--no-build`
- Builds the root Docker image with `docker build` (no push)

## License

MIT

