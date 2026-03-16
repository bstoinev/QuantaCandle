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
- AWS EC2 (deployment target)

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

Generate deterministic 1-minute candles from collected trades:

```bash
docker run --rm -v $(pwd)/trades-out:/trades -v $(pwd)/candles-out:/candles quantacandle generate-candles --source binance --timeframe 1m --inDir /trades --outDir /candles
```

Input format (trade files): `<inDir>/<INSTRUMENT>/<yyyy-MM-dd>.jsonl`

Output format (candle files): `<outDir>/binance/1m/<INSTRUMENT>/<yyyy-MM-dd>.jsonl`

Each candle line includes:
- `openTime` (UTC minute boundary)
- `open`, `high`, `low`, `close`
- `volume`
- `tradeCount`

Gap minutes (no trades) are emitted with `open/high/low/close = null`, `volume = 0`, and `tradeCount = 0`.

## CI: Build and publish to GHCR

Workflow: `.github/workflows/docker-ghcr.yml`

What it does:
- Triggers on `push` to `main` and `workflow_dispatch`
- Builds the root `Dockerfile`
- Publishes `ghcr.io/<owner-lowercase>/quantacandle:latest` (default branch only)
- Publishes `ghcr.io/<owner-lowercase>/quantacandle:<full-commit-sha>`

GHCR settings:
- The workflow uses `GITHUB_TOKEN` with `packages: write` permission.
- `<owner-lowercase>` is the lowercase form of `github.repository_owner`.
- Package visibility and pull access are managed in GitHub package settings (`Packages` -> `quantacandle`).
- For public pulls, set the package visibility to public.

## CI: Validation (no publish)

Workflow: `.github/workflows/validation.yml`

What it does:
- Triggers on `push` and `pull_request`
- Restores dependencies and builds `QuantaCandle.slnx` in `Release`
- Runs tests in `Release` with `--no-build`
- Builds the root Docker image with `docker build` (no push)

## License

MIT

