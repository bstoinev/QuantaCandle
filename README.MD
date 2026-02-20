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
- Validation-first: don’t trust upstream data blindly
- Minimal moving parts: simple deployment, predictable behavior
- Infrastructure over hype: correctness and reliability

## Tech Stack

- .NET (worker/service architecture)
- Docker / Docker Compose
- Linux-first runtime
- GitHub Actions (CI)
- AWS EC2 (deployment target)

## License

MIT