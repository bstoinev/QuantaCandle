# Step 3 manual verification (trade pipeline)

## Run (null sink)

```powershell
dotnet run -c Release --project src\QuantaCandle.CLI -- collect-trades `
  --instrument BTCUSDT `
  --duration 10s `
  --rate 20 `
  --batchSize 50 `
  --flushInterval 1s `
  --sink null
```

Expect:
- Process exits after `--duration` (or earlier on Ctrl+C) without hanging.
- Summary prints non-zero `Trades received`, `Trades written`, and `Batches flushed`.

## Run (file sink)

```powershell
dotnet run -c Release --project src\QuantaCandle.CLI -- collect-trades `
  --instrument BTCUSDT `
  --duration 10s `
  --rate 20 `
  --batchSize 50 `
  --flushInterval 1s `
  --sink file `
  --outDir trades-out
```

Inspect:
- `trades-out\BTC-USDT\YYYY-MM-DD.jsonl` exists.
- Each line is JSON; number of lines should match `Trades written` (for a fresh directory).

## Shutdown flush check (no periodic flush)

```powershell
dotnet run -c Release --project src\QuantaCandle.CLI -- collect-trades `
  --instrument BTCUSDT `
  --duration 5s `
  --rate 20 `
  --batchSize 100000 `
  --flushInterval 01:00:00 `
  --sink file `
  --outDir trades-out
```

Expect:
- `Batches flushed` is `1` (flush happens on shutdown).
- The day partition file still receives lines even though batch size was never reached.

