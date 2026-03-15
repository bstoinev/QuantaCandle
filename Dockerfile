# syntax=docker/dockerfile:1

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY Directory.Build.props ./
COPY QuantaCandle.slnx ./

COPY src/QuantaCandle.CLI/QuantaCandle.CLI.csproj src/QuantaCandle.CLI/
COPY src/QuantaCandle.Core/QuantaCandle.Core.csproj src/QuantaCandle.Core/
COPY src/QuantaCandle.Exchange.Binance/QuantaCandle.Exchange.Binance.csproj src/QuantaCandle.Exchange.Binance/
COPY src/QuantaCandle.Exchange.Bitstamp/QuantaCandle.Exchange.Bitstamp.csproj src/QuantaCandle.Exchange.Bitstamp/
COPY src/QuantaCandle.Infra/QuantaCandle.Infra.csproj src/QuantaCandle.Infra/
COPY src/QuantaCandle.Service/QuantaCandle.Service.csproj src/QuantaCandle.Service/
COPY src/QuantaCandle.Service.Stubs/QuantaCandle.Service.Stubs.csproj src/QuantaCandle.Service.Stubs/
COPY src/QuantaCandle.Storage.Csv/QuantaCandle.Storage.Csv.csproj src/QuantaCandle.Storage.Csv/

RUN dotnet restore src/QuantaCandle.CLI/QuantaCandle.CLI.csproj

COPY . .

RUN dotnet publish src/QuantaCandle.CLI/QuantaCandle.CLI.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS runtime
WORKDIR /app

ENV DOTNET_EnableDiagnostics=0

RUN mkdir -p /data && chmod 777 /data

COPY --from=build /app/publish/ ./

ENTRYPOINT ["dotnet", "QuantaCandle.CLI.dll"]
