aws_region = "eu-north-1"
name_prefix = "quanta-candle"
image_tag = "latest"

vpc_id = "vpc-059dde854a16c3d38"
subnet_ids = [
  "subnet-0a366879031529897",
  "subnet-0b4f5cbbafe80e07a",
]

assign_public_ip = false
schedule_expression = "rate(5 minutes)"
schedule_timezone = "UTC"

trade_source = "binance"
instrument = "BTCUSDT"
duration = "30s"
output_dir = "/data/trades-out"

# For US Binance endpoint, set:
# binance_ws_base = "wss://stream.binance.us:9443"
