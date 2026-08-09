# Gunicorn configuration for HL Trader
# Increased timeout to handle slow startup (candle preloading)
timeout = 120       # 2 minutes — enough for 13 sequential candle fetches
workers = 1         # Single worker — trading app has shared state
bind = "0.0.0.0:8080"
loglevel = "info"
