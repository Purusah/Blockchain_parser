DEBUG = True

if DEBUG:
    coins = {"ETH", "QTUM", "BTC"}
    databases = {
        "ETH": "pmesETHBalancer",
        "QTUM": "pmesQTUMBalancer",
        "BTC": "pmesBTCBalancer"}
else:
    coins = {"ETHTEST", "QTUMTEST", "BTCTEST"}
    databases = {
        "ETHTEST": "pmesETHTESTBalancer",
        "QTUMTEST": "pmesQTUMTESTBalancer",
        "BTCTEST": "pmesBTCTESTBalancer"}

