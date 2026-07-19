import pandas as pd
from tvDatafeed import TvDatafeed, Interval
from datetime import datetime
import socket, ssl, time, os

# === TV Datafeed Login ===
username = "EGAVSIV"
password = "Eric$1234"
tv = TvDatafeed(username, password)

# === Only Required Timeframes ===
interval_map = {
    'D': Interval.in_daily,
    'W': Interval.in_weekly,
    'M': Interval.in_monthly
}

# === Output Folder ===
output_dir = "stock_data"
os.makedirs(output_dir, exist_ok=True)

# === Delay for retries ===
retry_delay = 3  # seconds

# === Fetch with infinite retry ===
def fetch_with_retry(symbol, label, interval):
    attempt = 1
    while True:
        try:
            df = tv.get_hist(symbol=symbol, exchange='NSE', interval=interval, n_bars=1000)
            if df is not None and not df.empty:
                df['timeframe'] = label
                return df
            else:
                print(f"⚠️ Empty data for {symbol} [{label}] (Attempt {attempt})")
        except (socket.timeout, ssl.SSLError):
            print(f"⏳ Timeout for {symbol} [{label}] (Attempt {attempt})")
        attempt += 1
        time.sleep(retry_delay)

# === Fetch and Save for One Symbol ===
def fetch_and_save_all(symbol):
    symbol_data = {}

    for label, interval in interval_map.items():
        df = fetch_with_retry(symbol, label, interval)
        if df is not None:
            symbol_data[label] = df

    if len(symbol_data) == len(interval_map):  # All timeframes received
        df_all = pd.concat(symbol_data.values(), keys=symbol_data.keys(), names=['Timeframe'])
        filepath = os.path.join(output_dir, f"{symbol}.parquet")
        df_all.to_parquet(filepath)
        print(f"✅ Saved: {symbol}")
    else:
        print(f"❌ Skipped {symbol} due to missing data.")

# === Symbols List (Partial for testing) ===
symbols = [
    'PIDILITIND','PERSISTENT','PETRONET','LTIM','INDIANB','INDHOTEL','HFCL','HAVELLS','BRITANNIA','BSE',
    'CAMS','CANBK','CDSL','CGPOWER','CHOLAFIN','CIPLA','COALINDIA','COFORGE','COLPAL','CONCOR','CROMPTON',
    'CUMMINSIND','CYIENT','DABUR','DALBHARAT','DELHIVERY','DIVISLAB','DIXON','DLF','DMART','DRREDDY',
    'EICHERMOT','ETERNAL','EXIDEIND','FEDERALBNK','FORTIS','GAIL','GLENMARK','GMRAIRPORT','GODREJCP','GODREJPROP',
    'GRASIM','HAL','HDFCAMC','HDFCBANK','HDFCLIFE','HEROMOTOCO','HINDALCO','HINDPETRO','HINDUNILVR','HINDZINC',
    'HUDCO','ICICIBANK','ICICIGI','ICICIPRULI','IDEA','IDFCFIRSTB','IEX','IGL','IIFL','INDIGO','INDUSINDBK',
    'INDUSTOWER','INFY','INOXWIND','IOC','IRCTC','IREDA','IRFC','ITC','JINDALSTEL','JIOFIN','JSWENERGY',
    'JSWSTEEL','JUBLFOOD','KALYANKJIL','KAYNES','KEI','KFINTECH','KOTAKBANK','KPITTECH','LAURUSLABS',
    'LICHSGFIN','LICI','LODHA','LT','LTF','LUPIN','M&M','MANAPPURAM','MANKIND','MARICO','MARUTI','MAXHEALTH',
    'MAZDOCK','MCX','MFSL','MOTHERSON','MPHASIS','MUTHOOTFIN','NATIONALUM','NAUKRI','NBCC','NCC','NESTLEIND',
    'NMDC','NTPC','NUVAMA','NYKAA','OBEROIRLTY','OFSS','OIL','ONGC','PAGEIND','PATANJALI','PAYTM',
    'PFC','PGEL','PHOENIXLTD','PIIND','PNB','PNBHOUSING','POLICYBZR','POLYCAB','NHPC','HCLTECH','POWERGRID',
    'PPLPHARMA','PRESTIGE','RBLBANK','RECLTD','RELIANCE','RVNL','SAIL','SAMMAANCAP','SBICARD','SBILIFE',
    'SBIN','SHREECEM','SHRIRAMFIN','SIEMENS','SOLARINDS','SONACOMS','SRF','SUNPHARMA','SUPREMEIND','SUZLON',
    'SYNGENE','TATACONSUM','TATAELXSI','TATAMOTORS','TATAPOWER','TATASTEEL','TATATECH','TCS','TECHM','TIINDIA',
    'TITAGARH','TITAN','TORNTPHARM','TORNTPOWER','TRENT','TVSMOTOR','ULTRACEMCO','UNIONBANK','UNITDSPR',
    'UNOMINDA','UPL','VBL','VEDL','VOLTAS','WIPRO','YESBANK','ZYDUSLIFE','BANKNIFTY','CNXFINANCE','CNXMIDCAP',
    'NIFTY','NIFTYJR','360ONE','ABB','ABCAPITAL','ADANIENSOL','ADANIENT','ADANIGREEN','ADANIPORTS','ALKEM',
    'AMBER','AMBUJACEM','ANGELONE','APLAPOLLO','APOLLOHOSP','ASHOKLEY','ASIANPAINT','ASTRAL','AUBANK',
    'AUROPHARMA','AXISBANK','BAJAJ_AUTO','BAJAJFINSV','BAJFINANCE','BANDHANBNK','BANKBARODA','BANKINDIA',
    'BDL','BEL','BHARATFORG','BHARTIARTL','BHEL','BIOCON','BLUESTARCO','BOSCHLTD','BPCL','BAJAJHLDNG','WAAREEENER','PREMIERENE','SWIGGY'
]

# === Run for All Symbols ===
for symbol in symbols:
    fetch_and_save_all(symbol)

