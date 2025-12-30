from kiteconnect import KiteConnect, KiteTicker
import pandas as pd
import logging
from datetime import datetime
import requests
import json
import time 

logging.basicConfig(level=logging.DEBUG)

# Kite API credentials
api_key = "z0e38dbis6w8ccz7"
access_token = "HmtKzZcb8CxFb9d4ZxOcI7VhI6kPpIR5"

# Initialize Kite Connect
kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

# Read symbol list
csv_file = "Symbol_List_NFO.csv"
df = pd.read_csv(csv_file)
instrument = "instruments.csv"
output_file = "Data_FUT_NFO.csv"
sleep_interval = 5  # Interval in seconds for fetching quotes   

symbols = df["tradingsymbol"].tolist()
def download_zerodha_instrument():
    url = "https://api.kite.trade/instruments"
    response = requests.get(url)
    if response.status_code == 200:
        with open(instrument, "wb") as file:
            file.write(response.content)
        print("Instrument file saved as instruments.csv")
    else:
        print(f"Failed to download. Status code: {response.status_code}, Message: {response.text}")

download_zerodha_instrument()
inst_df = pd.read_csv(instrument)

def get_current_month_futures():
    # Get current date
    current_date = datetime.now()
    
    # Convert expiry column to datetime
    inst_df['expiry'] = pd.to_datetime(inst_df['expiry'])
    upcoming_exp = inst_df[inst_df['segment']=='NFO-FUT']['expiry'].iloc[0]
    print(f"Upcoming Futures Expiry: {upcoming_exp.strftime('%d-%b-%Y')}")

    # Filter current month futures
    if upcoming_exp.month == current_date.month:
        current_futures = inst_df[
            (inst_df['instrument_type'] == 'FUT') & 
            (inst_df['expiry'].dt.month == current_date.month) &
            (inst_df['expiry'].dt.year == current_date.year)
        ]
    else:
        current_futures = inst_df[
            (inst_df['instrument_type'] == 'FUT') & 
            (inst_df['expiry'].dt.month == current_date.month + 1) &
            (inst_df['expiry'].dt.year == current_date.year)
        ]

    # Get futures only for symbols in AT Symbol List
    symbol_futures = []
    for symbol in df['tradingsymbol']:
        future = current_futures[current_futures['name'] == symbol]
        if not future.empty:
            symbol_futures.append(future.iloc[0])
    
    # Create DataFrame with futures data
    futures_df = pd.DataFrame(symbol_futures)
    
    # Select relevant columns
    columns = ['tradingsymbol', 'name', 'expiry', 'lot_size', 'instrument_token']
    futures_df = futures_df[columns]
    
    print(f"\nCurrent Month Futures Expiry: {futures_df['expiry'].iloc[0].strftime('%d-%b-%Y')}")
    print(f"Number of futures found: {len(futures_df)}")
    
    # Save to CSV
    futures_df.to_csv(output_file, index=False)
    print(f"Futures data saved to {output_file}")
    
    return futures_df
class QuoteTracker:
    def __init__(self, kite, futures_df):
        self.kite = kite
        self.futures_df = futures_df
        self.instrument_tokens = futures_df['instrument_token'].tolist()
        
    def update_futures_data(self, quotes):
        current_time = datetime.now()
        
        # Create new columns for the additional data
        new_data = []
        
        for token, quote in quotes.items():
            row = self.futures_df[self.futures_df['instrument_token'] == int(token)].iloc[0].to_dict()
            
            # Add new columns with quote data
            row.update({
                'timestamp': current_time,
                'last_trade_time': quote['last_trade_time'],
                'bid': quote['depth']['buy'][0]['price'] if quote['depth']['buy'] else 0,
                'ask': quote['depth']['sell'][0]['price'] if quote['depth']['sell'] else 0,
                'last': quote['last_price'],
                'change': quote['net_change'],
                'high': quote['ohlc']['high'],
                'low': quote['ohlc']['low'],
                'close': quote['ohlc']['close'],
                'buyers': len(quote['depth']['buy']),
                'sellers': len(quote['depth']['sell']),
                'volume': quote['volume'],
                'oi': quote['oi'],
                'last_quantity': quote['last_quantity'],
                'buy_quantity': quote['buy_quantity'],
                'sell_quantity': quote['sell_quantity'],
                'upper_circuit': quote['upper_circuit_limit'],
                'lower_circuit': quote['lower_circuit_limit'],
                'atp': quote['average_price'],
                'oi_day_high': quote['oi_day_high'],
                'oi_day_low': quote['oi_day_low']
            })
            new_data.append(row)
            
        # Create updated DataFrame
        updated_df = pd.DataFrame(new_data)
        
        # Save to CSV
        updated_df.to_csv(output_file, index=False)
        
        # Print sample data
        print(f"\nData updated at {current_time.strftime('%H:%M:%S')}")
        print(f"Sample data for {updated_df['tradingsymbol'].iloc[0]}:")
        print(f"LTP: {updated_df['last'].iloc[0]}, OI: {updated_df['oi'].iloc[0]}")
        
    def fetch_full_quote(self):
        try:
            quotes = self.kite.quote(self.instrument_tokens)
            self.update_futures_data(quotes)
                
        except Exception as e:
            print(f"Error fetching quotes: {str(e)}")
    
    def start_quote_tracking(self, interval=1):
        """Start continuous quote tracking with specified interval (in seconds)"""
        print(f"\nStarting quote tracking for {len(self.instrument_tokens)} futures...")
        print(f"Updating {output_file} with real-time data...")

        while True:
            try:
                self.fetch_full_quote()
                time.sleep(interval)
            except KeyboardInterrupt:
                print("\nQuote tracking stopped by user")
                break
            except Exception as e:
                print(f"Error in quote tracking: {str(e)}")
                time.sleep(5)  # Wait before retrying

def main():
    try:
        # Get current month futures
        current_futures_df = get_current_month_futures()
        
        # Initialize quote tracker
        quote_tracker = QuoteTracker(kite, current_futures_df)
        
        # Start continuous quote tracking (1-second interval)
        quote_tracker.start_quote_tracking(interval=sleep_interval)
    
    except Exception as e:
        print(f"An Error Occurred in Main Loop: {str(e)}")
        time.sleep(5)

if __name__ == "__main__":
    main()