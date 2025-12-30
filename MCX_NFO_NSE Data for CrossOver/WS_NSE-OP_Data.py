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
access_token = "zjBVws6OG5Ql5jguqr2VZhwITDGKcGS6"

# Initialize Kite Connect
kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

# Read symbol list
csv_file = "Symbol_List_NSE-OP.csv"
instrument = "instruments.csv"
output_file = "Data_NFO-OP.csv"
sleep_interval = 5  # Interval in seconds for fetching quotes

symbol = "NIFTY"

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

def get_option_symbols(inst_df):
    df = inst_df[(inst_df['name'] == symbol) & (inst_df['segment'] == 'NFO-OPT')].copy()  # Filter NIFTY options and copy
    df['expiry'] = pd.to_datetime(df['expiry'])  # Convert expiry to datetime format

    # Get unique expiry dates and sort them
    expiry_dates = sorted(df['expiry'].unique())

    # Extract current week, next week, and current month expiries
    current_week_expiry = expiry_dates[0] if len(expiry_dates) > 0 else None
    next_week_expiry = expiry_dates[1] if len(expiry_dates) > 1 else None

    print(f"Current Week Expiry: {current_week_expiry.date() if current_week_expiry else None}")

    nifty_quote = kite.quote("NSE:NIFTY 50")
    nifty_ltp = nifty_quote["NSE:NIFTY 50"]["last_price"]
    atm_strike = int(round(nifty_ltp / 50) * 50)
    print(f"Nifty LTP: {nifty_ltp}; ATM Strike: {atm_strike}")

    # Generate list of strikes: 10 below and 10 above ATM
    strike_range = [atm_strike + 50 * i for i in range(-15, 16)]
    atm_df = df[(df['expiry'] == current_week_expiry) & (df['strike'].isin(strike_range))]

    output_cols = ['tradingsymbol', 'instrument_type', 'name', 'expiry', 'lot_size', 'instrument_token']
    atm_df_out = atm_df[output_cols]

    # Save to CSV
    atm_df_out.to_csv(csv_file, index=False)
    print(f"Saved {len(atm_df_out)} symbols to {csv_file}")

    df = pd.read_csv(csv_file)
    return df

class QuoteTracker:
    def __init__(self, kite, option_df):
        self.kite = kite
        self.option_df = option_df
        self.instrument_tokens = option_df['instrument_token'].tolist()

    def update_option_data(self, quotes):
        current_time = datetime.now()
        
        # Separate CE and PE data
        ce_data = []
        pe_data = []
        
        for token, quote in quotes.items():
            row = self.option_df[self.option_df['instrument_token'] == int(token)].iloc[0].to_dict()
            
            # Add quote data
            quote_data = {
                'tradingsymbol': row['tradingsymbol'],
                'bid': quote['depth']['buy'][0]['price'] if quote['depth']['buy'] else 0,
                'ask': quote['depth']['sell'][0]['price'] if quote['depth']['sell'] else 0,
                'last': quote['last_price'],
                'high': quote['ohlc']['high'],
                'low': quote['ohlc']['low'],
                'close': quote['ohlc']['close'],
                'buyers': len(quote['depth']['buy']),
                'sellers': len(quote['depth']['sell']),
                'volume': quote['volume'],
                'oi': quote['oi'],
                'strike': float(row['tradingsymbol'][-7:-2])  # Extract strike price from symbol
            }
            
            # Sort into CE and PE lists
            if row['instrument_type'] == 'CE':
                ce_data.append(quote_data)
            else:
                pe_data.append(quote_data)
        
        # Convert to DataFrames
        ce_df = pd.DataFrame(ce_data)
        pe_df = pd.DataFrame(pe_data)
        
        # Rename columns to distinguish CE and PE
        ce_df.columns = ['CE_' + col if col != 'strike' else col for col in ce_df.columns]
        pe_df.columns = ['PE_' + col if col != 'strike' else col for col in pe_df.columns]
        
        # Merge on strike price
        merged_df = pd.merge(
            ce_df.sort_values('strike'),
            pe_df.sort_values('strike'),
            on='strike',
            suffixes=('', '')
        )
        
        # Reorder columns to have CE data on left, strike in middle, PE data on right
        column_order = [
            'CE_tradingsymbol', 'CE_last', 'CE_sellers', 'CE_buyers', 'CE_close', 'CE_low', 'CE_high', 'CE_volume', 'CE_oi', 'CE_bid', 'CE_ask',
            'strike',
            'PE_ask', 'PE_bid', 'PE_oi', 'PE_volume', 'PE_high', 'PE_low', 'PE_close', 'PE_buyers', 'PE_sellers', 'PE_last', 'PE_tradingsymbol',
        ]
        merged_df = merged_df[column_order]
        
        # Save to CSV
        merged_df.to_csv(output_file, index=False)
        
        # Print sample data
        print(f"\nData updated at {current_time.strftime('%H:%M:%S')}")
        print(f"Number of strikes: {len(merged_df)}")
        print("\nSample data:")
        print(merged_df.head(1).to_string())

    def fetch_full_quote(self):
        try:
            quotes = self.kite.quote(self.instrument_tokens)
            self.update_option_data(quotes)
                
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
        option_df = get_option_symbols(inst_df)
        # Initialize quote tracker
        quote_tracker = QuoteTracker(kite, option_df)
        
        # Start continuous quote tracking (1-second interval)
        quote_tracker.start_quote_tracking(interval=sleep_interval)
    
    except Exception as e:
        print(f"An Error Occurred in Main Loop: {str(e)}")
        time.sleep(5)

if __name__ == "__main__":
    main()