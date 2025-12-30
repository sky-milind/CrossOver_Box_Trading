from kiteconnect import KiteConnect, KiteTicker
import pandas as pd
import logging
from datetime import datetime
import requests
import json
import time 
import threading

logging.basicConfig(level=logging.INFO)

# Kite API credentials
api_key = "z0e38dbis6w8ccz7"
access_token = "vnp184sB2P4HlFD2N8Y8iNsGtLYI7Cgd"

# Initialize Kite Connect
kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

instrument = "instruments.csv"
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

GREEN = '\033[92m'
BLUE = '\033[94m'
YELLOW = '\033[93m'
RESET = '\033[0m'

def get_NFO_OPT_data(inst_df):
    try:
        # Read symbol list
        csv_file = "Symbol_List_NSE-OP.csv"
        output_file = "Data_NFO-OP.csv"
        sleep_interval = 5  # Interval in seconds for fetching quotes

        symbol = "NIFTY"

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
                print(f"\n{YELLOW}NFO-OPT Data updated at {current_time.strftime('%H:%M:%S')}{RESET}")
                #print(f"Number of strikes: {len(merged_df)}")
                #print("\nSample data:")
                #print(merged_df.head(1).to_string())

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
                        print(f"\n{YELLOW}Fetching NFO Option Data for {symbol}{RESET}")
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
                print(f"An Error Occurred in get_NFO_OPT_data Main Loop: {str(e)}")
                time.sleep(5)

        if __name__ == "__main__":
            main()

    except Exception as e:
        print(f"An Error Occurred in get_NFO_OPT_data: {str(e)}")
        time.sleep(5)

def get_NFO_FUT_data(inst_df):  
    try:  
        # Read symbol list
        csv_file = "Symbol_List_NFO.csv"
        df = pd.read_csv(csv_file)
        instrument = "instruments.csv"
        output_file = "Data_FUT_NFO.csv"
        sleep_interval = 5  # Interval in seconds for fetching quotes   

        symbols = df["tradingsymbol"].tolist()
        def get_current_month_futures():
            # Get current date
            current_date = datetime.now()
            
            # Convert expiry column to datetime
            inst_df['expiry'] = pd.to_datetime(inst_df['expiry'])
            upcoming_nfo_exp = inst_df[inst_df['segment']=='NFO-FUT']['expiry'].iloc[0]
            print(f"Upcoming Futures Expiry: {upcoming_nfo_exp.strftime('%d-%b-%Y')}")

            # Filter current month futures
            if upcoming_nfo_exp.month == current_date.month:
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
                print(f"\n{GREEN}NFO-FUT Data updated at {current_time.strftime('%H:%M:%S')}{RESET}")
                #print(f"Sample data for {updated_df['tradingsymbol'].iloc[0]}:")
                #print(f"LTP: {updated_df['last'].iloc[0]}, OI: {updated_df['oi'].iloc[0]}")
                
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
                        print(f"\n{GREEN}Fetching NFO Futures Data{RESET}")
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
                print(f"An Error Occurred in get_NFO_FUT_data Main Loop: {str(e)}")
                time.sleep(5)

        if __name__ == "__main__":
            main()

    except Exception as e:
        print(f"An Error Occurred in get_NFO_FUT_data: {str(e)}")
        time.sleep(5)

def get_MCX_FUT_data(inst_df): 
    try:  
        # Read symbol list
        csv_file = "Symbol_List_MCX.csv"
        df = pd.read_csv(csv_file)
        output_file = "Data_FUT_MCX.csv"
        sleep_interval = 5  # Interval in seconds for fetching quotes

        symbols = df["tradingsymbol"].tolist()
        def get_current_month_futures():
            # Get current date
            current_date = datetime.now()
            
            # Convert expiry column to datetime
            inst_df['expiry'] = pd.to_datetime(inst_df['expiry'])
            upcoming_mcx_exp = inst_df[inst_df['segment']=='MCX-FUT']['expiry'].iloc[0]
            print(f"Upcoming Futures Expiry: {upcoming_mcx_exp.strftime('%d-%b-%Y')}")

            # Filter current month futures
            if upcoming_mcx_exp.month == current_date.month:
                # Filter current month futures
                current_futures = inst_df[
                    (inst_df['instrument_type'] == 'FUT') & 
                    (inst_df['expiry'].dt.month == current_date.month) &
                    (inst_df['expiry'].dt.year == current_date.year)
                ]
            else:
                # Filter next month futures
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
                print(f"\n{BLUE}MCX-FUT Data updated at {current_time.strftime('%H:%M:%S')}{RESET}")
                #print(f"Sample data for {updated_df['tradingsymbol'].iloc[0]}:")
                #print(f"LTP: {updated_df['last'].iloc[0]}, OI: {updated_df['oi'].iloc[0]}")
                
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
                        print(f"\n{BLUE}Fetching MCX Futures Data{RESET}")
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
                logging.error(f"An error occurred in get_MCX_FUT_data Main Loop: {str(e)}")
                time.sleep(5)

        if __name__ == "__main__":
            main()

    except Exception as e:
        logging.error(f"An error occurred in get_MCX_FUT_data: {str(e)}")
        time.sleep(5)

# Start threads for each data fetching function
nfo_opt_thread = threading.Thread(target=get_NFO_OPT_data, args=(inst_df,))
nfo_fut_thread = threading.Thread(target=get_NFO_FUT_data, args=(inst_df,))
mcx_fut_thread = threading.Thread(target=get_MCX_FUT_data, args=(inst_df,))
nfo_opt_thread.start()
nfo_fut_thread.start()
mcx_fut_thread.start()
nfo_opt_thread.join()
nfo_fut_thread.join()
mcx_fut_thread.join()

