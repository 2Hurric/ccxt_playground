import ccxt
import time
import pandas as pd
import os

def findActiveSymbols(exchange, symbolsCachePath="./symbols.txt", verbose=False):
    # find all acitve symbols
    symbols = []
    if os.path.isfile(symbolsCachePath):
        print("Found Symbols from cache... Load directly")
        with open(symbolsCachePath) as f:
            symbols = f.readlines()
        symbols = [x.strip() for x in symbols]
        return symbols

    print("Filtering out inacitve symbols...")
    for symbol in exchange.symbols:
        try:
            exchange.fetch_ticker(symbol)
            symbols.append(symbol)
        except:
            if verbose:
                print(f"{symbol} is inactive")
    print(f"Found {len(symbols)} active symbols!")
    with open(symbolsCachePath, "w+") as f:
        for symbol in symbols:
            f.write(symbol+"\n")
    return symbols

def searchDiff(exchange, symbols, startTime, endTime, timeOffset, verbose=False):
    print(f"Querrying pairs in {exchange.id} timestamp from {startTime} to {endTime}..." )
    prices = {}
    for i, symbol in enumerate(symbols):
        p1 = p2 = float("inf")
        print(f"Querring {symbol} ({(i+1)}/{len(symbols)})"+" "*10, end='\r')
        
        ohlcvs = exchange.fetch_ohlcv(symbol, '1d')
        # Looking for the close price for the endTime
        # Since some trading pairs' timestamp does not start from the today
        i = len(ohlcvs)-1
        while i >= 0 or ohlcvs[i][0] < endTime:
            curOHLCV = ohlcvs[i]
            if curOHLCV[0] >= endTime-timeOffset and curOHLCV[0] <= endTime+timeOffset:
                p1 = curOHLCV[4]
                break 
            i -= 1
        # Check if the current pair went online after our endTime
        if p1 == float("inf"): 
            if verbose:
                print(f"{symbol} went online after the specific endTime")
            continue

        # Looking for the cloce price for the startTime
        while i >= 0:
            curOHLCV = ohlcvs[i]
            if curOHLCV[0] >= startTime-timeOffset and curOHLCV[0] <= startTime+timeOffset:
                p2 = curOHLCV[4]
                break 
            i -= 1
        # Check if the current pair went online after startTime
        # We will take the earliest date as startTime
        if p2 == float("inf"):
            if verbose:
                print(f"{symbol} went online after the specific startTime, will take the online date as startTime")
            p2 = ohlcvs[0][4]

        time.sleep(exchange.rateLimit / 1000)

        prices[symbol] = p1 - p2
    return prices
        
if __name__ == "__main__":
    verbose = True
    bitmart = ccxt.bitmart() #Choose an exchange
    bitmart.load_markets()
    startTime = 1612137600000 # 2021-02-01T00:00:00Z
    startTimeString = '2021-02-01T00:00:00Z'
    endTime = 1612742400000 # 2021-02-08T00:00:00Z
    endTimeString = '2021-02-08T00:00:00Z'
    # cover corner cases that start and end time not exactly in T00:00:00Z
    timeOffset = 5000

    symbolsCachePath = "./symbols.txt"

    symbols = findActiveSymbols(bitmart, symbolsCachePath, verbose)
    prices = searchDiff(bitmart, symbols, startTime, endTime, timeOffset, verbose)

    df = pd.DataFrame(list(prices.items()), columns=['Pairs', 'Diff'])
    df.to_excel(f"diff_{startTimeString}_{endTimeString}.xlsx")





# OHLCV:
# [
#     [
#         1504541580000, // UTC timestamp in milliseconds, integer
#         4235.4,        // (O)pen price, float
#         4240.6,        // (H)ighest price, float
#         4230.0,        // (L)owest price, float
#         4230.7,        // (C)losing price, float
#         37.72941911    // (V)olume (in terms of the base currency), float
#     ],
#     ...
# ]