from trading_api import TRADING_API
import numpy as np
import asyncio, json, datetime

algo = TRADING_API(key = "key", 
                   secret = "secret",
                   log_path = "logpath"
                   )

async def martingale(symbol : str, limit : int, q : float, margin: float, type: int, on_off: bool) -> None:
    if on_off == True:
        df = algo.um_klines(symbol, '5m', limit, start = None, end = None)
        df["ohlc4"] = 0.25 * (df["open"] + df["close"] + df["high"] + df["low"])
        df["macd"] = df["ohlc4"].rolling(window = 100).mean() - df["ohlc4"].rolling(window = 200).mean()
        df["signal"] = df["macd"].rolling(window = 10).mean()
        df["hist"] = df["macd"] - df["signal"]
        if (df.loc[len(df) - 2, "hist"] <= 0.0 and df.loc[len(df) - 3, "hist"] > 0.0 and algo.um_position(symbol).loc[0, 'positionAmt'] == 0):
            leverage = algo.um_position(symbol).loc[0, 'leverage']
            algo.um_limit_order(symbol = symbol, 
                                side = "BUY", 
                                price = df['close'].loc[len(df) - 1]*1.005, 
                                qty = q * leverage/df['close'].loc[len(df) - 1], 
                                position = "LONG",
                                client_order_id = symbol + "_limit_" + str(df['close'].loc[len(df) - 1]*1.005)
                                )
            algo.um_modify_margin(symbol = symbol, 
                                type = 1, 
                                amt = margin,
                                position = "LONG"
                                )
            a = algo.um_position(symbol).loc[0, 'entryPrice']
            w = int(type / 3.0)
            for i in range(1, w + 1):
                algo.um_limit_order(symbol = symbol, 
                                    side = "BUY", 
                                    price = a * (1.0 - i * 0.03),  
                                    qty = q * leverage * (1.3 ** i)/(a * (1.0 - i * 0.03)), 
                                    position= "LONG",
                                    client_order_id = symbol + "_limit_" + str(a * (1.0 - i * 0.03))
                                    )
        if (algo.um_open_orders(symbol).empty == False):
                df_side = algo.um_open_orders(symbol)[['orderId', 'side']]
        else:
                df_side = algo.um_open_orders(symbol)
        if (algo.um_position(symbol).loc[0, 'positionAmt'] > 0):
                sell_side = df_side[df_side['side'] == 'SELL'].reset_index()
                sell_price = algo.um_position(symbol).loc[0, 'entryPrice'] * 1.007
                sell_qty = algo.um_position(symbol).loc[0, 'positionAmt']
                if (sell_side.empty == False):
                    algo.um_cancel_order(symbol = symbol, order_id= sell_side.loc[0, 'orderId'])
                    algo.um_limit_order(symbol = symbol, 
                                        side = "SELL", 
                                        price = sell_price, 
                                        qty = sell_qty,
                                        position= "LONG",
                                        client_order_id = symbol + "_limit_" + str(sell_price)
                                        )
                else:
                    algo.um_limit_order(symbol = symbol, 
                                        side = "SELL", 
                                        price = sell_price, 
                                        qty = sell_qty, 
                                        position= "LONG",
                                        client_order_id = symbol + "_limit_" + str(sell_price)
                                        )
        if (algo.um_position(symbol).loc[0, 'positionAmt'] == 0 and df_side.empty == False):
            if (df_side[df_side['side'] == 'BUY'].empty == False):
                algo.um_cancel_all(symbol = symbol)          
        print("Datetime:{} ,Symbol: {}".format(datetime.datetime.utcnow(), symbol))
        print(algo.um_position(symbol)[['symbol', 'positionAmt', 'entryPrice', 'leverage', 'isolated', 'PNL%']])
        print("===========================================================")

async def coro(symbol, limit, q, margin, type, on_off) -> None:
    await martingale(  symbol= symbol,
                        limit= limit,
                        q= q,
                        margin= margin,
                        type= type,
                        on_off= on_off)

async def task(symbol : str, limit : int, q : float, margin: float, type: int, on_off: bool) -> None:
    await asyncio.create_task(coro= coro( symbol= symbol,
                                                limit= limit,
                                                q= q,
                                                margin= margin,
                                                type= type,
                                                on_off= on_off)) 

async def asyncio_pool(path: str):
    stream = open(path, "r")
    pool = json.loads(stream.read())
    stream.close()
    await asyncio.gather(
        task(pool["task_1"][0], pool["task_1"][1], pool["task_1"][2], pool["task_1"][3], pool["task_1"][4],  pool["task_1"][5]),
        task(pool["task_2"][0], pool["task_2"][1], pool["task_2"][2], pool["task_2"][3], pool["task_2"][4], pool["task_2"][5]),
        task(pool["task_3"][0], pool["task_3"][1], pool["task_3"][2], pool["task_3"][3], pool["task_3"][4], pool["task_3"][5]),
        task(pool["task_4"][0], pool["task_4"][1], pool["task_4"][2], pool["task_4"][3], pool["task_4"][4], pool["task_4"][5]),
        task(pool["task_5"][0], pool["task_5"][1], pool["task_5"][2], pool["task_5"][3], pool["task_5"][4], pool["task_5"][5]),
        task(pool["task_6"][0], pool["task_6"][1], pool["task_6"][2], pool["task_6"][3], pool["task_6"][4], pool["task_6"][5]),
        task(pool["task_7"][0], pool["task_7"][1], pool["task_7"][2], pool["task_7"][3], pool["task_7"][4], pool["task_7"][5]),
        task(pool["task_8"][0], pool["task_8"][1], pool["task_8"][2], pool["task_8"][3], pool["task_8"][4], pool["task_8"][5]),
        task(pool["task_9"][0], pool["task_9"][1], pool["task_9"][2], pool["task_9"][3], pool["task_9"][4], pool["task_9"][5]),
        task(pool["task_10"][0], pool["task_10"][1], pool["task_10"][2], pool["task_10"][3], pool["task_10"][4], pool["task_10"][5]),
        task(pool["task_11"][0], pool["task_11"][1], pool["task_11"][2], pool["task_11"][3], pool["task_11"][4], pool["task_11"][5]),
        task(pool["task_12"][0], pool["task_12"][1], pool["task_12"][2], pool["task_12"][3], pool["task_12"][4], pool["task_12"][5]),
        task(pool["task_13"][0], pool["task_13"][1], pool["task_13"][2], pool["task_13"][3], pool["task_13"][4], pool["task_13"][5]),
        task(pool["task_14"][0], pool["task_14"][1], pool["task_14"][2], pool["task_14"][3], pool["task_14"][4], pool["task_14"][5]),
        task(pool["task_15"][0], pool["task_15"][1], pool["task_15"][2], pool["task_15"][3], pool["task_15"][4], pool["task_15"][5]),
        task(pool["task_16"][0], pool["task_16"][1], pool["task_16"][2], pool["task_16"][3], pool["task_16"][4], pool["task_16"][5]),
        task(pool["task_17"][0], pool["task_17"][1], pool["task_17"][2], pool["task_17"][3], pool["task_17"][4], pool["task_17"][5]),
        task(pool["task_18"][0], pool["task_18"][1], pool["task_18"][2], pool["task_18"][3], pool["task_18"][4], pool["task_18"][5])
                        )

def main():
    def exec():
        asyncio.run(asyncio_pool("managementPath"))
    start_time = {    datetime.datetime.strptime("00:00", "%M:%S").time(), 
                    datetime.datetime.strptime("05:00", "%M:%S").time(), 
                    datetime.datetime.strptime("10:00", "%M:%S").time(), 
                    datetime.datetime.strptime("15:00", "%M:%S").time(), 
                    datetime.datetime.strptime("20:00", "%M:%S").time(), 
                    datetime.datetime.strptime("25:00", "%M:%S").time(), 
                    datetime.datetime.strptime("30:00", "%M:%S").time(), 
                    datetime.datetime.strptime("35:00", "%M:%S").time(), 
                    datetime.datetime.strptime("40:00", "%M:%S").time(), 
                    datetime.datetime.strptime("45:00", "%M:%S").time(), 
                    datetime.datetime.strptime("50:00", "%M:%S").time(), 
                    datetime.datetime.strptime("55:00", "%M:%S").time()
                    }
    previous = datetime.datetime.strptime("00:00", "%M:%S").time()
    while True:
        now = datetime.datetime.utcnow()
        now = str(now.minute) + ":" + str(now.second)
        now = datetime.datetime.strptime(now, "%M:%S").time()
        try:
            if now in start_time and previous != now:
                exec()
            previous = now
        except KeyboardInterrupt:
            break
