import asyncio
import aiosqlite
import aiohttp
from datetime import datetime
import time
import os
from alpha_vantage.timeseries import TimeSeries
from dotenv import load_dotenv

API_KEY = os.getenv("API_KEY")
currency_pairs = ['EURUSD', 'GBPUSD', 'USDJPY', 'AUDUSD', 'USDCHF']  # Список валютных пар

async def create_quotes_table():
    conn = await aiosqlite.connect('quotes.db')
    cursor = await conn.cursor()
    await cursor.execute("CREATE TABLE IF NOT EXISTS quotes (currency_pair TEXT, bid REAL, ask REAL, timestamp TEXT)")
    await conn.commit()
    await cursor.close()
    await conn.close()

async def get_database_connection():
    await create_quotes_table()
    conn = await aiosqlite.connect('quotes.db')
    return conn

async def fetch_quote(currency_pair):
    ts = TimeSeries(key=API_KEY, output_format='pandas')
    data, meta_data = ts.get_intraday(symbol=currency_pair, interval='1min')

    bid = data['4. close'].iloc[-1]
    ask = data['4. close'].iloc[-1]

    return {
        'currency_pair': currency_pair,
        'bid': bid,
        'ask': ask,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

async def update_quotes():
    while True:
        async with get_database_connection() as conn:
            for currency_pair in currency_pairs:
                quote = await fetch_quote(currency_pair)

                cursor = await conn.cursor()

                # Удаление предыдущих котировок для валютной пары
                await cursor.execute("DELETE FROM quotes WHERE currency_pair = ?", (currency_pair,))

                # Вставка актуальной котировки
                await cursor.execute("INSERT INTO quotes (currency_pair, bid, ask, timestamp) VALUES (?, ?, ?, ?)",
                                     (quote['currency_pair'], quote['bid'], quote['ask'], quote['timestamp']))

                await conn.commit()
                await cursor.close()

        await asyncio.sleep(1)  # Пауза в 1 секунду перед обновлением котировок

# Запуск асинхронной функции
loop = asyncio.get_event_loop()
loop.run_until_complete(update_quotes())