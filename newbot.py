import requests
import json
import pandas as pd
import numpy as np
import time

db_path = 'db.csv'
db = pd.read_csv(db_path)

token_path = 'token_db.csv'
token_db_df = pd.read_csv(token_path)
token_db = token_db_df['token_list'].tolist()

db = db.drop(columns=['Unnamed: 0'])

i = 0
end = 500
for i in range(0, end):

    url_base = "https://public-api.birdeye.so/public/tokenlist"
    headers = {"X-API-KEY": "3cf38c209cad40649673b3ad3223d1a6"}

    all_data = []

    for offset in range(400, 1051, 50):  # Range from 0 to 151 with a step of 50
        url = f"{url_base}?sort_by=v24hUSD&sort_type=desc&offset={offset}&limit=50"

        time.sleep(1)

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            api_response = json.loads(response.text)
            data = api_response['data']['tokens']
            all_data.extend(data)

        else:
            print(f"Request failed with status code {response.status_code}")

    # Now, all_data contains the combined data from the three requests
    df = pd.DataFrame(all_data)
    len_old = len(db)

    db['old'] = 1
    df['old'] = df['symbol'].isin(token_db).astype(int)
    df['stamp'] = int((time.time_ns() / 1e6) / 1000)

    db = pd.concat([db, df])
    db = db.drop_duplicates(subset=db.columns.difference(['stamp', ]))
    len_new = len(db)

    if len_old == len_new:
        print('nothing to see')
    else:
        print('new stuff')

    token_db = db['symbol'].unique()
    token_db_df = pd.DataFrame({'token_list': token_db})

    new_count = len(token_db)

    db.to_csv("db.csv")

    token_db_df.to_csv("token_db.csv")

    db = db.sort_values(['lastTradeUnixTime', 'v24hUSD'], ascending=False)
    pd.set_option('display.float_format', lambda x: '%.5f' % x)
    len(db)
    dk = db.groupby('symbol', group_keys=False).apply(
        lambda group: group.sort_values(by=['lastTradeUnixTime'], ascending=False).iloc[:10])

    dk = dk[(dk['liquidity'] > 2000)]
    dk = dk[(dk['v24hUSD'] > 8000)]

    dk = dk.sort_values(by=['symbol', 'lastTradeUnixTime'], ascending=[True, True])

    dk['pmc'] = dk.groupby('symbol')['mc'].shift(1)
    dk['mc_change'] = (dk['mc'] - dk['pmc'])
    dk['updown'] = (dk['mc_change'].gt(0) | dk['mc_change'].isna()).astype(int)
    dk['rowcount'] = dk.groupby('symbol')['updown'].transform(lambda x: len(x))
    dk['upcount'] = dk.groupby('symbol')['updown'].transform(lambda x: x.sum())

    dk['pmc_1'] = dk.groupby('symbol')['mc'].shift(1)
    dk['mc_change_1'] = (dk['mc'] - dk['pmc_1']) / dk['pmc_1'] * 100
    dk['pt_1'] = dk.groupby('symbol')['lastTradeUnixTime'].shift(1)
    dk['t_1'] = dk['lastTradeUnixTime'] - dk['pt_1']
    dk['t_1'] = dk['t_1'] / 60
    dk['t_1'] = dk['t_1'].apply(lambda x: '{:,.0f}'.format(x))

    dk['pmc_2'] = dk.groupby('symbol')['mc'].shift(4)
    dk['mc_change_2'] = (dk['mc'] - dk['pmc_2']) / dk['pmc_2'] * 100
    dk['pt_2'] = dk.groupby('symbol')['lastTradeUnixTime'].shift(4)
    dk['t_2'] = dk['lastTradeUnixTime'] - dk['pt_2']
    dk['t_2'] = dk['t_2'] / 60
    dk['t_2'] = dk['t_2'].apply(lambda x: '{:,.0f}'.format(x))

    dk = dk.groupby('symbol', group_keys=False).apply(
        lambda group: group.sort_values(by=['lastTradeUnixTime'], ascending=False).iloc[:1])

    dk = dk.sort_values(['stamp'], ascending=False)
    last_stamp = dk['stamp'].iloc[0]
    dk = dk[dk['stamp'] == last_stamp]

    dk = dk.sort_values(['lastTradeUnixTime'], ascending=False)
    len(dk)

    # Lets form a clean db
    dx = pd.DataFrame()
    dx['symbol'] = dk['symbol']
    dx['name'] = dk['name']
    dx['mc'] = dk['mc']
    dx['volume'] = dk['v24hUSD']
    dx['liquidity'] = dk['liquidity']
    dx['lastTrade'] = dk['lastTradeUnixTime']
    dx['stamp'] = dk['stamp']
    dx['logo'] = dk['logoURI']

    dx['upcount'] = dk['upcount']
    dx['rowcount'] = dk['rowcount']

    dx['mc_change_1'] = dk['mc_change_1']
    dx['time_ago_1'] = dk['t_1']

    dx['mc_change_2'] = dk['mc_change_2']
    dx['time_ago_2'] = dk['t_2']

    dx['v24hd'] = dk['v24hChangePercent']

    dx['address'] = dk['address']
    dx['old'] = dk['old']

    dx = dx[(dx['v24hd'] > 0) | np.isnan(dx['v24hd'])]

    dx = dx[dx['mc'] < 1500000]

    dx['mc_change_2'] = pd.to_numeric(dx['mc_change_2'], errors='coerce')
    dx = dx[(dx['mc_change_2'] > -60) | np.isnan(dx['mc_change_2'])]

    dx['mc_change_1'] = pd.to_numeric(dx['mc_change_1'], errors='coerce')
    dx = dx[(dx['mc_change_1'] > -70) | np.isnan(dx['mc_change_1'])]

    dx = dx.sort_values(by=['lastTrade'], ascending=False)
    last_trade = dx['lastTrade'].iloc[0]
    trade_15min = last_trade - 1800
    dx = dx[dx['lastTrade'] > trade_15min]

    dx['v30mUSD'] = None
    dx['v30mchange'] = None
    dx['p30change'] = None
    dx['price'] = None

    dx = dx.reset_index(drop=True)  # Reset index to ensure it starts from 0

    # Get prices

    # Sample splitting of dx_add
    dx_add = dx['address'].unique().tolist()

    # Check if dx_add is not empty before proceeding
    if len(dx_add) > 0:
        chunk_size = max(len(dx_add) // 4, len(dx_add))  # Adjust the chunk size as needed

        headers = {"X-API-KEY": "3cf38c209cad40649673b3ad3223d1a6"}

        # Initialize an empty dictionary to store the combined results
        combined_api_response = {'data': {}}

        # Loop over chunks of dx_add
        for i in range(0, len(dx_add), chunk_size):
            chunk = dx_add[i:i + chunk_size]
            chunk_string = ",".join(chunk)

            # URL for the current chunk
            url_price_chunk = "https://public-api.birdeye.so/defi/multi_price?list_address={}".format(chunk_string)

            # Make the request
            response_chunk = requests.get(url_price_chunk, headers=headers)
            api_response_chunk = json.loads(response_chunk.text)

            # Update the combined results with the current chunk
            combined_api_response['data'].update(api_response_chunk.get('data', {}))

        # Update the DataFrame using the combined results
        for i in range(0, len(dx)):
            address = dx['address'].iloc[i]
            dx.at[i, 'price'] = combined_api_response['data'].get(address, {}).get('value', None)
            dx.at[i, 'price_time'] = combined_api_response['data'].get(address, {}).get('updateUnixTime', None)

        dx['mc_change_2'] = pd.to_numeric(dx['mc_change_2'], errors='coerce')

        dx['BSratio5m'] = None
        dx['p5change'] = None
        dx['v5mUSD'] = None

        mask = dx['name'].str.contains("Wormhole")
        dx = dx.loc[~mask]

        #Filter Baby

        # Define a list of conditions
        conditions = [
            (dx['mc'] >= 10000) & (dx['mc'] < 70000) & (dx['liquidity'] > 1000),
            (dx['mc'] >= 70000) & (dx['mc'] < 200000) & (dx['liquidity'] > 3000),
            (dx['mc'] >= 200000) & (dx['mc'] < 700000) & (dx['liquidity'] > 5000),
            (dx['mc'] >= 700000) & (dx['mc'] < 1500000) & (dx['liquidity'] > 10000)

        ]

        # Create a list of condition names for reference
        condition_names = ['bb_db', 'smol_db', "mid_db", "big_db"]

        # Create a dictionary to store filtered DataFrames
        filtered_dfs = {}

        # Apply conditions and store filtered DataFrames in the dictionary
        for name, condition in zip(condition_names, conditions):
            filtered_dfs[name] = dx[condition]

        # Access the results using the original DataFrame names
        bb_db = filtered_dfs['bb_db']
        smol_db = filtered_dfs['smol_db']
        mid_db = filtered_dfs['mid_db']
        big_db = filtered_dfs['big_db']

        filtered = pd.concat([bb_db, smol_db, mid_db, big_db], ignore_index=True)

        # if new

        new = filtered[filtered["old"] == 0]

        # Define the desired column order
        desired_columns_order = ['old', 'symbol', 'name', 'mc', 'lastTrade', 'p5change', 'BSratio5m', 'v5mUSD',
                                 'address', 'volume', 'liquidity', 'mc_change_1', 'time_ago_1', 'mc_change_2',
                                 'time_ago_2', 'stamp']

        # Rearrange the columns
        new = new[desired_columns_order]
        new = new.sort_values(['v5mUSD', 'p5change', ], ascending=[False, False])

        if len(new) != 0:
            message = "************** DYOR NFA " + "\n"
            message += "<b>MEGA RISKY MOOON OR DUST BRAND NEW VOLUME </b>" + "\n"
            message += "************** DYOR NFA " + "\n"

            TOKEN = "6604748018:AAHsgx5dIQfE8wErDyHCtJdUZ37ghjtlBx8"
            chat_id = "-4038071582"

            # -4038071582
            # -4072211127

            url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&parse_mode=HTML&text={message}&disable_web_page_preview=True"
            print(requests.get(url).json())  # this sends the message

            for i in range(0, len(new)):
                add = str(new['address'].iloc[i])

                url_security = "https://public-api.birdeye.so/defi/token_security?address={}".format(add)
                headers = {"X-API-KEY": "3cf38c209cad40649673b3ad3223d1a6"}

                response = requests.get(url_security, headers=headers)
                api_response = json.loads(response.text)
                top10percent = float(api_response['data']['top10HolderPercent']) * 100
                top10percent = '{:,.0f}'.format(top10percent)

                symbol = str(new['symbol'].iloc[i])
                mc = new['mc'].iloc[i]
                mc = '{:,.0f}'.format(mc)
                vol = new['volume'].iloc[i]
                vol = '{:,.0f}'.format(vol)
                name = str(new['name'].iloc[i])
                liquid = new['liquidity'].iloc[i]
                liquid = '{:,.0f}'.format(liquid)

                message = "<a>Ticker: {}</a>".format(symbol) + "\n"
                message += "<a>Name: {}</a>".format(name) + "\n"
                message += "<a>MC: ${}</a>".format(mc) + "\n"
                message += "<a>Liquidity: ${}</a>".format(liquid) + "\n"
                message += "<a>24hVolume: ${}</a>".format(vol) + "\n"
                message += "<a>Top10holds: {}%</a>".format(top10percent) + "\n"
                message += "<a href='https://rugcheck.xyz/tokens/{}'>rugcheck</a>".format(add) + "   "
                message += "<a href='https://birdeye.so/token/{}?chain=solana/'>birdeye</a>".format(add) + "   "
                message += "<a href='https://t.me/bonkbot_bot?start=ref_aa5bm_ca_{}'>bonkbot</a>".format(
                    add) + "\n" + "\n"

                TOKEN = "6604748018:AAHsgx5dIQfE8wErDyHCtJdUZ37ghjtlBx8"
                chat_id = "-4038071582"

                # -4038071582

                url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&parse_mode=HTML&text={message}&disable_web_page_preview=True"
                print(requests.get(url).json())  # this sends the message

            i += 1

            time.sleep(30)
            print("new volume")

        else:
            i += 1

            time.sleep(30)
            print("no new volume")
    else:
        pass
    if i == end:
        print('hi guyss baby bot went to sleep! he will be back soon <3 lysm tysm <3')
    else:
        pass

print('yes')