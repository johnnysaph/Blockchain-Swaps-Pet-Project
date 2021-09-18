#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from exchange_bsc_api_lib import SwapAPI
from itertools import combinations
import mysql.connector as mysql
from datetime import datetime
import logging
import sys

# paths
log_file_name = "./tokens_reserves.log"
# queries
query_select_tokens = "SELECT * FROM tokens"
query_insert_reserves = "INSERT INTO reserve (swap_id, tokenA, tokenB, reserveA, reserveB, updated_at) VALUES (%s, %s, %s, %s, %s, %s)"
query_insert_tokens = "INSERT INTO tokens (symbol, name, address, decimals) VALUES (%s, %s, %s, %s)"

def get_tokens_pair_reserves(from_token_symbol, to_token_symbol, SwapAPIObject, swaps, time_stamp, address_to_id):
    from_token_addres = SwapAPIObject.symbol_to_token[from_token_symbol]['address']
    to_token_addres = SwapAPIObject.symbol_to_token[to_token_symbol]['address']
    from_token_id = address_to_id[from_token_addres]
    to_token_id = address_to_id[to_token_addres]
    tokens_pair_data = []
    for swap in swaps:
        swap_id = swap['id']
        price_responce = SwapAPIObject.getPrice(swap_id, from_token_addres, to_token_addres, 1.0)
        if price_responce['status'] == 500:
            reserved_from_token = -1
            reserved_to_token = -1
        else:
            reserved_from_token = price_responce['reserved_from_token']
            reserved_to_token = price_responce['reserved_to_token']
        tokens_pair_data.append((swap_id, from_token_id, to_token_id, reserved_from_token, reserved_to_token, time_stamp))
    return tokens_pair_data 

def get_reserves_data(tokens_symbols, SwapAPIObject, time_stamp, address_to_id):
    swaps = SwapAPIObject.swaplist()['swaps']
    reserves = {i:0 for i in combinations(sorted(tokens_symbols), 2)}
    reserves_data = []
    for first_token_symbol in tokens_symbols:
        for second_token_symbol in tokens_symbols:
            if first_token_symbol == second_token_symbol:
                continue
            sorted_tokens_pair = tuple(sorted([first_token_symbol, second_token_symbol]))
            if reserves[sorted_tokens_pair] != 0:
                continue
            from_token_symbol = sorted_tokens_pair[0]
            to_token_symbol = sorted_tokens_pair[1]
            reserves_data.extend(get_tokens_pair_reserves(from_token_symbol, to_token_symbol, SwapAPIObject, swaps, time_stamp, address_to_id))
            reserves[sorted_tokens_pair] = 1
    return reserves_data

def get_tokens_to_insert(tokens_addresses, SwapAPIObject):
    tokens_to_insert = []
    for token_adress in tokens_addresses:
        token = SwapAPIObject.address_to_token[token_adress]
        tokens_to_insert.append((token['symbol'], token['name'], token['address'], token['decimals']))
    return tokens_to_insert

def update_tokens_table(db, cursor, tokens_addresses, SwapAPIObject):
    cursor.execute(query_select_tokens)
    selected_tokens_data = cursor.fetchall()
    if len(selected_tokens_data) == 0:
        tokens_to_insert = get_tokens_to_insert(tokens_addresses, SwapAPIObject)
    else:
        print(len(selected_tokens_data))
        selected_adresses = [i[3] for i in selected_tokens_data]
        new_adresses = list(set(tokens_addresses) - set(selected_adresses))
        print(len(new_adresses))
        tokens_to_insert = get_tokens_to_insert(new_adresses, SwapAPIObject)
    logging.warning('Ready for uploading new tokens to DB')
    cursor.executemany(query_insert_tokens, tokens_to_insert)
    db.commit()
    cursor.execute(query_select_tokens)
    selected_tokens_data = cursor.fetchall()
    print(len(selected_tokens_data))
    return {i[3]:i[0] for i in selected_tokens_data}

def main(launchType='remote'):

    if launchType == 'infura':
        SwapAPIObject = SwapAPI(False, "infura")
    else:
        SwapAPIObject = SwapAPI(False, "local", "http")
    tokens = SwapAPIObject.tokenlist()['tokens']
    tokens_symbols = [i['symbol'] for i in tokens]
    tokens_addresses = [i['address'] for i in tokens]

    # setting connection to DB
    db = mysql.connect(
        host = "localhost",
        user = "ivan",
        passwd = "Eig8TaidierohQuae3OS",
        database = "swaps_bsc"
    )
    cursor = db.cursor()
    address_to_id = update_tokens_table(db, cursor, tokens_addresses, SwapAPIObject)
    time_stamp = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
    reserves_data = get_reserves_data(tokens_symbols, SwapAPIObject, time_stamp, address_to_id)
    logging.warning('Reserves data is ready')
    cursor.executemany(query_insert_reserves, reserves_data)
    logging.warning('Reserves query is done')
    db.commit()

if __name__ == "__main__":
    logging.basicConfig(
        filename = log_file_name,
        level = logging.WARNING,
        format = "[%(asctime)s] FILE: %(filename)-25s LINE: #%(lineno)d | %(levelname)-8s | %(message)s"
    )
    logging.warning('Started')
    print('Started')
    launchType = sys.argv[1]
    main(launchType)
    logging.warning('Finished')
    print('Finished')








