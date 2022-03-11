#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from exchange_bsc_api_lib import SwapAPI
from requests import Session
import mysql.connector as mysql
from datetime import datetime
from datetime import timedelta
from datetime import date
import logging
import json
import time
import sys

def find(cursor, eth_address, amount, SwapAPIObject, broken_tokens):
	query_select_reserves = "SELECT swap_id, toTokenAddress FROM reserve_eth_test WHERE fromTokenReserve <> -1 and toTokenReserve <> -1"
	cursor.execute(query_select_reserves)
	rows = cursor.fetchall()
	acircle = []
	circle = []
	profit = []
	gasUsage = 2 * (122757 + 44174) ## from log of node
	gasPrice = SwapAPIObject.getGasPrice()
	fee = gasUsage*gasPrice*10**-9
	for row in rows:
		swap_id_start = row[0]
		to_token_address = row[1]
		if to_token_address in broken_tokens:
			continue
		res = SwapAPIObject.getPrice(swap_id_start, eth_address, to_token_address, amount) # swap eth -> tkx
		amountOut =  res['amountOut']
		amountIn  = amountOut
		if res['status'] == 200:
			# try final pair
			for row in rows:
				swap_id_finish = row[0]
				from_token_address = row[1]
				if to_token_address != from_token_address:
					continue
				res = SwapAPIObject.getPrice(swap_id_finish, from_token_address, eth_address, amountIn) # swap tkx -> eth
				amountOut =  res['amountOut']
				profit_value = amountOut - amount - fee
				if res['status'] == 200 and profit_value > 0:
					circle = []
					circle.append((eth_address, to_token_address, swap_id_start))
					circle.append((to_token_address, eth_address, swap_id_finish))
					profit.append(profit_value)
					acircle.append(circle)
	return acircle, profit

def get_new_broken_tokens(temp_storage, today, date_format):
    week = timedelta(days=7)
    broken_tokens = []
    for token_address in temp_storage.keys():
        first_error_date = datetime.strptime(temp_storage[token_address], date_format)
        if today - first_error_date > week:
            broken_tokens.append(token_address)
    return broken_tokens

# logging
log_file_name = "./tokens_test_find_12_01_22.log"
logging.basicConfig(
		filename = log_file_name,
		level = logging.WARNING,
		format = "[%(asctime)s] FILE: %(filename)-25s LINE: #%(lineno)d | %(levelname)-8s | %(message)s"
)

# start
logging.warning('Started')

# broken tokens
path_to_temp_storage = '/home/ivan/temp_storage.txt'
path_to_broken_tokens = '/home/ivan/broken_tokens.txt'
today = datetime.today()
date_format = '%Y-%m-%d'
try:
    with open(path_to_temp_storage, 'r') as f:
        temp_storage = json.load(f)
except FileNotFoundError:
    temp_storage = {}
try:
	with open(path_to_broken_tokens, 'r') as f:
		old_broken_tokens = [i.strip() for i in f.readlines()]
except FileNotFoundError:
	old_broken_tokens = []
new_broken_tokens = get_new_broken_tokens(temp_storage, today, date_format)
all_broken_tokens = set(old_broken_tokens) | set(new_broken_tokens)
# to save all broken_tokens
with open(path_to_broken_tokens, 'w') as f:
	for broken_token in all_broken_tokens:
		f.write(broken_token + '\n')
today = datetime.strftime(today, date_format)
# to update temp storage
temp_storage = {i:temp_storage[i] for i in temp_storage.keys() if i not in new_broken_tokens}

# connect to blockchain
SwapAPIObject = SwapAPI(False, "infura")

# connect to DB
db = mysql.connect(
		host = "localhost",
		user = "ivan",
		passwd = "Eig8TaidierohQuae3OS",
		database = "swaps_bsc"
)
cursor = db.cursor()

# find all the circles
eth_address = '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'
amount = 1.0
start = time.time()
routes, profit = find(cursor, eth_address, amount, SwapAPIObject, all_broken_tokens)
end = time.time()
print("The time of execution of above program is :", end-start)
logging.warning("The time of execution of above program is :{}".format(end-start))

#balanceETH = SwapAPIObject.balanceAccountETH()/10**18
#print("----")
#print("ganache-cli at {} now {} ETH".format(SwapAPIObject.getOwner(),balanceETH))
#print("----")

# telegram
session = Session()
path_to_chats = '/home/ivan/chats.txt'
telegram_token = '2146497296:AAGBBQw0GoFYkYR7_86X48lk-zKFi__2EgE'
telegram_api_url = 'https://api.telegram.org/bot' + telegram_token
get_updates_url = telegram_api_url + '/getUpdates'
resp = session.get(get_updates_url)
if 'result' in resp.json():
	updates = resp.json()['result']
else:
	updates = []
chats = []
for update in updates:
	if 'message' in update:
		chat = str(update['message']['chat']['id'])
		chats.append(chat)
print(chats)
# saving all the chats to local storage
with open(path_to_chats, 'a') as f:
	for chat in chats:
		f.write(chat + '\n')
# reading actual telegram chats
with open(path_to_chats, 'r') as f:
	chats = set([i.strip() for i in f.readlines()])

# alert all the bot users
#if len(routes) > 0:
#	str_profits = ', '.join([str(i) for i in profit])
#	msg = 'Hi! I have found {} circles with the porfits: {}'.format(len(routes), str_profits)
#	send_msg_url = telegram_api_url + '/sendMessage'
#	for chat in chats:
#		req_url = send_msg_url + '?chat_id=' + chat + '&text=' + msg
	      	# resp = session.get(req_url) ## don't send msg to telegram

# validation
for index, route in enumerate(routes):
	amount_raw = int(amount * 10**18)
	print("route:{} with profit: {}".format(route,profit[index]))
	result, state, gasused, txn_h, error = SwapAPIObject.swapbyrouterDryRun(route,amount_raw)
	print("Binnace Smart Cache dry run swap via self router route:{} result: {} {} {} {} {}".format(route, result, state, gasused, txn_h, error))
	print("++++")
	if gasused > 0:
        	## BINGO!!
		result, state, gasused, txn_h, error = SwapAPIObject.swapbyrouter(route,amount_raw)
		print("BINGO!!! Binnace Smart Cache dry run swap via self router route:{} result: {} {} {} {} {}".format(route, result, state, gasused, txn_h, error))
		mybnb = 'https://bscscan.com/address/0x2EE0531A26809D8ae37174703BF203fCfc333433'
		msg = 'Hi! I have found {} trande txn: {} in {}'.format(route, txn_h, mybnb)
		send_msg_url = telegram_api_url + '/sendMessage'
		for chat in chats:
			req_url = send_msg_url + '?chat_id=' + chat + '&text=' + msg
			resp = session.get(req_url)
	# to add broken token in temp storage
	else:
		broken_token_address = route[0][1]
		if broken_token_address not in temp_storage:
			temp_storage[broken_token_address] = today

# to save temp storage
with open(path_to_temp_storage, 'w') as f:
    json.dump(temp_storage, f)

# the end
logging.warning('Finished')
