from datetime import datetime
from utils import Utils
import asyncio
import socketio
import csv
import json
import pandas as pd
import xlrd
import os
import glob

URI = 'wss://api-v4.zerion.io'
API_TOKEN = 'ZERION_KEY_HERE'
ORIGIN = 'http://localhost:3000'

sio = socketio.AsyncClient(logger=False, engineio_logger=False)

CONNECTED_TO_SOCKET = False

ADDRESS_TRANSACTIONS = {}

ADDRESSES = []


class Transaction:
    def __init__(self, data):
        # get direction
        def get_direction():
            changes = data["changes"]
            if (len(changes) == 0):
                return ""
            if (len(changes) > 1):
                return ""
            return changes[0]['direction']

        # set init
        self.data = data
        self.direction = get_direction()

    def date(self):
        date = int(self.data["mined_at"])
        return datetime.utcfromtimestamp(date).strftime("%m/%d/%Y")

    def time(self):
        date = int(self.data["mined_at"])
        return datetime.utcfromtimestamp(date).strftime("%H:%M")

    def status(self):
        return self.data["status"].capitalize()

    def type(self):
        type = self.data["type"]
        if (type == "execution"):
            return "Contract Execution"
        return type.capitalize()

    def application(self):
        return f'{self.data["protocol"]}' if self.data["protocol"] else ""

    def accounting_type(self):
        changes = self.data["changes"]

        ins = [change for change in changes if change["direction"] == "in"]
        outs = [change for change in changes if change["direction"] == "out"]

        # no changes
        if (len(changes) == 0):
            return "Spend"
        # in
        if (len(ins) > 0 and len(outs) == 0):
            return "Income"
        # out
        if (len(outs) > 0 and len(ins) == 0):
            return "Spend"

        return "Trade"

    def buy_amount(self):
        changes = self.data["changes"]

        # filter for direction in
        ins = [change for change in changes if change["direction"] == "in"]
        length = len(ins)

        if (length == 0):
            return ""

        amounts = []
        for i in range(length):
            change = ins[i]
            decimals = float(change["asset"]["decimals"]
                             ) if change["asset"]["decimals"] else 0
            value = float(change["value"]) if change["value"] else 0
            amounts.append(value / 10 ** decimals)

        return '\n'.join(str(v) for v in amounts)

    def buy_asset(self):
        changes = self.data["changes"]

        # filter for direction in
        ins = [change for change in changes if change["direction"] == "in"]
        length = len(ins)

        if (length == 0):
            return ""

        assets = []
        for i in range(length):
            change = ins[i]
            asset = change["asset"]["symbol"]
            if (asset != None):
                assets.append(change["asset"]["symbol"])

        return '\n'.join(assets)

    def buy_asset_address(self):
        changes = self.data["changes"]
        if (len(changes) == 0):
            return ""

        # filter for direction in
        ins = [change for change in changes if change["direction"] == "in"]
        length = len(ins)

        if (length == 0):
            return ""

        addresses = []
        for i in range(length):
            change = ins[i]
            addresses.append(change["asset"]["asset_code"])

        return '\n'.join(addresses)

    def buy_fiat_amount(self):
        changes = self.data["changes"]

        # filter for direction in
        ins = [change for change in changes if change["direction"] == "in"]
        length = len(ins)

        if (length == 0):
            return ""

        fiat_amounts = []
        for i in range(length):
            change = ins[i]
            price = float(change["price"]) if (change["price"]) else 0
            value = float(change["value"]) if (change["value"]) else 0
            decimals = float(change["asset"]["decimals"]) if (
                change["asset"]["decimals"]) else 0
            fiat_amounts.append((value / 10 ** decimals) * price)

        summed = sum(float(v) for v in fiat_amounts)
        if (summed == 0):
            return ""
        return '\n'.join(str(v) for v in fiat_amounts)

    def buy_fiat_currency(self):
        fiat_amounts_str = self.buy_fiat_amount()
        fiat_amounts = [x for x in fiat_amounts_str.split(
            '\n') if x != ""]

        if (len(fiat_amounts) == 0):
            return ""

        return '\n'.join('USD' for _ in fiat_amounts)

    def sell_amount(self):
        changes = self.data["changes"]

        # filter for direction out
        outs = [change for change in changes if change["direction"] == "out"]
        length = len(outs)

        if (length == 0):
            return ""

        amounts = []
        for i in range(length):
            change = outs[i]
            decimals = float(change["asset"]["decimals"]
                             ) if change["asset"]["decimals"] else 0
            value = float(change["value"]) if change["value"] else 0
            amounts.append(value / 10 ** decimals)

        return '\n'.join(str(v) for v in amounts)

    def sell_asset(self):
        changes = self.data["changes"]

        # filter for direction out
        outs = [change for change in changes if change["direction"] == "out"]
        length = len(outs)

        if (length == 0):
            return ""

        assets = []
        for i in range(length):
            change = outs[i]
            asset = change["asset"]["symbol"]
            if (asset != None):
                assets.append(change["asset"]["symbol"])

        return '\n'.join(assets)

    def sell_asset_address(self):
        changes = self.data["changes"]
        if (len(changes) == 0):
            return ""

        # filter for direction out
        outs = [change for change in changes if change["direction"] == "out"]
        length = len(outs)

        if (length == 0):
            return ""

        addresses = []
        for i in range(length):
            change = outs[i]
            addresses.append(change["asset"]["asset_code"])

        return '\n'.join(addresses)

    def sell_fiat_amount(self):
        changes = self.data["changes"]

        # filter for direction out
        outs = [change for change in changes if change["direction"] == "out"]
        length = len(outs)

        if (length == 0):
            return ""

        fiat_amounts = []
        for i in range(length):
            change = outs[i]
            price = float(change["price"]) if (change["price"]) else 0
            value = float(change["value"]) if (change["value"]) else 0
            decimals = float(change["asset"]["decimals"]) if (
                change["asset"]["decimals"]) else 0
            fiat_amounts.append((value / 10 ** decimals) * price)

        summed = sum(float(v) for v in fiat_amounts)
        if (summed == 0):
            return ""
        return '\n'.join(str(v) for v in fiat_amounts)

    def sell_fiat_currency(self):
        fiat_amounts_str = self.sell_fiat_amount()
        fiat_amounts = [x for x in fiat_amounts_str.split(
            '\n') if x != ""]

        if (len(fiat_amounts) == 0):
            return ""

        return '\n'.join('USD' for _ in fiat_amounts)

    def fee_amount(self):
        fee = self.data["fee"]
        if fee != None:
            fee_amount = '{:.18f}'.format(
                float(fee["value"]) / 1e18).rstrip('0')
            return fee_amount if fee_amount != '' else None
        else:
            return None

    def fee_currency(self):
        return "ETH" if self.fee_amount() != None else ""

    def fee_fiat_amount(self):
        fee = self.data["fee"]
        if fee == None:
            return ""
        return (fee["value"] * fee["price"]) / 1e18

    def fee_fiat_currency(self):
        return "USD" if self.fee_amount() != None else ""

    def sender(self):
        # corrects ERC20 transfer receiver/spender
        try:
            json_changes = json.loads(self.changes_json())
        except:
            json_changes = []

        if len(json_changes) > 0:
            return json_changes[0]["address_from"]
        # old logic / fallback for no changes
        return self.data["address_from"]

    def receiver(self):
        # corrects ERC20 transfer receiver/spender
        try:
            json_changes = json.loads(self.changes_json())
        except:
            json_changes = []

        if len(json_changes) > 0:
            return json_changes[0]["address_to"]
        # old logic / fallback for no changes
        return self.data["address_to"]

    def hash(self):
        return self.data["hash"]

    def link(self):
        return f'https://etherscan.io/tx/{self.hash()}'

    def timestamp(self):
        date = int(self.data["mined_at"])
        return datetime.utcfromtimestamp(date).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    def changes_json(self):
        changes = self.data["changes"]

        if (len(changes) == 0):
            return []

        # returns array of indices that match type 'in'
        in_indices = [changes.index(change)
                      for change in changes if change["direction"] == "in"]
        out_indices = [changes.index(change)
                       for change in changes if change["direction"] == "out"]

        obj = []
        in_count = 0
        out_count = 0

        for i in range(len(changes)):
            if i in in_indices:
                is_multiple = len(self.buy_amount().split('\n')) > 1
                amount = self.buy_amount().split('\n')[in_count]
                symbol = self.buy_asset().split('\n')[in_count]
                asset = self.buy_asset_address().split('\n')[in_count]
                fiat_currency = self.buy_fiat_currency().split(
                    '\n')[in_count] if self.buy_fiat_currency() != "" else ""
                fiat_amount = self.buy_fiat_amount().split(
                    '\n')[in_count] if self.buy_fiat_amount() != "" else ""

                obj.append(
                    {
                        "type": "in",
                        "address_from": changes[i]["address_from"],
                        "address_to": changes[i]["address_to"],
                        "amount": amount,
                        "symbol": symbol,
                        "asset": asset,
                        "fiat_currency": fiat_currency,
                        "fiat_amount": fiat_amount,
                    }
                )

                # if multiple values, bump & index next
                in_count += 1 if is_multiple else 0
            if i in out_indices:
                is_multiple = len(self.sell_amount().split('\n')) > 1
                amount = self.sell_amount().split('\n')[out_count]
                symbol = self.sell_asset().split('\n')[out_count]
                asset = self.sell_asset_address().split('\n')[out_count]
                fiat_currency = self.sell_fiat_currency().split('\n')[
                    out_count] if self.sell_fiat_currency() != "" else ""
                fiat_amount = self.sell_fiat_amount().split(
                    '\n')[out_count] if self.sell_fiat_amount() != "" else ""

                obj.append({
                    "type": "out",
                    "address_from": changes[i]["address_from"],
                    "address_to": changes[i]["address_to"],
                    "amount": amount,
                    "symbol": symbol,
                    "asset": asset,
                    "fiat_currency": fiat_currency,
                    "fiat_amount": fiat_amount,
                })

                # if multiple values, bump & index next
                out_count += 1 if is_multiple else 0

        return json.dumps(obj)


class API:
    @sio.event(namespace='/address')
    async def connect():
        global CONNECTED_TO_SOCKET
        print('Connected to /address namespace!')
        CONNECTED_TO_SOCKET = True

    @sio.on('received address transactions', namespace='/address')
    def received_address_transactions(data):
        global ADDRESS_TRANSACTIONS
        print('Address transactions is received')

        if (len(data['meta']) > 0):
            address = data['meta']['address']
            ADDRESS_TRANSACTIONS[address] = data['payload']['transactions']

        else:
            print("Not enough transactions!")

    def results_ready(self) -> bool:
        return len(ADDRESS_TRANSACTIONS.keys()) == len(ADDRESSES)

    async def fetch_address(self, address: str):
        # Request address information
        await sio.emit('subscribe', {
            'scope': ['transactions'],
            'payload': {
                'address': address,
                'currency': 'usd',
                'transactions_limit': 9000,
                'transactions_offset': 0,
                'transactions_search_query': "",
                'wallet_provider': "watchAddress"
            }
        }, namespace='/address')

    async def use_live_data(self):
        # Initiate the connection with the websocket
        await sio.connect(
            f'{URI}/?api_token={API_TOKEN}',
            headers={'Origin': ORIGIN},
            namespaces=['/address'],
            transports=['websocket']
        )

        # Wait until the connection is established
        while not CONNECTED_TO_SOCKET:
            await asyncio.sleep(0)

        # Fetch data
        global ADDRESSES
        for address in ADDRESSES:
            await self.fetch_address(address)

        # Wait until all information about the address is received
        while not self.results_ready():
            await asyncio.sleep(0)


class Zerion:
    utils: Utils

    def __init__(self):
        self.utils = Utils()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.main())

    def xlsx_to_csv(self, address: str):
        # convert to bittytax file
        os.system(f'bittytax_conv {address}.csv')

        wb = xlrd.open_workbook('BittyTax_Records.xlsx')
        sh = wb.sheet_by_name('Zerion')
        replacement_csv = open(f'wallets/{address}.csv', 'w')
        wr = csv.writer(replacement_csv, quoting=csv.QUOTE_ALL)

        # update wallet row & export
        for i in range(sh.nrows):
            row = sh.row_values(i)
            if i != 0:
                # skip tx hash, needs manual editing
                # if (row[35] == "BAD_TX_HERE"):
                #     continue
                # assign address
                row[10] = self.utils.format_address(address)
                # format date properly
                if isinstance(row[11], float):
                    row[11] = xlrd.xldate_as_datetime(row[11], wb.datemode)
                # assign type correctly
                # both sender + receiver = deposit/withdrawal
                sender = row[33]
                receiver = row[34]
                if (sender == address and receiver == address):
                    print("FIXME: SELF SEND TX")
                if (sender in ADDRESSES and receiver in ADDRESSES):
                    if (sender == address):
                        if (row[0] == 'Withdrawal'):
                            print("Withdrawal", i)
                            row[0] = 'Withdrawal'
                    else:
                        if (row[0] == 'Deposit'):
                            row[0] = 'Deposit'
                            print("Deposit", i)
                else:
                    if (sender in ADDRESSES or receiver in ADDRESSES):
                        if (sender in ADDRESSES and row[0] == 'Withdrawal'):
                            row[0] = 'Spend'
                        if (receiver in ADDRESSES and row[0] == 'Deposit'):
                            row[0] = 'Gift-Received'
                            # if no buy asset, skip
                            if (row[2] == ""):
                                continue
                    else:
                        # Unknowns
                        if (row[0] == 'Deposit'):
                            # if no buy asset, skip
                            if (row[2] == ""):
                                continue
                            row[0] = 'Gift-Received'
                        if (row[0] == 'Withdrawal'):
                            row[0] = 'Spend'

            # FIXME: - This removes double txs but we need them
            if (row[0] != ""):
                wr.writerow(row)

        replacement_csv.close()
        os.remove(f"{address}.csv")
        os.remove('BittyTax_Records.xlsx')

    def merge_csv(self):
        all_filenames = [i for i in glob.glob('wallets/*.csv')]
        combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
        combined_csv.to_csv("outputs/zerion.csv",
                            index=False, encoding='utf-8-sig')

    def clear_wallets(self):
        [os.remove(f) for f in glob.glob('wallets/*.csv')]

    def compile_data(self):
        for address in ADDRESSES:
            with open(f'{address}.csv', 'w', newline='') as csvfile:
                fieldnames = ['Date', 'Time', 'Transaction Type', 'Status', 'Application', 'Accounting Type', 'Buy Amount', 'Buy Currency', 'Buy Currency Address', 'Buy Fiat Amount',	'Buy Fiat Currency', 'Sell Amount',
                              'Sell Currency', 'Sell Currency Address', 'Sell Fiat Amount', 'Sell Fiat Currency', 'Fee Amount', 'Fee Currency', 'Fee Fiat Amount', 'Fee Fiat Currency', 'Sender', 'Receiver', 'Tx Hash', 'Link', 'Timestamp', 'Changes JSON']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                print(ADDRESS_TRANSACTIONS.keys(), address)
                for transaction in ADDRESS_TRANSACTIONS[address]:
                    tx = Transaction(transaction)
                    if (tx.direction == "self"):
                        continue
                    if (tx.type() == "Contract Execution"):
                        continue
                    writer.writerow({
                        'Date': tx.date(),
                        'Time': tx.time(),
                        'Transaction Type': tx.type(),
                        'Status': tx.status(),
                        'Application': tx.application(),
                        'Accounting Type': tx.accounting_type(),
                        'Buy Amount': tx.buy_amount(),
                        'Buy Currency': tx.buy_asset(),
                        'Buy Currency Address': tx.buy_asset_address(),
                        'Buy Fiat Amount': tx.buy_fiat_amount(),
                        'Buy Fiat Currency': tx.buy_fiat_currency(),
                        'Sell Amount': tx.sell_amount(),
                        'Sell Currency': tx.sell_asset(),
                        'Sell Currency Address': tx.sell_asset_address(),
                        'Sell Fiat Amount': tx.sell_fiat_amount(),
                        'Sell Fiat Currency': tx.sell_fiat_currency(),
                        'Fee Amount': tx.fee_amount(),
                        'Fee Currency': tx.fee_currency(),
                        'Fee Fiat Amount': tx.fee_fiat_amount(),
                        'Fee Fiat Currency': tx.fee_fiat_currency(),
                        'Sender': tx.sender(),
                        'Receiver': tx.receiver(),
                        'Tx Hash': tx.hash(),
                        'Link': tx.link(),
                        'Timestamp': tx.timestamp(),
                        'Changes JSON': tx.changes_json(),
                    })
            # convert to csv
            self.xlsx_to_csv(address)

    def load_wallets(self):
        with open(f'wallets/known.txt', 'r', newline='') as txtfile:
            return [x.replace('\n', '').lower() for x in txtfile]

    async def main(self):
        # setup api
        api = API()

        # wipe /wallet dir of csvs
        self.clear_wallets()

        global ADDRESSES
        ADDRESSES = self.load_wallets()
        await api.use_live_data()

        print('Compiling data...\n')
        self.compile_data()

        print('Merging CSV...\n')
        self.merge_csv()
