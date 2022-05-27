from datetime import datetime
import asyncio
import socketio
import csv
import json
import pandas as pd
import xlrd
import os
import glob
import re


class ColumnIndices:
    timestamp = 0
    notes = 0
    asset = 0
    type = 0

    def __init__(self, header):
        self.notes = list(filter(lambda x: (x > 0), [
            i if x == 'Notes' else 0 for (i, x) in enumerate(header)]))[0]
        self.timestamp = list(filter(lambda x: (x > 0), [
            i if x == 'Timestamp' else 0 for (i, x) in enumerate(header)]))[0]
        self.asset = list(filter(lambda x: (x > 0), [
            i if x == 'Asset' else 0 for (i, x) in enumerate(header)]))[0]
        self.type = 0


class Utils:
    addresses = []
    indices: ColumnIndices

    def __init__(self):
        self.addresses = self.load_addresses()

    def format_address(self, address):
        return f"{address[0:6]}...{address[-4:]}"

    def load_addresses(self):
        with open(f'wallets/known.txt', 'r', newline='') as txtfile:
            return [x.replace('\n', '').lower() for x in txtfile]

    def coinbase_row(self, i: int, row: list, datemode):
        notes_index = self.indices.notes
        timestamp_index = self.indices.timestamp
        asset_index = self.indices.asset
        type_index = self.indices.type

        # skip header
        if i == 0:
            return row

        # format date properly
        if isinstance(row[timestamp_index], float):
            row[timestamp_index] = xlrd.xldate_as_datetime(row[11], datemode)

        # skip btc; for now anyway
        # + exclude atom
        if (row[asset_index] == "BTC" or row[asset_index] == "ATOM"):
            return None

        notes = row[notes_index]
        eth_regex_match = re.search(r'0x+.*', notes)

        if (eth_regex_match != None):
            match = eth_regex_match.group().lower()

            # mark unknown addresses as spends
            if match not in self.addresses:
                row[type_index] = 'Spend'

        return row

    def convert_and_parse(self, input_path: str, output_path: str, sheet_name: str):
        # convert to bittytax file
        os.system(f'bittytax_conv {input_path}.csv')

        wb = xlrd.open_workbook('BittyTax_Records.xlsx')
        sh = wb.sheet_by_name(f'{sheet_name}')
        replacement_csv = open(f'{output_path}.csv', 'w')
        wr = csv.writer(replacement_csv, quoting=csv.QUOTE_ALL)

        # update wallet row & export
        for i in range(sh.nrows):
            row = sh.row_values(i)

            # header row
            if i == 0:
                # get indices for columns
                self.indices = ColumnIndices(row)
                wr.writerow(row)
            else:
                filtered_row = self.coinbase_row(i, row, wb.datemode)
                if filtered_row != None:
                    wr.writerow(filtered_row)

        replacement_csv.close()
        os.remove('BittyTax_Records.xlsx')
