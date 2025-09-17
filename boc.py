import pandas as pd
import numpy as np
import openpyxl
import sys
import os
from datetime import datetime
from pathlib import Path
import glob
import io

def read_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        html_lines = []
        for line in f:
            if '<tr>' in line:
                html_lines.append(line.strip())
    single_html_str = ''.join(html_lines)
    html_io = io.StringIO(single_html_str)
    # raw = pd.concat(pd.read_html(html_io), ignore_index=True)
    raw_list = pd.read_html(html_io)

    keep = ['记账日期', '记账时间', '币别', '金额', '余额', '交易名称', '附言', '对方账户名',]
    for i in raw_list:
        i.columns = i.iloc[0]
        i.drop(index=[0], inplace=True)
        drop = [c for c in i.columns if c not in keep]
        i.drop(columns=drop, inplace=True)

    raw = pd.concat(raw_list, ignore_index=True, names=keep)
    return raw

def clean_raw(raw: pd.DataFrame):
    raw = raw.copy()
    raw.columns=['date', 'time', 'currency', 'amount', 'balance', 'name', 'note', 'counterparty']
    raw.date = raw.date.apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
    raw.currency = raw.currency.map({'人民币': 'CNY', '美元': 'USD'})
    raw.amount = raw.amount.apply(lambda x: float(x))
    raw.balance = raw.balance.apply(lambda x: float(x))

    raw.drop(index=raw[raw['name'] == '网上快捷支付'].index, inplace=True)
    raw.drop(index=raw[raw['name'] == '银联入账'].index, inplace=True)
    raw.drop(index=raw[raw['name'] == '网上快捷退款'].index, inplace=True)
    raw.drop(index=raw[raw['name'] == '网上快捷提现'].index, inplace=True)

    return raw

def cvt_record(source: pd.DataFrame, idx=False):
    source = source.copy()
    if idx:
        source = source.iloc[idx]
    source = source.copy()
    record = pd.DataFrame(columns=['id', 'date', 'source', 'category', 'name', 'amt (RMB)', 'amt (Foreign)', 'balance', 'note'])
    record.id = source.time
    record.date = source.date.apply(lambda x: datetime.strftime(x, '%y%m%d'))
    record.source = '中国银行储蓄卡(7633)'
    record.name = source.name 
    record['amt (RMB)'] = source[source.currency == 'RMB'].amount
    record['amt (Foreign)'] = source[source.currency != 'RMB'].amount
    record.balance = source.balance
    
    record = record.sort_values(by='date', ascending=True)
    return record
    

def cvt_all(dir='source/boc'):
    source_list = glob.glob(f'{dir}/*.md')
    rec_list = []
    print('BOC', '-' * 30)
    def cvt_item(filepath):
        raw = read_file(filepath)
        cleaned = clean_raw(raw)
        s = cleaned['date'].min().strftime('%y%m%d'); e = cleaned['date'].max().strftime('%y%m%d')
        print(f'Detected data from {s} to {e}')
        os.rename(filepath, f'{dir}/boc_{s}_{e}{os.path.splitext(filepath)[-1]}')
        record = cvt_record(cleaned)
        return record
    for f in source_list:
        rec_list.append(cvt_item(f))
    
    print('Process complete for BOC!\n')
    all_rec = pd.concat(rec_list, ignore_index=True)
    return all_rec

