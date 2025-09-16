import pandas as pd
import numpy as np
import openpyxl
import sys
import os
from datetime import datetime
from pathlib import Path
import glob

def read_file(filepath):
    raw = pd.read_excel(filepath)
    for i in raw.index:
        if raw.iloc[i, 0] == '交易时间':
            raw = raw.iloc[i+1:, :]
            break
    return raw

def clean_raw(raw: pd.DataFrame):
    raw = raw.copy()
    raw.drop(columns=['Unnamed: 9', 'Unnamed: 10'], inplace=True)
    raw.columns = ['time', 'type', 'counterparty', 'name', 'direction', 'amount', 'payment', 'status', 'id']

    # Clean each columns
    refund_idx = raw[raw.status == '已全额退款'].index
    raw.drop(index = refund_idx, inplace=True)
    raw.payment = raw.payment.map({'/': '零钱'}).fillna(raw.payment)
    raw.time = raw.time.apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    raw.direction = raw.direction.map({'收入': 1, '支出': 0, '/': -1})

    # Handle '/' cases
    rec_to_concat = pd.DataFrame()
    for i, r in raw.iterrows():
        if r['type'].startswith('微信红包'):
            if r['direction'] == 1:
                raw.loc[i, 'counterparty'] = '红包 from ' + raw.loc[i, 'counterparty']
            else:
                raw.loc[i, 'counterparty'] = '红包 to ' + raw.loc[i, 'counterparty']
        if r['type'] == '转账':
            if r['direction'] == 1:
                raw.loc[i, 'payment'] = '零钱'
                raw.loc[i, 'counterparty'] = '转账 from ' + raw.loc[i, 'counterparty']
            else:
                raw.loc[i, 'counterparty'] = '转账 to ' + raw.loc[i, 'counterparty']
        if r['type'] == '零钱提现':
            raw.loc[i, 'counterparty'] = '提现 to ' + raw.loc[i, 'counterparty']
            raw.loc[i, 'direction'] = 1
            new_rec = raw.loc[i].copy()
            new_rec['payment'] = '零钱'
            new_rec['direction'] = 0
            rec_to_concat = pd.concat([rec_to_concat, new_rec.to_frame().T])
    
    raw = pd.concat([raw, rec_to_concat], ignore_index=True)

    raw.amount = raw.apply(lambda x: 
    -float(x['amount'][1:]) if x['direction'] == 0
    else float(x['amount'][1:]),
    axis=1)

    return raw

def cvt_record(source: pd.DataFrame, idx=False):
    source = source.copy()
    if idx:
        source = source.iloc[idx]
    source = source.copy()
    record = pd.DataFrame(columns=['id', 'date', 'source', 'category', 'name', 'amt (RMB)', 'amt (Foreign)', 'balance', 'note'])
    record['id'] = source['id']
    record['date'] = source.time.apply(lambda x: datetime.strftime(x, '%y%m%d'))
    record['source'] = source.payment
    record['category']= source.type.map({'微信红包': '红包', '微信红包（群红包）': '红包'})
    record['name'] = source.counterparty
    record['amt (RMB)'] = source.amount

    record = record.sort_values(by='date', ascending=True)

    return record

def cvt_all(dir='source/wechat'):
    source_list = glob.glob(f'{dir}/*.xlsx')
    rec_list = []
    def cvt_item(filepath):
        raw = read_file(filepath)
        cleaned = clean_raw(raw)
        s = cleaned['time'].min().strftime('%y%m%d'); e = cleaned['time'].max().strftime('%y%m%d')
        os.rename(filepath, f'{dir}/wechat_{s}_{e}{os.path.splitext(filepath)[-1]}')
        record = cvt_record(cleaned)
        return record
    for f in source_list:
        rec_list.append(cvt_item(f))
    
    all_rec = pd.concat(rec_list, ignore_index=True)
    return all_rec

def main():
    file_path = Path(sys.argv[1])
    output_path = file_path.parent / f"wx_output_{datetime.now().strftime('%y%m%d_%H-%M-%S')}.xlsx"
    print(f'Reading {file_path}...')
    try:
        raw = pd.read_excel(file_path)
        for i in raw.index:
            if raw.iloc[i, 0] == '交易时间':
                raw = raw.iloc[i+1:, :]
                break
        print('Complate!')
    except Exception as e:
        print(f'Reading failed: {e}')
    record = clean_raw(raw)
    record = cvt_record(record)
    
    record.to_excel(output_path, index=False)

if __name__ == "__main__":
    main()