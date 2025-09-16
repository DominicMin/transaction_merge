import pandas as pd
import numpy as np
import openpyxl
import itertools
import io
import os
import sys
from datetime import datetime
from pathlib import Path
import glob

def read_file(filepath):    
    def open_file_skip_lines(filepath, num_lines_to_skip=25):
        with open(filepath, 'r', encoding='gbk') as f:
            # 使用 islice 跳过前 num_lines_to_skip 行
            remaining_lines = itertools.islice(f, num_lines_to_skip, None)
            
            # 将剩下的行内容合并成一个字符串
            content_after_skip = "".join(remaining_lines)
            
            # 将字符串内容封装成一个 io.StringIO 对象，使其具有文件对象的行为
            return io.StringIO(content_after_skip)
    fields = ['time', 'type', 'counterparty', 'account', 'name', 'direction', 'amount', 'payment', 'status', 'id', '_2', '_3', '_4']
    file_io = open_file_skip_lines(filepath)
    raw = pd.read_csv(file_io, encoding='gbk', names=fields)

    return raw

def clean_raw(raw: pd.DataFrame, idx=False):
    raw = raw.copy()
    if idx:
        raw = raw.iloc[idx]

    # Clean each columns
    raw.drop(columns=['account', '_2', '_3', '_4'], inplace=True)
    raw.time = raw.time = raw.time.apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    refund_idx = raw[(raw.status == '退款成功') | (raw.status == '交易关闭')].index
    raw.drop(index=refund_idx, inplace=True)
    raw.direction = raw.direction.map({'支出': 0, '不计收支': -1, '收入': 1})

    # Handle 'direction == -1' cases:
    case_idx = raw[raw.direction == -1].index
    rec_to_concat = pd.DataFrame()
    for i in case_idx:
        if raw.loc[i, 'name'].startswith('余额宝-转出'):
            # Add a new record
            raw.loc[i, 'direction'] = 1
            new_rec = raw.loc[i].copy()
            new_rec['counterparty'] = '余额宝-转出到银行卡'
            new_rec['direction'] = 0
            new_rec['payment'] = '余额宝'
            rec_to_concat = pd.concat([rec_to_concat, new_rec.to_frame().T], ignore_index=True)
            
        elif raw.loc[i, 'name'].endswith('转入'):
            raw.loc[i, 'direction'] = 0
            raw.loc[i, 'counterparty'] = '银行卡-转入到余额宝'
            new_rec = raw.loc[i].copy()
            new_rec['counterparty'] = '余额宝-转入'
            new_rec['direction'] = 1
            new_rec['payment'] = '余额宝'
            rec_to_concat = pd.concat([rec_to_concat, new_rec.to_frame().T], ignore_index=True)

        elif raw.loc[i, 'name'].startswith('提现'):
            raw.loc[i, 'direction'] = 0
            new_rec = raw.loc[i].copy()
            new_rec['counterparty'] = '提现'
            new_rec['direction'] = 1
            new_rec['payment'] = '中国银行储蓄卡(7633)'
            rec_to_concat = pd.concat([rec_to_concat, new_rec.to_frame().T], ignore_index=True)

        elif raw.loc[i, 'name'].endswith('收益发放'):
            raw.loc[i, 'direction'] = 1
            raw.loc[i, 'counterparty'] = raw.loc[i, 'name']
    
    raw = pd.concat([raw, rec_to_concat], ignore_index=True)
    raw.payment = raw.payment.fillna('账户余额')
    raw.payment = raw.payment.map({'余额': '支付宝余额', '账户余额': '支付宝余额', '账户余额&碰一下立减': '支付宝余额'}).fillna(raw.payment)
            
    return raw

def cvt_record(source: pd.DataFrame, idx= False):
    source = source.copy()
    if idx:
        source = source.iloc[idx]
    record = pd.DataFrame(columns=['id', 'date', 'source', 'category', 'name', 'amt (RMB)', 'amt (Foreign)', 'balance', 'note'])
    record['id'] = source['id']
    record['date'] = source['time'].apply(lambda x: datetime.strftime(x, '%y%m%d'))
    record['source'] = source['payment']
    record['name'] = source['counterparty']
    record['amt (RMB)'] = source.apply(lambda x:
    -x['amount'] if x['direction'] == 0
    else x['amount'],
    axis=1)

    record = record.sort_values(by='date', ascending=True)
    
    return record

def cvt_all(dir='source/alipay'):
    source_list = glob.glob(f'{dir}/*.csv')
    rec_list = []
    def cvt_item(filepath):
        raw = read_file(filepath)
        cleaned = clean_raw(raw)
        s = cleaned['time'].min().strftime('%y%m%d'); e = cleaned['time'].max().strftime('%y%m%d')
        os.rename(filepath, f'{dir}/alipay_{s}_{e}{os.path.splitext(filepath)[-1]}')
        record = cvt_record(cleaned)
        return record
    for f in source_list:
        rec_list.append(cvt_item(f))
    
    all_rec = pd.concat(rec_list, ignore_index=True)
    return all_rec

def main():
    file_path = Path(sys.argv[1])
    output_path = file_path.parent / f"alipay_output_{datetime.now().strftime('%y%m%d_%H-%M-%S')}.xlsx"
    print(f'Reading {file_path}...')
    try:
        raw = read_file(file_path)
        print('Complate!')
    except Exception as e:
        print(f'Reading failed: {e}')
    record = clean_raw(raw)
    record = cvt_record(record)
    
    record.to_excel(output_path, index=False)

if __name__ == "__main__":
    main()