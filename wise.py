import argparse
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import sys
from pathlib import Path
import glob
import os

def clean_raw(raw: pd.DataFrame):
    raw = raw.copy()
    raw.drop(columns=['Reference', 'Batch', 'Created by',
       'Category', 'Note'], inplace=True)
    raw.Status = raw.Status.map({'COMPLETED': 1, 'REFUNDED': -1, 'CANCELLED': 0})
    raw.Direction = raw.Direction.map({'OUT': 0, 'IN': 1})
    raw['Created on'] = raw['Created on'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    raw['Finished on'] = raw['Finished on'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    raw.drop(index=raw[raw['Source amount (after fees)'] == 0].index, inplace=True)

    return raw

def cvt_record(source: pd.DataFrame, idx= False):
    source = source.copy()
    if idx:
        source = source.iloc[idx]
    record = pd.DataFrame(columns=['id', 'date', 'source', 'category', 'name', 'amt (RMB)', 'amt (Foreign)', 'balance', 'note'])
    record['id'] = source['ID']
    record['date'] = source['Created on'].apply(lambda x: datetime.strftime(x, '%y%m%d'))
    record['source'] = 'Wise'
    record['name'] = source['Target name']
    record['amt (Foreign)'] = source.apply(lambda x: 
    -x['Source amount (after fees)'] if x['Direction'] == 0 else x['Source amount (after fees)'], axis=1)
    
    return record

def cvt_all(dir='source/wise'):
    source_list = glob.glob(f'{dir}/*.csv')
    rec_list = []
    print('Wise', '-' * 30)
    def cvt_item(filepath):
        raw = pd.read_csv(filepath)
        cleaned = clean_raw(raw)
        s = cleaned['Created on'].min().strftime('%y%m%d'); e = cleaned['Created on'].max().strftime('%y%m%d')
        print(f'Detected data from {s} to {e}')
        os.rename(filepath, f'{dir}/wise_{s}_{e}{os.path.splitext(filepath)[-1]}')
        record = cvt_record(cleaned)
        return record
    for f in source_list:
        rec_list.append(cvt_item(f))
    
    print('Process complete for Wise!\n')
    all_rec = pd.concat(rec_list, ignore_index=True)
    return all_rec


def main():
    file_path = Path(sys.argv[1])
    output_path = file_path.parent / f"wise_output_{datetime.now().strftime('%y%m%d_%H-%M-%S')}.xlsx"
    print(f'Reading {file_path}...')
    try:
        raw = pd.read_csv(file_path)
        print('Complate!')
    except Exception as e:
        print(f'Reading failed: {e}')
    record = clean_raw(raw)
    record = cvt_record(record)
    
    record.to_excel(output_path, index=False)

if __name__ == "__main__":
    main()