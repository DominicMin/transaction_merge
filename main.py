import alipay, wechat, wise, boc
import pandas as pd
from sqlalchemy import create_engine
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

print('\n\n')
# Get records from all sources
ali_rec = alipay.cvt_all()
wx_rec =wechat.cvt_all()
wise_rec = wise.cvt_all()
boc_rec = boc.cvt_all()

# Concatenate into one dataframe
all_rec = pd.concat([ali_rec, wx_rec, wise_rec, boc_rec], ignore_index=True)
all_rec.drop_duplicates(inplace=True)
all_rec.drop(columns=['id'], inplace=True)

# Sort records
all_rec = all_rec.sort_values(by=['date', 'source'], ascending=[True, True])

# Calculate turnover by sources and write into dataframe
sdate = '250815'
to_sum = all_rec.copy()
to_sum['amt (RMB)'] = to_sum['amt (RMB)'] * 100
to_sum['amt (Foreign)'] = to_sum['amt (Foreign)'] * 100
total_turnover = to_sum.query('date >= @sdate').groupby('source')[['amt (RMB)', 'amt (Foreign)']].agg('sum') / 100
for p in all_rec.source.unique():
    idx = all_rec.query('source == @p').index[-1]
    if p == 'Wise':
        all_rec.loc[idx, 'balance'] = total_turnover.loc[p]['amt (Foreign)']
        all_rec.loc[idx, 'note'] = p
    else:
        all_rec.loc[idx, 'balance'] = total_turnover.loc[p]['amt (RMB)']
        all_rec.loc[idx, 'note'] = p
        
# Export to Excel and SQL
all_rec.to_excel('output.xlsx', index=False)
engine = create_engine('sqlite:///account.db')
all_rec.to_sql('users', con=engine, if_exists='replace', index=False)

# Print summary
print('\n')
print('-' * 56)
print(total_turnover.to_markdown())
print('-' * 56)