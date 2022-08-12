from sqlalchemy import create_engine
import pandas as pd

mysql_usr = 'root'
mysql_pwd = 'root'
mysql_host = 'localhost'
mysql_db = 'apms'
table = 'raw_tbl'

db_connection_str = 'mysql+pymysql://' + mysql_usr + ':' + mysql_pwd + '@' + mysql_host + '/' + mysql_db
db_connection = create_engine(db_connection_str)

df = pd.read_sql('select * from apms.{}'.format(table), con=db_connection)

print(df.values)
print(df.columns)

counter = 0
counter_two = 0
df = df.sort_values(by='ts')
state_prev = df['channel'][0]
current_status = 'Off'


def write_to_sql(data_f,tbl):
    data_fr = data_f.to_frame()
    data_fr = data_fr.swapaxes("index", "columns")
    return data_fr.to_sql(name=tbl, con=db_connection, if_exists='append', index=False)


for ind in df.index:
    if df['channel'][ind] == 'On' and state_prev == 'On':
        counter += 1
        if counter == 1:
            current_status = 'On'
            # Entry in On table
            write_to_sql(df.iloc[ind], 'active_tbl')
            print('always active table ',df.iloc[ind])

    elif df['channel'][ind] == 'Off' and state_prev == 'Off' and current_status != 'On':
        write_to_sql(df.iloc[ind],'inactive_tbl')
        print('always inactive table ', df.iloc[ind])

    elif df['channel'][ind] == 'On' and state_prev == 'Off':
        # Entry in On table
        write_to_sql(df.iloc[ind],'active_tbl')
        print('inactive to active ', df.iloc[ind])

    elif df['channel'][ind] == 'Off' and current_status == 'On':
        counter_two += 1
        if counter_two == 7:
            current_status = 'Off'
            # Entry in Off table after 7 counter
            write_to_sql(df.iloc[ind],'inactive_tbl')
            print('active to inactive', df.iloc[ind])

    state_prev = df['channel'][ind]