# STD library
import os
import sqlite3
from datetime import datetime

# local library


# Parameters
today = datetime.today().date()


class Sqlite3Server:

    def __init__(self, path, column_dict):
        self.path = path
        self.data_base = sqlite3.connect(path, check_same_thread=False)
        self.column_dict = column_dict
        self.column_list = list(column_dict.keys())

    def create_tables(self, tokens):
        cursor = self.data_base.cursor()

        for token in tokens:
            column_string = ""
            for element in self.column_dict:
                column_string += f"{element} {self.column_dict[element][0]}, "
            query = f"CREATE TABLE IF NOT EXISTS TOKEN{token} ({column_string[:-2]})"
            # print(query)
            try:
                cursor.execute(query)
                self.data_base.commit()
            except Exception as message:
                print(message)

    def insert_ticks(self, ticks):
        cursor = self.data_base.cursor()

        for tick in ticks:
            try:
                values = []
                table_name = "TOKEN" + str(tick['instrument_token'])
                for key in self.column_dict:
                    name = self.column_dict[key][1]
                    if name == 'exchange_timestamp':
                        values.append(str(tick[name]))
                    else:
                        values.append(tick[name])
                query = f"INSERT or IGNORE INTO {table_name} ({str(self.column_list)[1:-1]}) VALUES {tuple(values)}"
                # print(query)
                cursor.execute(query)
                self.data_base.commit()

            except Exception as e:
                print(e)
                pass


if __name__ == "__main__":

    path = os.getcwd() + f"/{today}.db"
    print(path)


#
# c = db.cursor()
# for m in c.execute('''SELECT * FROM TOKEN975873'''):
#     print(m)
#
# db.commit()
#
# df = pd.read_sql_query("SELECT * FROM TOKEN3861249", db)
# df.to_csv('sample2.csv')
"""
c.execute('SELECT name from sqlite_master where type= "table"')
c.fetchall()

c.execute('''PRAGMA table_info(TOKEN975873)''')
c.fetchall()

for m in c.execute('''SELECT * FROM TOKEN975873'''):
    print(m)
"""