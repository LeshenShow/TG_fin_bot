import yfinance
import sqlite3 as sq
import requests
from bs4 import BeautifulSoup
import re

# ~~~Создание таблицы stock_bd.db~~~
with sq.connect('stock_bd.db') as con:
    cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS stock_bd (
        ticker_id INTEGER PRIMARY KEY AUTOINCREMENT,
        tick TEXT UNIQUE,
        sector TEXT,
        name TEXT,
        moex_check TEXT,
        leshen_check TEXT,
        moex_k TEXT,
        leshen_k TEXT,
        capital INTEGER,
        moex_capital INTEGER,
        leshen_capital INTEGER,
        moex_share FLOAT,
        leshen_share FLOAT
        )""")

# # ~~~Обработка данных с эксель в stock_bd.db~~~
with open("for_bd.csv", "r", encoding="utf-8") as file:
    file = file.readlines()
file_result = []

for x, y in enumerate(file):
    file_result.append(y.split(','))
    file_result[x] = file_result[x]+[3, 2, 4, 6.0, 5.0]

# ~~~Запись данных в stock_bd.db~~~
with sq.connect('stock_bd.db') as con:
    cur = con.cursor()
    for x in file_result:
        x = tuple(x)
        try:
            cur.execute('INSERT INTO stock_bd VALUES(NULL, ?, ?, ?, ?, ? ,?, ?, ?, ?, ?, ?, ?)', x)
        except:
            cur.execute('REPLACE INTO stock_bd VALUES(NULL, ?, ?, ?, ?, ? ,?, ?, ?, ?, ?, ?, ?)', x)

# ~~~Сбор данных о капитализации~~~
capital_stock = []
for _, info in enumerate(file_result):
    result = []
    flag = False
    try:
        url = f'http://iss.moex.com/iss/engines/stock/markets/shares/securities/{info[0]}.xml'
        pages = requests.get(url, timeout=(5, 5))
        soup = BeautifulSoup(pages.text, 'xml')
        info_moex = soup.find_all('row')
        for x in info_moex:
            if x.get('ISSUECAPITALIZATION') not in (None, ''):
                cap = int(float(x.get('ISSUECAPITALIZATION')))
                flag = True
                print('moex', info[0], cap)
                break
        if not flag:
            url = f'https://eninvs.com/company.php?name={info[0]}'
            pages = requests.get(url, timeout=(5, 5))
            soup = BeautifulSoup(pages.text, 'lxml')
            info_eninvs = soup.find('div', id='main_fin_data').text
            info_eninvs = re.split("\n|:|: |;|,", info_eninvs)
            for _ in info_eninvs:
                if _ not in ('', ' '):
                    result.append(_.strip(' '))
            result = {_: __ for _, __ in enumerate(result)}
            qty = result.get(22).split(' ')
            price = float(result.get(19))
            cap = int((int(''.join(qty)) * price))
            print('parser', info[0], cap)
    except:
        info_yahoo = yfinance.Ticker(info[0] + ".ME").info
        cap = info_yahoo.get('marketCap')
        print('yahoo', info[0], cap)
    capital_stock.append([info[0], cap])

# # ~~~Запись капитализации, долей~~~
with sq.connect('stock_bd.db') as con:
    cur = con.cursor()
    for _, y in enumerate(capital_stock):
        cur.execute(f"UPDATE stock_bd "
                    f"SET capital ={y[1]} "
                    f"where tick ='{y[0]}'")  # {y[1]}
        cur.execute(f"UPDATE stock_bd "
                    f"SET moex_capital = capital * moex_k, "
                    f"leshen_capital = capital * leshen_k "
                    f"where tick ='{y[0]}'")
    cur.execute(f"UPDATE stock_bd "
                f"SET leshen_capital= max"
                f"((SELECT sum(capital)*0.0005 "
                f"from stock_bd "
                f"where leshen_check= 'LESHEN'), "
                f"(select capital * leshen_k)) "
                f"where leshen_check= 'LESHEN'")
    sum_cap_leshen = cur.execute("SELECT sum(leshen_capital) "
                                 "FROM stock_bd").fetchone()[0]
    sum_cap = cur.execute("SELECT sum(moex_capital) "
                          "FROM stock_bd").fetchone()[0]
    for _, y in enumerate(capital_stock):
        cap_from_id = cur.execute(f"SELECT moex_capital "
                                  f"FROM stock_bd "
                                  f"where tick='{y[0]}'").fetchone()[0]
        share = float("{0:.2f}".format(cap_from_id / sum_cap * 100))
        cur.execute(f"UPDATE stock_bd "
                    f"SET moex_share = {share} "
                    f"where tick ='{y[0]}'")
        cap_from_id = cur.execute(f"SELECT leshen_capital "
                                  f"FROM stock_bd "
                                  f"where tick='{y[0]}'").fetchone()[0]
        share = float("{0:.2f}".format(cap_from_id / sum_cap_leshen * 100))
        cur.execute(f"UPDATE stock_bd "
                    f"SET leshen_share = {share} "
                    f"where tick='{y[0]}'")