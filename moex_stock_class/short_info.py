import telebot
from os import getenv
import requests
import re
from bs4 import BeautifulSoup
import yfinance

bot = telebot.TeleBot(getenv("BOT_TOKEN"))


class ShortInfoForStock:
    def __init__(self, message):
        self.message = message
        self.clean_mes = self.message.text.upper().replace('!', '').strip('') \
            if '!' in message.text else self.message.text.upper().strip('')
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, "
                                      "like Gecko) Chrome/73.0.3683.103 Safari/537.36"}
        print('INIT Class ShortInfoForStock')
        self.price = None
        self.cap = None
        self.supplier = None
        self.open_v = None
        self.russia = True

    def info_moex(self):
        print('info_moex', self.message)
        url = f'http://iss.moex.com/iss/engines/stock/markets/shares/securities/{self.clean_mes}.xml'
        pages = requests.get(url, timeout=(5, 5), headers=self.headers)
        soup = BeautifulSoup(pages.text, 'xml')
        info = soup.find_all('row')
        print(info)
        try:
            price = float(info[4].get('MARKETPRICE'))
            print(info[4].get('OPEN', 0))
            open_v = round(price * 100 / (float(info[4].get('OPEN', 0))) - 100, 2)
            if price is None:
                return self.info_enin()
            else:
                cap = float(info[4].get('ISSUECAPITALIZATION', 0))
                supplier = 'moex'
        except (IndexError, TypeError, ValueError):
            return self.info_enin()
        return self.set_info(price, cap, open_v, supplier)

    def info_enin(self):
        try:
            print('info_enin', self.message)
            url = f"https://eninvs.com/company.php?name={self.clean_mes}"
            pages = requests.get(url, timeout=(5, 5), headers=self.headers)
            soup = BeautifulSoup(pages.text, 'lxml')
            info = re.split('\n|:|: |;|,', soup.find('div', id='main_fin_data').text)
            result = {i: x.strip(' ') for i, x in enumerate(info) if x not in ('', ' ')}
            qty = result.get(39).replace(' ', '') if result.get(39) != 'RUB' else result.get(42).replace(' ', '')
            price = float(result.get(33))
            print(result)
            cap = float(qty) * price
            open_v = result.get(7).strip('\xa0()').split('%')[0]
        except (AttributeError, TimeoutError, requests.exceptions.ConnectTimeout):
            return self.info_yfinance()
        supplier = 'eninvs'
        return self.set_info(price, cap, open_v, supplier)

    def info_yfinance(self):
        supplier = 'yahoo'
        try:
            print('info_yfinance', self.message)
            info_yahoo = yfinance.Ticker(self.clean_mes + ".ME").info
            if len(info_yahoo) < 4:
                info_yahoo = yfinance.Ticker(self.clean_mes).info
                self.russia = False
            price = round(info_yahoo.get('regularMarketPrice'), 2)
            print(price)
            open_v = round(info_yahoo.get('open'), 2)
            cap = info_yahoo.get('marketCap')
            if cap is None:
                cap = info_yahoo.get('enterpriseValue')
            if open_v > 50:
                open_v = round(price * 100 / open_v - 100, 2)
            print(info_yahoo)
            print(price, cap)
            with open("../files/info_yahoo.txt", "w+") as info_yahoo_last:
                info_yahoo_last.write(str(info_yahoo))
                print(info_yahoo_last)
        except (AttributeError, TimeoutError, TypeError, requests.exceptions.ConnectTimeout):
            price = cap = open_v = 0
        return self.set_info(price, cap, open_v, supplier)

    def set_info(self, price, cap, open_v, supplier: str = ''):
        self.price = f"<b>{price}</b>, " if price else 0
        self.cap = f"<b>{cap / 1000000000 // 0.1 / 10} </b>млрд., " if cap is not None else ''
        self.supplier = supplier
        self.open_v = f"С открытия:<b>{open_v}</b>%"

    def get_info(self):
        self.info_moex()
        if self.price:
            return f"{self.price} {self.cap} {self.open_v} ➠{self.supplier}"
        else:
            return f"{self.clean_mes} - Значение не найдено или сервер не отвечает"


if __name__ == '__main__':
    print('start short info')
    bot.polling()
