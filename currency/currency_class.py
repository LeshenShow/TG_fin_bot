import requests
from bs4 import BeautifulSoup
import yfinance


class CurrencyParser:
    moex_dict = {"$": 'USD000UTSTOM', "€": 'EUR_RUB__TOM', "¥": 'CNYRUB_TOM', 'r': 'EUR_RUB__TOM'}
    yahoo_dict = {"$": "RUB=X", "€": "EURRUB=X", "¥": "CNYRUB=X"}

    def __init__(self, text):
        self.currency = text[-1]
        val = text[:-1]
        self.val = 1 if val in ['', 'eu'] else val.strip().strip('eu').strip('!')
        try:
            url = f'https://iss.moex.com/iss/engines/currency/markets/selt/securities/' \
                  f'{self.moex_dict.get(self.currency)}.xml'
            pages = requests.get(url, timeout=(5, 5))
            soup = BeautifulSoup(pages.text, 'xml')
            info = soup.find_all('row')
            price = info[1].get('PREVWAPRICE')
        except:
            price = yfinance.Ticker(self.yahoo_dict.get(self.currency)).info.get('bid')
        self.price = round(float(price) * float(self.val), 2)

    @property
    def cur_info(self):
        return self.price


if __name__ == '__main__':
    cp = CurrencyParser('text')
