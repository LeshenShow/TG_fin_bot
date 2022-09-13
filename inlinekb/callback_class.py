import telebot
from os import getenv
import yfinance

bot = telebot.TeleBot(getenv("BOT_TOKEN"))


class CallbackInfoForStock:
    def __init__(self):
        print('INIT Class CallbackInfoForStock')
        with open("../files/info_yahoo.txt", 'r') as yh:
            yh = eval(yh.readlines()[0])
            ticker = yh.get('symbol')

        with open("../files/last_stock_cb.txt", "r") \
                as last_stock_cb:
            last_stock_cb = last_stock_cb.readlines()[0]

        info_yahoo = yfinance.Ticker(last_stock_cb + ".ME").info if ticker != last_stock_cb else yh
        price = info_yahoo.get('regularMarketPrice')
        recomend = info_yahoo.get('recommendationKey')
        roe = info_yahoo.get('returnOnEquity')
        pe = info_yahoo.get('trailingPE')
        ps = info_yahoo.get('priceToSalesTrailing12Months')
        pb = info_yahoo.get('priceToBook')
        open_v = info_yahoo.get('open')
        high = info_yahoo.get('regularMarketDayHigh')
        low = info_yahoo.get('regularMarketDayLow')
        close = info_yahoo.get('regularMarketPreviousClose')
        high52 = info_yahoo.get('fiftyTwoWeekHigh')
        low52 = info_yahoo.get('fiftyTwoWeekLow')
        shortname = info_yahoo.get('shortName')
        capital = info_yahoo.get('marketCap')
        sector = info_yahoo.get('sector')

        self.last_stock_cb = f"<b>{last_stock_cb}:</b> "
        self.price = f"{round(price, 2)}\n" if price is not None else ''
        self.shortname = f"{shortname}  " if shortname is not None else ''
        self.sector = f"<b>Sector:</b> {sector}\n" if sector is not None else ''
        self.capital = f"<b>MarketCap:</b> {int(capital):,}\n" if capital is not None else ''
        self.open_v = f"<b>Open:</b> {round(open_v, 2)}  " if open_v is not None else ''
        self.close = f"<b>last Close:</b>  {round(close, 2)}\n" if close is not None else ''
        self.high = f"<b>High:</b> {round(high, 2)}  " if high is not None else ''
        self.low = f"<b>Low:</b> {round(low, 2)}\n" if low is not None else ''
        self.high52 = f"<b>High 52week:</b> {round(high52, 2)}  " if high52 is not None else ''
        self.low52 = f"<b>Low 52week:</b> {round(low52, 2)}\n" if low52 is not None else ''
        self.pe = f"<b>P/E:</b> {round(pe, 2)}  " if pe is not None else ''
        self.ps = f"<b>P/S:</b> {round(ps, 2)}  " if ps is not None else ''
        self.roe = f"<b>ROE:</b> {round(roe * 100, 2)}%  " if roe is not None else ''
        self.pb = f"<b>P/B:</b> {round(pb, 2)}\n" if pb is not None else ''
        self.recomend = f"Yahoo recommendation: <b><u>{recomend}</u></b>" if recomend is not None else ''

    @property
    def cb_info(self):
        try:
            return f"{self.last_stock_cb}" \
                   f"{self.price}" \
                   f"{self.shortname}" \
                   f"{self.sector}" \
                   f"{self.capital}" \
                   f"{self.open_v}" \
                   f"{self.close}" \
                   f"{self.high}" \
                   f"{self.low}" \
                   f"{self.high52}" \
                   f"{self.low52}" \
                   f"{self.pe}" \
                   f"{self.ps}" \
                   f"{self.roe}" \
                   f"{self.pb}" \
                   f"{self.recomend}"
        except TypeError:
            return f" - Значение не найдено!"


if __name__ == '__main__':
    print('start callback class')