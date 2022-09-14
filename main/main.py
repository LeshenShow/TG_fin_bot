import telebot
from telebot import types
from config.config import my_token, list_index, text_info
from moex_stock_class.short_info import ShortInfoForStock as ShIFS
from inlinekb.callback_class import CallbackInfoForStock as CbIFS
from reply_keyboard_button_stock.rkb_stock_list import add_stock
from charts.chart_for_cb import chart_top_10, chart_any, chart_stock_vs_snp
from currency.currency_class import CurrencyParser
import config.config
bot = telebot.TeleBot(config.config.my_token)


@bot.message_handler(commands=['info'])
@bot.message_handler(func=lambda message: message.text in 'info')
def info_for_new_users(message):
    bot.reply_to(message, text_info)


@bot.message_handler(func=lambda message: message.text.endswith(('$', "€", '¥', 'eur')))
def text_private_moex_stock(message):
    answer = CurrencyParser(message.text)
    bot.reply_to(message, answer.cur_info)


@bot.message_handler(func=lambda message: 3 < len(message.text) < 6 or message.text in ['snp', 's&p'],
                     chat_types=['private'])
def text_private_stock(message):
    temp_obj = ShIFS(message)
    answer = temp_obj.get_info()
    if message.text in ['snp', 's&p']:
        message.text = '^GSPC'

    if temp_obj.price:
        with open("../files/last_stock_cb.txt", "w+") as last_stock_cb:
            last_stock_cb.write(temp_obj.clean_mes)
        btns: list = add_stock(temp_obj.clean_mes)
        last_stock = types.ReplyKeyboardMarkup(resize_keyboard=True,
                                               one_time_keyboard=False,
                                               row_width=8).add(*btns)
        bot.reply_to(message,
                     f"<code>{answer}</code>",
                     parse_mode='HTML',
                     reply_markup=last_stock)

        cb_btn_info = [types.InlineKeyboardButton(text='Показатели от Yahoo', callback_data='indicators_stock')]
        if not temp_obj.russia:
            cb_btn_info.append(types.InlineKeyboardButton(text='График за год', callback_data='stock_vs_snp'))
        if message.text == 'IMOEX':
            cb_btn_info = [types.InlineKeyboardButton(text='Yahoo inf.', callback_data='indicators'),
                           types.InlineKeyboardButton(text='Top 10', callback_data='composition_10'),
                           types.InlineKeyboardButton(text='📊 №1-10', callback_data='pie_10'),
                           types.InlineKeyboardButton(text='> № 10', callback_data='composition_any'),
                           types.InlineKeyboardButton(text='📊 >№10', callback_data='chart_any'),
                           ]
        ikb = types.InlineKeyboardMarkup(row_width=8).add(*cb_btn_info)
        bot.send_message(message.chat.id,
                         f"<code>Доп. инфо по {temp_obj.clean_mes}</code>",
                         reply_markup=ikb,
                         parse_mode='HTML'
                         )
    else:
        bot.reply_to(message, f"<code>{answer}</code>", parse_mode='HTML')


@bot.message_handler(func=lambda message: message.text.startswith('!') and 3 < len(message.text) < 7,
                     chat_types=['supergroup'])
def text_supergroup_stock(message):
    check = ShIFS(message)
    bot.reply_to(message, message.from_user.first_name + ',group ' + check.get_info(), parse_mode='HTML')


@bot.message_handler(chat_types=['supergroup'])
def text_private_stock(message):
    print(message.text)


@bot.callback_query_handler(func=lambda callback: callback.data)
def cb_stock(callback):
    cb_btn_info = [types.InlineKeyboardButton(text='Yahoo inf.', callback_data='indicators'),
                   types.InlineKeyboardButton(text='Состав 1-10', callback_data='composition_10'),
                   types.InlineKeyboardButton(text='📊 №1-10', callback_data='pie_10'),
                   types.InlineKeyboardButton(text='Состав ост.', callback_data='composition_any'),
                   types.InlineKeyboardButton(text='📊 >№10', callback_data='chart_any'),
                   ]

    cb_btn_info_stock = [types.InlineKeyboardButton(text=f'Показатели от Yahoo',
                                                    callback_data='indicators_stock'),
                         types.InlineKeyboardButton(text='График за год', callback_data='stock_vs_snp')
                         ]
    if callback.data == 'indicators_stock':
        bot.send_message(callback.message.chat.id, f"<code>{CbIFS().cb_info}</code>", parse_mode='HTML')
    elif callback.data == 'stock_vs_snp':
        with open("../files/last_stock_cb.txt", "r") as last_stock_cb:
            last_stock_cb = last_stock_cb.readlines()[0]
        chart_stock_vs_snp(last_stock_cb)
        ikb = types.InlineKeyboardMarkup(row_width=5).add(cb_btn_info_stock[0])
        bot.send_photo(callback.message.chat.id, photo=open('../files/stock_vs_snp.png', 'rb'), reply_markup=ikb)
    elif callback.data == 'indicators':
        print(callback.message.text)
        ikb = types.InlineKeyboardMarkup(row_width=5).add(*cb_btn_info[1:])
        bot.reply_to(callback.message, f"<code>'_(cb-info)_' + {CbIFS().cb_info}</code>", parse_mode='HTML',
                     reply_markup=ikb)
    elif callback.data == 'composition_10':
        ikb = types.InlineKeyboardMarkup(row_width=5).add(cb_btn_info[0], *cb_btn_info[2:])
        bot.send_message(callback.message.chat.id, f"<code>{list_index[0]}</code>", parse_mode='HTML',
                         reply_markup=ikb)
    elif callback.data == 'pie_10':
        chart_top_10()
        ikb = types.InlineKeyboardMarkup(row_width=5).add(*cb_btn_info[:2], *cb_btn_info[3:])
        bot.send_photo(callback.message.chat.id, photo=open('../files/chart_top_10.png', 'rb'), reply_markup=ikb)
    elif callback.data == 'composition_any':
        ikb = types.InlineKeyboardMarkup(row_width=5).add(*cb_btn_info[:3], cb_btn_info[-1])
        bot.send_message(callback.message.chat.id, f"<code>{list_index[1]}</code>", parse_mode='HTML',
                         reply_markup=ikb)
    elif callback.data == 'chart_any':
        chart_any()
        ikb = types.InlineKeyboardMarkup(row_width=5).add(*cb_btn_info[:4])
        bot.send_photo(callback.message.chat.id, photo=open('../files/chart_any.png', 'rb'), reply_markup=ikb)


if __name__ == '__main__':
    print('lets go', 'start handlers')
    bot.polling()
