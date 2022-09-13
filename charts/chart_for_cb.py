from config.config import moex
import matplotlib.pyplot as plt
import yfinance
import pandas as pd

color = ('#bf3f27', '#5d9497', '#eacc7c', '#92c1df', '#ca9eca', '#8ecb73')


def chart_top_10():
    moex_share_10 = [float(x[-1].strip(' ')) for x in moex][:10]
    moex_tick_10 = [x[0].strip(' ') for x in moex][:10]
    fig, ax = plt.subplots(figsize=(10, 10))
    wedges, texts, autotexts = ax.pie(moex_share_10,
                                      labels=moex_tick_10,
                                      colors=color,
                                      autopct=lambda v: "{:.1f}%".format(v * sum(moex_share_10) / 100, v),
                                      pctdistance=0.8,
                                      wedgeprops={'width': 0.5, 'lw': 0, 'edgecolor': "k"},
                                      startangle=90,
                                      counterclock=False,
                                      rotatelabels=False
                                      )
    ax.axis("equal")
    fig.set_facecolor('#182633')
    plt.setp(texts, size=14, weight=True, color='#a8bed6')
    plt.setp(autotexts, size=14, rotation=0)
    plt.savefig("../files/chart_top_10.png", bbox_inches='tight', dpi=200)
    return


def chart_any():
    moex_share_any = [float(x[-1].strip(' ')) for x in moex][10:]
    moex_tick_any = [x[0].strip(' ') for x in moex][10:]
    fig, ax = plt.subplots(figsize=(10, 10))
    chart = plt.barh(moex_tick_any, moex_share_any, color=color, ecolor='r')
    plt.xticks(rotation='vertical', color='#a8bed6')
    plt.yticks(fontsize=14, color='#a8bed6')
    fig.set_facecolor('#182633')
    ax.patch.set_facecolor('#182633')
    ax.spines['left'].set_color('#a8bed6')
    ax.spines['right'].set_color('#182633')
    ax.spines['bottom'].set_color('#182633')
    ax.spines['top'].set_color('#182633')
    ax.tick_params(axis='x', colors='#182633')
    ax.tick_params(axis='y', color='#a8bed6')
    ax.invert_yaxis()
    ax.bar_label(chart,
                 labels=["{:.2f}%".format(float(v)) for v in moex_share_any],
                 padding=3,
                 color='#a8bed6',
                 fontsize=14
                 )
    ax.set_xlim(right=max(moex_share_any + [3]))
    plt.savefig("../files/chart_any.png", bbox_inches='tight', dpi=200)
    return


def chart_stock_vs_snp(ticker):
    data = yfinance.download(f'{ticker}', period='1y', interval='1wk')['Adj Close']
    df = pd.DataFrame(data)
    price = df.values.tolist()
    index = df.index.tolist()

    data_snp_500 = yfinance.download('^GSPC', period='1y', interval='1wk')['Adj Close']
    df = pd.DataFrame(data_snp_500)
    price_snp_500 = df.values.tolist()

    price_snp_500_clean = []
    for x in price_snp_500:
        for q in x:
            price_snp_500_clean.append(str(q))

    price_clean = []
    for x in price:
        for q in x:
            price_clean.append(str(q))

    index_clean = []
    for x in index:
        index_clean.append(str(x).strip('Timestamp(').split()[0])

    for _ in price_clean:
        if price_clean.count('nan') > 0:
            check2 = price_clean.index('nan')
            price_clean.pop(check2)
            index_clean.pop(check2)

    price_clean = [float(x) for x in price_clean]
    price_snp_500_clean = [float(x) for x in price_snp_500_clean]

    date = index_clean
    price = price_clean
    price2 = price_snp_500_clean

    fig, ax = plt.subplots(figsize=(12, 5))
    chart = plt.plot(date, price, color='#bf3f27', marker='o', linestyle='solid', linewidth=2, markersize=6)
    ax2 = ax.twinx()
    ax2.plot(date, price2, color='#5d9497', marker='_', linestyle='dashed', linewidth=1, markersize=6)

    ax.set_xlabel("Date", color='#a8bed6', fontsize=14)
    ax.tick_params(axis='x', rotation=90, colors='#a8bed6', labelsize=10)
    ax.set_ylabel(f'{ticker}', color='#bf3f27', fontsize=14)
    ax.tick_params(axis='y', rotation=0, color='#bf3f27', labelcolor='#bf3f27', labelsize=14)
    ax.grid(alpha=.1)
    ax.spines['top'].set_color('#182633')

    ax2.set_ylabel("S&P 500", color='#5d9497', fontsize=14)
    ax2.tick_params(axis='y', color='#5d9497', labelcolor='#5d9497', labelsize=14)
    fig.set_facecolor('#182633')
    ax.patch.set_facecolor('#182633')

    ax2.spines['top'].set_color('#182633')
    ax2.spines['left'].set_color('#bf3f27')
    ax2.spines['right'].set_color('#5d9497')
    ax2.spines['bottom'].set_color('#a8bed6')
    plt.savefig("../files/stock_vs_snp.png", bbox_inches='tight', dpi=200)
    return


if __name__ == '__main__':
    chart_top_10()
    chart_any()
