list_stock = ['IMOEX', 'ROSN', 'OZON', 'GMKN', 'NVTK', 'PIKK', 'SBER']


def add_stock(value):
    # Создается список по условию + перезаписывается по новым запросам, IMOEX остается, остальные форматируются
    if value not in 'IMOEX':
        list_stock.insert(1, value)
        if list_stock.count(value) > 1:
            list_stock.pop(list_stock.index(value, 2))
        if len(list_stock) > 6:
            list_stock.pop()
    return list_stock


if __name__ == '__main__':
    print(add_stock('123'))
    print(add_stock('456'))
    print(add_stock('789'))
    print(add_stock('IMOEX'))