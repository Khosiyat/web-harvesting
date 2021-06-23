import bs4 
import datetime as dTime
import os
import pandas as pd
import pandas_datareader.data as dataReader
import pickle
import requests


def save_data():
    request = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')

    beautifilSoup = bs4.BeautifulSoup(request.text, "lxml")
    table = beautifilSoup.find('table', {'class': 'wikitable sortable'})

    stockTickers = []

    for row in table.findAll('tr')[1:]:
        stockTicker_unit = row.findAll('td')[0].text.strip()
        stockTickers.append(stockTicker_unit)

    with open("pickledStockTickers.pickle", "wb") as file:
        pickle.dump(stockTickers, file)
        print(stockTickers)

    return stockTickers
save_data()

def reloadData(reload_data=False):
    if reload_data:
        stockTickers = save_data()
    else:
        with open("pickledStockTickers.pickle", "rb") as file:
            stockTickers = pickle.load(file)

    if not os.path.exists('stock_file'):
        os.makedirs('stock_file')

    startDate = dTime.datetime(2019, 1, 1)
    endDate = dTime.datetime(2021, 12, 31)

    #for stockTicker_unit in stockTickers[:10]:
    for stockTicker_unit in stockTickers:

        print(stockTicker_unit)

        if not os.path.exists('stock_file/{}.csv'.format(stockTicker_unit)):
            try:
                dataFrame = dataReader.DataReader(stockTicker_unit, 'yahoo', startDate, endDate)
                dataFrame.to_csv('stock_file/{}.csv'.format(stockTicker_unit))
            except Exception as ex:
                print('Error:', ex)
        else:
            print('It exists {}'.format(stockTicker_unit))


reloadData(True)

def compileFeatures():
    with open("pickledStockTickers.pickle", "rb") as file:
        stockTickers = pickle.load(file)

    main_DataFrame = pd.DataFrame()

    for calculate, stockTicker_unit in enumerate(stockTickers):
        try:
            dataFrame = pd.read_csv('stock_file/{}.csv'.format(stockTicker_unit))
            dataFrame.set_index('Date', inplace=True)

            dataFrame.rename(columns={'Adj Close': stockTicker_unit}, inplace=True)
            dataFrame.drop(['Open', 'High', 'Low', 'Close', 'Volume'], 1, inplace=True)

            if main_DataFrame.empty:
                main_DataFrame = dataFrame
            else:
                main_DataFrame = main_DataFrame.join(dataFrame, how='outer')

            if calculate % 10 == 0:
                print(calculate)
        except Exception as ex:
                print('Error:', ex)
    print(main_DataFrame.head())
    main_DataFrame.to_csv('compiledStockTickers.csv')


compileFeatures()
