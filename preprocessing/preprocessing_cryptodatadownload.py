import pandas as pd
import json
import logging
from logging.handlers import RotatingFileHandler
import os

logger = logging.getLogger('bot_logger')

DIRNAME = 'E:/crypto/project/'
FORMAT = '%(asctime)s %(filename)s'


def find_currency_name(currency_pair_name, second_currencies=['USD'], crypto_names=[]):
    if len(crypto_names):
        for crypto_name in crypto_names:
            if crypto_name in currency_pair_name:
                return crypto_name
    else:
        for second_currenciy in second_currencies:
            if second_currenciy in currency_pair_name:
                return currency_pair_name.split(second_currenciy)[0]


def get_symbols(file_names, path):
    """Get symbols dict from CryptoDataDownload.com filenames"""

    symbols = {}

    for file_name in file_names:
        print(file_name)
        sourse, currency_pair, scale = file_name.split('_')
        scale = scale.split('.')[0]
        print(scale)
        if sourse not in symbols:
            symbols[sourse] = dict()
        #  if scale not in symbols[sourse]:
        #     symbols[sourse][scale] = dict()
        symbols[sourse][currency_pair + '_' + scale] = path + file_name

    return symbols


# if __name__ = '__main__':
crypto_douwnload_files_dir = 'data/row/crypto_data_download/'
file_names = os.listdir(DIRNAME + crypto_douwnload_files_dir)
symbols = get_symbols(file_names, crypto_douwnload_files_dir)

logger.setLevel(logging.INFO)
logger.addHandler(RotatingFileHandler("bot.log", maxBytes=10000, backupCount=2))
#logger.addHandler(StreamHandler())
logging.basicConfig(format='%(filename)s %(funcName)s %(lineno)d %(levelname)s')

if not os.path.exists(DIRNAME + 'data/preprocessing/CryptoDataDownload/'):
    os.mkdir(DIRNAME + 'data/preprocessing/CryptoDataDownload/')

index = {}
for sourse, csv_names in symbols.items():
    print(sourse)
    index[sourse] = dict()
    for sym, csv_name in csv_names.items():
        print(sym)
        logger.info(csv_name + ' processing')
        csv = DIRNAME + csv_name
        # df = pd.read_csv(csv, sep=',', encoding='utf-8', index_col='date',parse_dates=True)
        df = pd.read_csv(csv, sep=',', encoding='utf-8', parse_dates=True, index_col='date', skiprows=1)

        ## Drop rows containing NaN
        drop_rows_num = sum(df.isna().sum(axis=1))
        df = df.dropna(axis=0)
        logger.info('Drop ' + str(drop_rows_num) + ' rows.')

        way_to_new_csv = DIRNAME + 'data/preprocessing/CryptoDataDownload/{}/'.format(sourse)
        if not os.path.exists(way_to_new_csv):
            os.mkdir(way_to_new_csv)
        csv_path = way_to_new_csv + '{}.csv'.format(sym.lower())
        df.to_csv(csv_path, sep=',', encoding='utf-8', index=True, index_label='date')
        index[sym] = {'csv': csv_path}
        logger.info('Saved {} in data/preprocessing/CryptoDataDownload/'.format(sym))

with open(DIRNAME + 'data/preprocessing/index.json', 'w') as f:
    json.dump(index, f, sort_keys=True, indent=4)