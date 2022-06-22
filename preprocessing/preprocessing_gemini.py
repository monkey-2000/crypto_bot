import pandas as pd
import json
import logging
from logging.handlers import RotatingFileHandler

logger = logging.getLogger('bot_logger')

DIRNAME = 'F:/crypto/project/'
FORMAT = '%(asctime)s %(filename)s'

symbols = {'BCH': 'data/row/Gemini/Gemini_BCHUSD_1h.csv',
           'BTC': 'data/row/Gemini/Gemini_BTCUSD_1h.csv',
           'ETH': 'data/row/Gemini/Gemini_ETHUSD_1h.csv'}

if __name__ == '__main__':

    logger.setLevel(logging.INFO)
    logger.addHandler(RotatingFileHandler("bot.log", maxBytes=10000, backupCount=2))
    # logger.addHandler(StreamHandler())
    logging.basicConfig(format='%(filename)s %(funcName)s %(lineno)d %(levelname)s')

    index = {}
    for sym, csv_name in symbols.items():
        logger.info(csv_name + ' processing')
        csv = DIRNAME + csv_name
        # df = pd.read_csv(csv, sep=',', encoding='utf-8', index_col='date',parse_dates=True)
        df = pd.read_csv(csv, sep=',', encoding='utf-8', parse_dates=True, index_col='date', skiprows=1)

        ## Drop rows containing NaN
        drop_rows_num = sum(df.isna().sum(axis=1))
        df = df.dropna(axis=0)
        logger.info('Drop ' + str(drop_rows_num) + ' rows.')

        csv_path = DIRNAME + 'data/preprocessing/Gemini/{}.csv'.format(sym.lower())
        df.to_csv(csv_path, sep=',', encoding='utf-8', index=True, index_label='date')
        index[sym] = {'csv': csv_path}
        logger.info('Saved {} in data/preprocessing/Gemini/'.format(sym))

    with open(DIRNAME + 'data/preprocessing/Gemini/index.json', 'w') as f:
        json.dump(index, f, sort_keys=True, indent=4)
