#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import logging
import aiohttp
import async_timeout
import io
import asyncio
import pandas as pd
import datetime as dt
import argparse
import pandas_datareader as pdr
import matplotlib.pyplot as plt

logging.basicConfig(level=logging.INFO)

HKEX_URL_ROOT = 'https://www.hkex.com.hk/eng/cbbc/download/CBBC{:02d}.zip'

SOURCE = 'yahoo'

USD_HKD_FX = 7.72


def str_lower(string):
    return string.lower()


def parse_args():
    parser = argparse.ArgumentParser(description='plot structured product notional traded')
    parser.add_argument('symbol', type=str)
    parser.add_argument('-i', '--issuers', action='store_true',
                        help='display issuers')
    parser.add_argument('-v', '--value', type=str_lower, default='notional',
                        choices=('contracts', 'notional', 'turnover'),
                        help='value to display. default: notional')
    args = parser.parse_args()

    return args


def adjust_symbol(symbol):
    if symbol.isnumeric():
        dts_symbol = '{:05d}'.format(int(symbol))
        yahoo_symbol = '{:04d}.HK'.format(int(symbol))
    elif symbol == 'HSCE':
        dts_symbol = 'HSCEI'
        yahoo_symbol = '^HSCE'
    else:
        dts_symbol = symbol
        if symbol == 'HSCEI':
            symbol = symbol[:-1]
        yahoo_symbol = '^{}'.format(symbol)
    return dts_symbol, yahoo_symbol


async def download_coroutine(session, url, data_dict):
    logging.info('attempting download of {}'.format(url))
    file_name = url.split('/')[-1]
    with async_timeout.timeout(600):
        async with session.get(url) as response:
            bytes_written = 0
            last_log = dt.datetime.now()
            with io.BytesIO() as buffer:
                while True:
                    chunk = await response.content.read(1024)
                    if not chunk:
                        break
                    bytes_written += buffer.write(chunk)
                    if dt.datetime.now() - last_log > dt.timedelta(seconds=15):
                        logging.info('file: {} has written {} bytes into buffer'.format(
                                     file_name, bytes_written))
                        last_log = dt.datetime.now()

                df = pd.read_csv(buffer, compression='zip', encoding='utf-16', sep='\t', dtype={'CBBC Code': str})

    raw_length = len(df)
    # .loc does not play well with str.extract here for reasons unknown. works fine when tested on small dataframes
    df['CBBC Code'] = df['CBBC Code'].str.extract(r'(\d+)')

    mask = ~df['CBBC Code'].isna()
    df = df.loc[mask, :]
    updated_length = len(df)
    removed_records = raw_length - updated_length
    log_msg = '{} raw df length: {}, removed non-numeric cbbc codes, new length: {}. \033[32m {} records removed' \
              '\033[0m'
    logging.info(log_msg.format(
                 file_name, raw_length, updated_length, removed_records
                 ))

    if removed_records != 3:
        raise AssertionError('dropping non-numeric cbbc codes should remove 3 records. instead {} removed'.format(
            removed_records
        ))

    data_dict[file_name] = df

    return await response.release()


async def get_data(loop, url_list, data_dict):

    async with aiohttp.ClientSession(loop=loop) as session:
        await asyncio.gather(*(download_coroutine(session, url, data_dict) for url in url_list))


def main():
    args = parse_args()
    dts_symbol, yahoo_symbol = adjust_symbol(args.symbol.upper())
    logging.info('symbol: {} being converted to {} for dts and {} for yahoo'.format(
        args.symbol, dts_symbol, yahoo_symbol))

    file_urls = [HKEX_URL_ROOT.format(i) for i in range(1, 13)]
    data_dict = {}
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_data(loop, file_urls, data_dict))

    final_df = pd.concat(data_dict.values(), axis=0)

    # clean up column names
    column_names = [v.replace('*', '') for v in final_df.columns]
    column_names = [v.replace('^', '') for v in column_names]
    column_names = [v.replace('%', 'percent') for v in column_names]
    final_df.columns = column_names

    logging.info('available columns: {}'.format(final_df.columns))

    symbol_df = final_df.loc[final_df.loc[:, 'Underlying'] == dts_symbol, :]

    if len(symbol_df) == 0:
        logging.info('No CBBC volume for symbol {}'.format(dts_symbol))
        exit(0)

    symbol_df.loc[:, 'index units traded'] = symbol_df.apply(lambda x: x['Volume'] / x['Ent. Ratio'], axis=1)

    if args.issuers:
        index_unit_ts = symbol_df.groupby(['Trade Date', 'Issuer']).sum()
        index_unit_ts = index_unit_ts.loc[:, ['index units traded', 'Volume', 'Turnover']]
        index_unit_ts = index_unit_ts.unstack('Issuer')
    else:
        index_unit_ts = symbol_df.groupby('Trade Date').sum()
        index_unit_ts = index_unit_ts.loc[:, ['index units traded', 'Volume', 'Turnover']]

    start = index_unit_ts.index[0]
    end = index_unit_ts.index[-1]

    if args.value == 'notional':
        close_price_data = pdr.data.DataReader(yahoo_symbol, SOURCE, start, end)
        close_price_data.index.rename('Trade Date', inplace=True)
        notional_df = index_unit_ts.loc[:, 'index units traded'].multiply(
                        close_price_data.Close,
                        axis=0)

        notional_df *= (1/(USD_HKD_FX*1e6))

        notional_df.plot(title='{} CBBC Daily Notional Traded ($MM USD)'.format(args.symbol))

    elif args.value == 'contracts':
        index_unit_ts['Volume'].plot(title='{} CBBC Daily Volume'.format(args.symbol))
    elif args.value == 'turnover':
        index_unit_ts['Turnover'].apply(lambda x: x / USD_HKD_FX / 1e6).plot(
            title='{} CBBC Daily Turnover Traded ($MM USD)'.format(args.symbol)
        )

    plt.tight_layout()
    plt.show()


if __name__ == '__main__':
    main()
