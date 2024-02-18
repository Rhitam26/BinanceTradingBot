# Final strategy with ema exit value (25, 50) in sdictionary available. Left to be tested.


# !./generate_models.sh
# cd "/Users/satya2/Desktop/Algo Projects/Suruchi/bot"
# -------- marketorder table - -----
# orderId: from binance
# transaction_type: -1 short 1 Long
# price_executed: from binance
# status: 0 - placed, 1 - FILLED 3 -Closed
# (where status is 2, it means that order is cancelled--for open orders only)
# intent: 0 - Normal Order 1 - Stop loss order
# client_price: initial price
# StopLossStatus - 0 None, 1: placed
# Position: capital amount(trying)

import time
import pandas as pd
import asyncio
from datetime import datetime, timedelta
import platform
import os
import talib
from binance_order import binan
b = binan()
from background.database_alchemy import DatabaseConnection
db2 = DatabaseConnection()
from background.async_db import DatabaseConnection as Database_async
db1 = Database_async()
#addeed by satya
import sys
import random
global short_order_pending, price_info_pending, tp_order_pending, sl_order_pending

short_order_pending = {}
price_info_pending = {}
tp_order_pending = {}
sl_order_pending = {}

global data_collection
data_collection = {}
global basic_indicators
basic_indicators = {}
global ref_collection
ref_collection = {}
global df_newdata
run_job = True
interval = '5m'
global prvdata;
prvdata = pd.DataFrame(columns=['s', 'E', 'o', 'h', 'l', 'c'])
import numpy as np
from numba import jit, njit, types, vectorize
import warnings
from numba import NumbaWarning

warnings.filterwarnings("ignore", category=NumbaWarning)
global pending_orders
pending_orders = []
global capital
global stratagies
# stratagies = ['strategy11', 'strategy12']
stratagies = ['strategy1', 'strategy2', 'strategy3', 'strategy4', 'strategy5', 'strategy6', 'strategy7',
              'strategy8', 'strategy9', 'strategy10', 'strategy11', 'strategy12', 'strategy13',
              'strategy14', 'strategy15', 'strategy16', 'strategy17', 'strategy18', 'strategy19',
              'strategy20', 'strategy21', 'strategy22', 'strategy23', 'strategy24', 'strategy25',
              'strategy26', 'strategy27', 'strategy28', 'strategy29', 'strategy30', 'strategy31',
              'strategy32', 'strategy33', 'strategy34', 'strategy35', 'strategy36', 'strategy37',
              'strategy38', 'strategy39', 'strategy40', 'strategy41', 'strategy42', 'strategy43',
              'strategy44', 'strategy45', 'strategy46', 'strategy47', 'strategy48', 'strategy49',
              'strategy50', 'strategy51', 'strategy52', 'strategy53', 'strategy54', 'strategy55',
              'strategy56', 'strategy57', 'strategy58', 'strategy59', 'strategy60',
              'strategy61', 'strategy62', 'strategy63', 'strategy64', 'strategy65', 'strategy66',
              'strategy67', 'strategy68', 'strategy69', 'strategy70',
              'strategy71', 'strategy72', 'strategy73', 'strategy74', 'strategy75', 'strategy76',
              'strategy77', 'strategy78', 'strategy79', 'strategy80', 'strategy81', 'strategy82',
              'strategy83', 'strategy84', 'strategy85', 'strategy86', 'strategy87', 'strategy88',
              'strategy89', 'strategy90', 'strategy91', 'strategy92', 'strategy93', 'strategy94',
              'strategy95', 'strategy96', 'strategy97', 'strategy98', 'strategy99', 'strategy100',
              'strategy101', 'strategy102', 'strategy103', 'strategy104', 'strategy105', 'strategy106',
              'strategy107', 'strategy108', 'strategy109', 'strategy110', 'strategy111', 'strategy112',
              'strategy113', 'strategy114', 'strategy115', 'strategy116', 'strategy117']

# atr_mult = 0
# tp_percentage = 1.5
# sl_percentage = 1
# fixed_sl_percent = 2
# atr_mult_exit_value = 2
# ema_exit_value = 25, 50
strategy_dict = {
    'strategy1': [0.75, 0.5, 1.5, 2.0, 0.5, 50],  # against the trend
    'strategy2': [0.0, 0.5, 1.5, 2.0, 0.5, 50],  # against the trend
    'strategy3': [0.75, 0.5, 1.5, 2.0, 0.5, 50],  # against the trend
    'strategy4': [0.0, 0.5, 1.5, 2.0, 0.5, 50],  # against the trend
    'strategy5': [0.75, 0.5, 1.5, 2.0, 0.5, 50],  # against the trend
    'strategy6': [0.75, 0.5, 1.5, 2.0, 0.5, 50],  # against the trend
    'strategy7': [0.75, 0.5, 1.5, 2.0, 0.5, 50],  # against the trend
    'strategy8': [0.75, 0.5, 1.5, 2.0, 0.5, 50],  # against the trend
    'strategy9': [0.75, 0.5, 1.5, 2.0, 0.5, 50],  # against the trend
    'strategy10': [0.0, 0.5, 1.5, 2.0, 0.5, 50],  # against the trend
    'strategy11': [0.0, 0.5, 1.5, 2.0, 0.5, 50],  # against the trend
    'strategy12': [0.0, 0.5, 0.7, 1.5, 0.5, 50],  # with the trend
    'strategy13': [0.0, 0.5, 0.7, 1.5, 0.5, 50],  # with the trend
    'strategy14': [0.0, 0.5, 0.7, 1.5, 0.5, 50],  # with the trend
    'strategy15': [0.0, 0.5, 0.7, 1.5, 0.5, 50],  # with the trend
    'strategy16': [0.5, 0.5, 1.5, 2.0, 0.5, 50],  # against the trend
    'strategy17': [0.5, 0.5, 1.5, 2.0, 0.5, 50],  # against the trend
    'strategy18': [0.5, 0.5, 1.5, 2.0, 0.5, 50],  # against the trend
    'strategy19': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy20': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy21': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy22': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy23': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy24': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy25': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy26': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy27': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy28': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy29': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy30': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy31': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy32': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy33': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy34': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy35': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy36': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy37': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy38': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy39': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy40': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy41': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy42': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy43': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy44': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy45': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy46': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy47': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy48': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy49': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy50': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy51': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy52': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy53': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy54': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy55': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy56': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy57': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy58': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy59': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy60': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy61': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy62': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy63': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy64': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy65': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy66': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy67': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy68': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy69': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy70': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy71': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy72': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy73': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy74': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy75': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy76': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy77': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy78': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy79': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy80': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy81': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy82': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy83': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy84': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy85': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy86': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy87': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy88': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy89': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy90': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy91': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy92': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy93': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy94': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy95': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy96': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy97': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy98': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy99': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy100': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy101': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy102': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy103': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy104': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy105': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy106': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy107': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy108': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy109': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy110': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy111': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy112': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy113': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy114': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy115': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy116': [0.0, 0.5, 0.7, 2.0, 0.5, 50],  # with the trend
    'strategy117': [0.0, 0.5, 0.7, 2.0, 0.5, 50]  # with the trend
    # atr_mult, tp_percentage, sl_percentage, fixed_sl_percent, atr_mult_exit_value, ema_exit_value
}

global atr_percentage
global strategies_df

# ema3_avg_value = 0.95
# ema7_avg_value = 0.95
# lower_band_avg_value = 0.95
# upper_band_avg_value = 0.95
# Nmacd_avg_value = 0.95
# Signal_avg_value = 0.95
avg_values_dict = {
    'strategy1': [0.95, 0.95, 0.95, 0.95, 0.95, 0.95],  # against the trend
    'strategy2': [0.97, 0.97, 0.99, 0.99, 0.95, 0.97],  # against the trend
    'strategy3': [0.97, 0.97, 0.99, 0.99, 0.95, 0.97],  # against the trend
    'strategy4': [0.95, 0.95, 0.90, 0.90, 0.90, 0.95],  # against the trend
    'strategy5': [0.95, 0.95, 0.90, 0.90, 0.90, 0.90],  # against the trend
    'strategy6': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # against the trend
    'strategy7': [0.95, 0.95, 0.95, 0.95, 0.90, 0.95],  # against the trend
    'strategy8': [0.97, 0.97, 0.95, 0.95, 0.97, 0.97],  # against the trend
    'strategy9': [0.95, 0.95, 0.95, 0.95, 0.95, 0.95],  # against the trend
    'strategy10': [0.95, 0.95, 0.99, 0.99, 0.90, 0.95],  # against the trend
    'strategy11': [0.95, 0.95, 0.99, 0.99, 0.90, 0.90],  # against the trend
    'strategy12': [0.95, 0.95, 0.99, 0.99, 0.90, 0.95],  # with the trend
    'strategy13': [0.95, 0.95, 0.99, 0.99, 0.90, 0.95],  # with the trend
    'strategy14': [0.95, 0.95, 0.99, 0.99, 0.90, 0.95],  # with the trend
    'strategy15': [0.95, 0.95, 0.99, 0.99, 0.90, 0.95],  # with the trend
    'strategy16': [0.95, 0.95, 0.95, 0.95, 0.95, 0.95],  # against the trend
    'strategy17': [0.95, 0.95, 0.95, 0.95, 0.95, 0.95],  # against the trend
    'strategy18': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # against the trend
    'strategy19': [0.95, 0.95, 0.95, 0.95, 0.90, 0.95],  # with the trend
    'strategy20': [0.95, 0.95, 0.95, 0.95, 0.90, 0.90],  # with the trend
    'strategy21': [0.95, 0.95, 0.95, 0.95, 0.90, 0.95],  # with the trend
    'strategy22': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy23': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy24': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy25': [0.95, 0.95, 0.99, 0.99, 0.90, 0.95],  # with the trend
    'strategy26': [0.95, 0.95, 0.99, 0.99, 0.90, 0.95],  # with the trend
    'strategy27': [0.95, 0.95, 0.99, 0.99, 0.90, 0.95],  # with the trend
    'strategy28': [0.95, 0.95, 0.99, 0.99, 0.90, 0.95],  # with the trend
    'strategy29': [0.95, 0.95, 0.99, 0.99, 0.90, 0.95],  # with the trend
    'strategy30': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy31': [0.95, 0.95, 0.95, 0.95, 0.95, 0.95],  # with the trend
    'strategy32': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy33': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy34': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy35': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy36': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy37': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy38': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy39': [0.95, 0.95, 0.95, 0.95, 0.95, 0.95],  # with the trend
    'strategy40': [0.95, 0.95, 0.97, 0.97, 0.95, 0.95],  # with the trend
    'strategy41': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy42': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy43': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy44': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy45': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy46': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy47': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy48': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy49': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy50': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy51': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy52': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy53': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy54': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy55': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy56': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy57': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy58': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy59': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy60': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy61': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy62': [0.95, 0.95, 0.95, 0.95, 0.95, 0.95],  # with the trend
    'strategy63': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy64': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy65': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy66': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy67': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy68': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy69': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy70': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy71': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy72': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy73': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy74': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy75': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy76': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy77': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy78': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy79': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy80': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy81': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy82': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy83': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy84': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy85': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy86': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy87': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy88': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy89': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy90': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy91': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy92': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy93': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy94': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy95': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy96': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy97': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy98': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy99': [0.95, 0.95, 0.95, 0.95, 0.95, 0.95],  # with the trend
    'strategy100': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy101': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy102': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy103': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy104': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy105': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy106': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy107': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy108': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy109': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy110': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy111': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy112': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy113': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95],  # with the trend
    'strategy114': [0.95, 0.95, 0.95, 0.95, 0.90, 0.95],  # with the trend
    'strategy115': [0.95, 0.95, 0.95, 0.95, 0.95, 0.95],  # with the trend
    'strategy116': [0.95, 0.95, 0.95, 0.95, 0.95, 0.95],  # with the trend
    'strategy117': [0.95, 0.95, 0.99, 0.99, 0.95, 0.95]  # with the trend
    # atr_mult, tp_percentage, sl_percentage, fixed_sl_percent, atr_mult_exit_value
}


@njit
def pine_ema(src, length):
    # print("in pine_ema................")

    alpha = 2 / (length + 1)
    ema_values = []
    sum_ema = None
    for value in src:
        if sum_ema is None:
            sum_ema = np.mean(src[:length])
        else:
            sum_ema = alpha * value + (1 - alpha) * sum_ema
        ema_values.append(sum_ema)
    return ema_values


@jit
def bbands(close, window, window_dev):
    # print("in bbands................")

    sma = np.convolve(close, np.ones(window) / window, mode='valid')
    rolling_std = np.std([close[i:i + window] for i in range(len(close) - window + 1)], axis=1)
    upper_band = sma + (rolling_std * window_dev)
    lower_band = sma - (rolling_std * window_dev)
    return upper_band, lower_band


@njit
def calculate_wma(close: np.ndarray, window: int = 9) -> np.ndarray:
    # print("in wma................")

    weight = np.array([
        i * 2 / (window * (window + 1))
        for i in range(1, window + 1)
    ])
    wma = np.convolve(close, weight[::-1])
    wma = wma[window - 1:-window + 1]
    return wma


@jit
def get_basic_indicators_np(ohlc_np, symbol):
    print()
    print("in get_basic_indicators_np................")
    print("symbol............................", symbol)
    basic_np = ohlc_np
    print("ohlc_np...............")
    print(ohlc_np)

    Open = basic_np[:, 0]
    prev_open = np.roll(Open, 1)
    prev_open[0] = np.nan

    High = basic_np[:, 1]
    prev_high = np.roll(High, 1)
    prev_high[0] = np.nan

    Low = basic_np[:, 2]
    prev_low = np.roll(Low, 1)
    prev_low[0] = np.nan

    Close = basic_np[:, 3]
    Prev_Close = np.roll(Close, 1)
    Prev_Close[0] = np.nan

    close_price = Close[-1]
    high_price = High[-1]

    arr_ema3 = pine_ema(Close, 3)
    arr_ema7 = pine_ema(Close, 7)
    arr_ema25 = pine_ema(Close, 25)
    arr_ema50 = pine_ema(Close, 50)

    EMA3 = arr_ema3[-1] if len(arr_ema3) >= 25 else 0.0
    EMA25 = arr_ema25[-1] if len(arr_ema25) >= 25 else 0.0

    window = 14
    SMA14 = np.convolve(Close, np.ones(window) / window)[window - 1:-window + 1]

    if len(SMA14) >= 14:
        SMA = SMA14[-1]
    else:
        SMA = 0.0
    upper_band, lower_band = bbands(Close, window=20, window_dev=0.4)
    ATR14 = talib.ATR(High, Low, Close, timeperiod=14)

    l = len(Close)

    print("l- ", l)

    Conversion_Line = np.empty(l)
    Base_Line = np.empty(l)
    Leading_Span_A = np.empty(l)
    Leading_Span_B = np.empty(l)
    Upper_Cloud = np.empty(l)
    Lower_Cloud = np.empty(l)

    for i in range(l):
        low_9 = prev_low[i - 9 + 1: i + 1]
        min_9 = max_9 = min_26 = max_26 = min_52 = max_52 = np.nan
        if len(low_9) > 0:
            min_9 = low_9.min()
        high_9 = prev_high[i - 9 + 1: i + 1]
        if len(high_9) > 0:
            max_9 = high_9.max()
        conv_line = (min_9 + max_9) / 2
        Conversion_Line[i] = conv_line
        low_26 = prev_low[i - 26 + 1: i + 1]
        if len(low_26) > 0:
            min_26 = low_26.min()
        high_26 = prev_high[i - 26 + 1: i + 1]
        if len(high_26) > 0:
            max_26 = high_26.max()
        base_line = (min_26 + max_26) / 2
        Base_Line[i] = base_line
        leading_span_A = (Conversion_Line[i] + Base_Line[i]) / 2
        Leading_Span_A[i] = leading_span_A
        low_52 = prev_low[i - 52 + 1: i + 1]
        if len(low_52) > 0:
            min_52 = low_52.min()
        high_52 = prev_high[i - 52 + 1: i + 1]
        if len(high_52) > 0:
            max_52 = high_52.max()
        leading_span_B = (min_52 + max_52) / 2
        Leading_Span_B[i] = leading_span_B
        if leading_span_A > leading_span_B:
            upper_cloud = leading_span_A
            lower_cloud = leading_span_B
            Upper_Cloud[i] = upper_cloud
            Lower_Cloud[i] = lower_cloud
        else:
            upper_cloud = leading_span_B
            lower_cloud = leading_span_A
            Upper_Cloud[i] = upper_cloud
            Lower_Cloud[i] = lower_cloud
    # upper_cloud25 = lower_cloud25 = 0
    # if len(Close) >= 78:
    #     upper_cloud25 = Upper_Cloud[-25]
    #     lower_cloud25 = Lower_Cloud[-25]
    sma = 12
    lma = 21
    np_val = 50

    sh = np.array(pine_ema(Close, sma))
    lon = np.array(pine_ema(Close, lma))
    min_val = np.minimum(sh, lon)
    max_val = np.maximum(sh, lon)
    ratio = np.minimum(sh, lon) / np.maximum(sh, lon)

    Mac = (np.where(sh > lon, 2 - ratio, ratio) - 1)

    Mac_NoNA = Mac[~np.isnan(Mac)]
    window_size = 50
    l2 = len(Mac_NoNA) - window_size + 1

    print("l2- ", l2)

    min_values = np.empty(l2)
    max_values = np.empty(l2)

    for i in range(l2):
        window = Mac_NoNA[i:i + window_size]
        min_value = window.min()
        max_value = window.max()
        min_values[i] = min_value
        max_values[i] = max_value

    Mac_NoNA = Mac_NoNA[-len(min_values):]
    Mac_NoNA[-5:-1]

    l3 = len(Mac_NoNA)
    print("l3- ", l3)

    Nmacds_curr = np.empty(l3)
    for i in range(l3):
        a = Mac_NoNA[i]
        b = min_values[i]
        c = max_values[i]
        MacNorm = ((a - b) / (c - b + 0.000001) * 2) - 1
        if np_val < 2:
            Nmacd = a
        else:
            Nmacd = MacNorm
        Nmacds_curr[i] = Nmacd

    tsp = 9
    Signal = calculate_wma(close=Nmacds_curr, window=tsp)

    tail = -len(Nmacds_curr)
    basic_np = basic_np[tail:]
    arr_ema3 = arr_ema3[tail:]
    arr_ema7 = arr_ema7[tail:]
    arr_ema25 = arr_ema25[tail:]
    arr_ema50 = arr_ema50[tail:]
    SMA14 = SMA14[tail:]
    upper_band = upper_band[tail:]
    lower_band = lower_band[tail:]
    ATR14 = ATR14[tail:]
    prev_low = prev_low[tail:]
    prev_high = prev_high[tail:]
    Prev_Close = Prev_Close[tail:]
    Conversion_Line = Conversion_Line[tail:]
    Base_Line = Base_Line[tail:]
    Leading_Span_A = Leading_Span_A[tail:]
    Leading_Span_B = Leading_Span_B[tail:]
    Upper_Cloud = Upper_Cloud[tail:]
    Lower_Cloud = Lower_Cloud[tail:]
    sh = sh[tail:]
    lon = lon[tail:]
    min_val = min_val[tail:]
    max_val = max_val[tail:]
    ratio = ratio[tail:]
    Mac = Mac[tail:]

    if len(Signal) < -tail:
        num_rows_to_insert = -tail - len(Signal)
        zeros_array = np.zeros(num_rows_to_insert, dtype=Signal.dtype)
        Signal = np.concatenate((zeros_array, Signal))

    combined_array = np.column_stack((
        basic_np, prev_high, prev_low, Prev_Close, arr_ema3, arr_ema7, arr_ema25, SMA14,
        upper_band, lower_band, ATR14, Conversion_Line, Base_Line, Leading_Span_A, Leading_Span_B,
        Upper_Cloud, Lower_Cloud, sh, lon, min_val, max_val, ratio, Mac, Nmacds_curr, Signal, arr_ema50))

    return combined_array


# Define a Numba-compatible function to calculate avg
@njit
def calculate_avg(x, y):

    # Calculate avg
    if len(x) == len(y):
        avg = (np.sum(x) + np.sum(y))/len(x)
    else:
        avg = 0
    return avg


@njit(nogil=True)
def get_adv_indicators_npv2(ref_len, close, high, low, ref_np, basic_np_sub, symbol):
    # print("in get_adv_indicators_npv2................")

    ema3_ref = ref_np[:, 0]
    ema7_ref = ref_np[:, 1]
    upper_band_ref = ref_np[:, 2]
    lower_band_ref = ref_np[:, 3]
    upper_cloud_ref = ref_np[:, 4]
    lower_cloud_ref = ref_np[:, 5]
    Nmacd_ref = ref_np[:, 6]
    Signal_ref = ref_np[:, 7]

    ema3_curr = basic_np_sub[:, 0]
    ema7_curr = basic_np_sub[:, 1]
    upper_band_curr = basic_np_sub[:, 2]
    lower_band_curr = basic_np_sub[:, 3]
    upper_cloud_curr = basic_np_sub[:, 4]
    lower_cloud_curr = basic_np_sub[:, 5]
    Nmacd_curr = basic_np_sub[:, 6]
    Signal_curr = basic_np_sub[:, 7]
    l = len(Signal_curr)
    result_array = np.empty((1, 8))

    actual_ref_len = ref_len / 2

    ema3 = ema3_curr[-actual_ref_len:]  # extracting last ref_len values
    ema3_avg = calculate_avg(ema3, ema3_ref)

    ema7 = ema7_curr[-actual_ref_len:]
    ema7_avg = calculate_avg(ema7, ema7_ref)

    upper_band = upper_band_curr[-actual_ref_len:]
    upper_band_avg = calculate_avg(upper_band, upper_band_ref)

    lower_band = lower_band_curr[-actual_ref_len:]
    lower_band_avg = calculate_avg(lower_band, lower_band_ref)

    upper_cloud = upper_cloud_curr[-actual_ref_len:]
    upper_cloud_avg = calculate_avg(upper_cloud, upper_cloud_ref)

    lower_cloud = lower_cloud_curr[-actual_ref_len:]
    lower_cloud_avg = calculate_avg(lower_cloud, lower_cloud_ref)

    Nmacd = Nmacd_curr[-actual_ref_len:]
    Nmacd_avg = calculate_avg(Nmacd, Nmacd_ref)

    Signal = Signal_curr[-actual_ref_len:]
    Signal_avg = calculate_avg(Signal, Signal_ref)

    result_array[0, 0] = ema3_avg
    result_array[0, 1] = ema7_avg
    result_array[0, 2] = upper_band_avg
    result_array[0, 3] = lower_band_avg
    result_array[0, 4] = upper_cloud_avg
    result_array[0, 5] = lower_cloud_avg
    result_array[0, 6] = float(Nmacd_avg)
    result_array[0, 7] = Signal_avg
    return result_array[-1]
# added by Satya
async def generate_unique_key():
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f') 
    random_number = random.randint(1, 1000000)
    unique_key = f"{timestamp}_{random_number}"
    return unique_key
async def place_sl_order(qty, symbol, strategy_name, price, strategy_logic_value,stoploss_value, take_profit_level, fixed_sl, decimal_places, main_order_id):
    rounded_fixed_sl = round(fixed_sl, decimal_places)
    status_sl_order, binanceid_sl = b.place_sl_order(symbol, "BUY", qty, rounded_fixed_sl)
    if status_sl_order == True:
        print(f"{status_sl_order}, {binanceid_sl=}")
        id_sl = await db1.insert_marketorder(orderId=binanceid_sl, transaction_type=1,
                                                price_executed=0, status=0, intent=1,
                                                client_price=rounded_fixed_sl, symbol=symbol,
                                                StopLossStatus=0, position=capital, qty=qty,
                                                remarks='sl_open_order', strategy=strategy_name,
                                                stop_loss=0, take_profit=0,
                                                fixed_sl=0, main_orderid=main_order_id)
        return status_sl_order # True
    else:
        print("SL Open order not placed.")
        return status_sl_order # False
async def place_tp_order( qty, symbol, rounded_take_profit_level, strategy_name, price, strategy_logic_value,stoploss_value, take_profit_level, fixed_sl, decimal_places, main_order_id):
    status_tp_order, binance_id_tp = b.place_tp_order(symbol, "BUY", qty, rounded_take_profit_level)
    print(f"{status_tp_order}, {binance_id_tp=}")
    if status_tp_order == True:
        id_tp = await db1.insert_marketorder(orderId=binance_id_tp, transaction_type=1,
                                            price_executed=0, status=0, intent=1,
                                            client_price=rounded_take_profit_level, symbol=symbol,
                                            StopLossStatus=0, position=capital, qty=qty,
                                            remarks='tp_open_order', strategy=strategy_name,
                                            stop_loss=0, take_profit=0,
                                            fixed_sl=0, main_orderid=main_order_id)
        if (strategy_logic_value == 'short_against_the_trend'):
            global sl_order_pending
            sl_order_info = {}
            sl_order_info['symbol'] = symbol
            sl_order_info['qty'] = qty
            sl_order_info['price'] = price  
            sl_order_info['strategy_name'] = strategy_name  
            sl_order_info['strategy_logic_value'] = strategy_logic_value  
            sl_order_info['stoploss_value'] = stoploss_value  
            sl_order_info['take_profit_level'] = take_profit_level  
            sl_order_info['fixed_sl'] = fixed_sl
            sl_order_info['main_order_id'] = main_order_id
            sl_order_info['decimal_places'] = decimal_places  
            sl_order_info['timestamp'] = datetime.now()
            unique_key = await generate_unique_key()
            sl_order_pending[unique_key] = sl_order_info
        return status_tp_order # True
    else:
        return status_tp_order #False
async def place_short_order(qty, symbol, price, strategy_name, strategy_logic_value,
                            stoploss_value, take_profit_level, fixed_sl, repeat=0):
    status, binance_orderid = b.place_order(symbol, 'SELL', qty)
    global dfsymbols
    if status == True:
        # deactivate symbol in local dataframe
        dfsymbols.loc[dfsymbols['s'] == symbol, 'active'] = 0
        # deactivate symbol in database and set iter to 6
        await db1.update_symbol_trading_active(symbol, 0, strategy_name, 6)
        # status true means order placed with binance. Now save in database
        print(f'Placed Short Order {symbol}')
        sys.stdout.flush()
        id = await db1.insert_marketorder(orderId=binance_orderid, transaction_type=-1,
                                            price_executed=0, status=0, intent=0,
                                            client_price=price, symbol=symbol, StopLossStatus=0, position=capital,
                                            qty=qty, remarks='Normal short order', strategy=strategy_name,
                                            stop_loss=0, take_profit=0,
                                            fixed_sl=0, main_orderid=0)
        global price_info_pending
        price_info = {}
        price_info['symbol'] = symbol
        price_info['binance_orderid'] = binance_orderid
        price_info['qty'] = qty
        price_info['price'] = price  
        price_info['strategy_name'] = strategy_name  
        price_info['strategy_logic_value'] = strategy_logic_value  
        price_info['stoploss_value'] = stoploss_value  
        price_info['take_profit_level'] = take_profit_level  
        price_info['fixed_sl'] = fixed_sl  
        price_info['timestamp'] = datetime.now()
        price_info['id'] = id
        unique_key = await generate_unique_key()
        price_info_pending[unique_key] = price_info
        return True
    else:
        if repeat == 0:
            global short_order_pending
            order_info = {}
            order_info['symbol'] = symbol
            order_info['qty'] = qty
            order_info['price'] = price  
            order_info['strategy_name'] = strategy_name  
            order_info['strategy_logic_value'] = strategy_logic_value  
            order_info['stoploss_value'] = stoploss_value  
            order_info['take_profit_level'] = take_profit_level  
            order_info['fixed_sl'] = fixed_sl  
            order_info['timestamp'] = datetime.now()
            unique_key = await generate_unique_key()
            short_order_pending[unique_key] = order_info
        return False
async def get_price_info_short_order(symbol, price, strategy_name, strategy_logic_value, stoploss_value, take_profit_level, fixed_sl, binance_orderid, qty, id):
    status, priceExecuted, response_status = b.get_order_details(binance_orderid, symbol)
    print("in get_price_info_short_order: ", status)
    sys.stdout.flush()
    if status == 1:
        print("priceExecuted: ", priceExecuted)
        sys.stdout.flush()
        decimal_places = b.get_pricePrecision(symbol)
        # decimal_places = await get_decimal_places(priceExecuted)
        #print(f"Number of decimal places: {decimal_places}")
        rounded_take_profit_level = round(take_profit_level, decimal_places)
        # Calculate Stop loss and take profit values here
        # Update market order table with price executed, status =1 and rounded_take_profit_level
        priceExecuted = round(priceExecuted, 8)
        await db1.run_query(f"UPDATE marketorders SET price_executed={priceExecuted}, status=1, take_profit={rounded_take_profit_level} WHERE id={id}")
        # Save take profit order info in collection
        #tp_order_status = await place_tp_order(qty, symbol, rounded_take_profit_level, strategy_name, price, strategy_logic_value,stoploss_value, take_profit_level, fixed_sl, decimal_places, id)
        global tp_order_pending
        tp_order_info = {}
        tp_order_info['symbol'] = symbol
        tp_order_info['qty'] = qty
        tp_order_info['price'] = price  
        tp_order_info['strategy_name'] = strategy_name  
        tp_order_info['strategy_logic_value'] = strategy_logic_value  
        tp_order_info['stoploss_value'] = stoploss_value
        tp_order_info['rounded_take_profit_level'] = rounded_take_profit_level  
        tp_order_info['take_profit_level'] = take_profit_level  
        tp_order_info['fixed_sl'] = fixed_sl
        tp_order_info['main_order_id'] = id
        tp_order_info['decimal_places'] = decimal_places  
        tp_order_info['timestamp'] = datetime.now()
        unique_key = await generate_unique_key()
        tp_order_pending[unique_key] = tp_order_info
        return True
    else:
        return False

# async def get_decimal_places(number):
#     number_str = str(number).rstrip('0')
#     decimal_places = len(number_str.split('.')[1]) if '.' in number_str else 0
#     result = float(number_str)
#     return decimal_places

async def process_short_order_pending():
    print('in process_short_order_pending')
    sys.stdout.flush()
    global short_order_pending
    short_keys_to_delete = []
    for key in short_order_pending.keys():
        temporary_info = short_order_pending[key]
        result = await place_short_order(temporary_info['qty'], temporary_info['symbol'], temporary_info['price'], temporary_info['strategy_name'], temporary_info['strategy_logic_value'], temporary_info['stoploss_value'], temporary_info['take_profit_level'], temporary_info['fixed_sl'], 1)
        if result == True:
            short_keys_to_delete.append(key)
    for key in short_keys_to_delete:
        del short_order_pending[key]
        print('deleted from short_order_pending')
        sys.stdout.flush()

async def process_price_info_pending():
    global price_info_pending
    if len(price_info_pending) > 0:
        print('in process_price_info_pending')
    sys.stdout.flush()
    price_keys_to_delete = []
    for key in price_info_pending.keys():
        temporary_info = price_info_pending[key]      
        result = await get_price_info_short_order(temporary_info['symbol'], temporary_info['price'], temporary_info['strategy_name'], temporary_info['strategy_logic_value'], temporary_info['stoploss_value'], temporary_info['take_profit_level'], temporary_info['fixed_sl'], temporary_info['binance_orderid'], temporary_info['qty'], temporary_info['id'])          
        if result == True:
            price_keys_to_delete.append(key)
    for key in price_keys_to_delete:
        del price_info_pending[key]
        print('deleted from price_info_pending')
        sys.stdout.flush()
async def process_tp_order_pending():
    print('in process_tp_order_pending')
    sys.stdout.flush()
    global tp_order_pending
    tp_keys_to_delete = []
    for key in tp_order_pending.keys():
        temporary_info = tp_order_pending[key]
        result = await place_tp_order(temporary_info['qty'], temporary_info['symbol'], temporary_info['rounded_take_profit_level'], temporary_info['strategy_name'], temporary_info['price'], temporary_info['strategy_logic_value'], temporary_info['stoploss_value'], temporary_info['take_profit_level'], temporary_info['fixed_sl'], temporary_info['decimal_places'], temporary_info['main_order_id'])
        if result == True:
            tp_keys_to_delete.append(key)
    for key in tp_keys_to_delete:
        del tp_order_pending[key]
        print('deleted from tp_order_pending')
        sys.stdout.flush()
async def process_sl_order_pending():
    print('in process_sl_order_pending')
    sys.stdout.flush()
    global sl_order_pending
    sl_keys_to_delete = []
    for key in sl_order_pending.keys():
        temporary_info = sl_order_pending[key]
        result = await place_sl_order(temporary_info['qty'], temporary_info['symbol'], temporary_info['strategy_name'], temporary_info['price'], temporary_info['strategy_logic_value'], temporary_info['stoploss_value'], temporary_info['take_profit_level'], temporary_info['fixed_sl'], temporary_info['decimal_places'], temporary_info['main_order_id'])
        if result == True:
            sl_keys_to_delete.append(key)
    for key in sl_keys_to_delete:
        del sl_order_pending[key]
        print('deleted from sl_order_pending')
        sys.stdout.flush()

# -------- end of added by Satya

async def place_buy_order(qty, symbol, price, strategy_name, strategy_logic_value,
                          id, order_id_tp, order_id_sl, id_tp, id_sl, stoploss_value):
    # print("in place_buy_order................")

    status, orderid = b.place_order(symbol, 'BUY', qty)
    status_order, priceExecuted, response_status = b.get_order_details(orderid, symbol)
    # status = True
    print("status- ", status)
    print("orderid- ", orderid)
    global dfsymbols, capital
    if status == True:
        await db1.insert_marketorder(orderId=orderid, transaction_type=1,
                                     price_executed=priceExecuted, status=3, intent=1,
                                     client_price=price, symbol=symbol, StopLossStatus=0,
                                     position=capital, qty=qty, remarks='Exit short order in sl',
                                     strategy=strategy_name, stop_loss=stoploss_value, take_profit=0,
                                     fixed_sl=0, main_orderid=id)
        print("Inserted buy order successfully............")
        b.cancel_open_orders(order_id_tp, symbol)
        print("Cancelled tp open order successfully............")
        # code changed here
        await db1.update_symbol_trading_active(symbol, 1, strategy_name, 6)
        dfsymbols.loc[dfsymbols['s'] == symbol, 'active'] = 1
        await db1.update_marketorder_status(id, 3)
        await db1.update_marketorder_status(id_tp, 2)  # status = 2 for cancelled
        print("Updated market order table successfully............")
        if strategy_logic_value == "short_against_the_trend":
            b.cancel_open_orders(order_id_sl, symbol)
            print("Cancelled sl open order successfully............")
            await db1.update_marketorder_status(id_sl, 2)  # status = 2 for cancelled


# Loads initial tick from 5 min table
async def get_old_data():
    start_time_old_data = datetime.now()
    print(f"getting old data............")
    print(f"start_time_old_data: {start_time_old_data}")
    prvdata = await db1.get_old_tick_data()
    prvdata = prvdata[::-1]
    prvdata = prvdata.sort_values(by='E')
    prvdata['E'] = pd.to_datetime(prvdata['E'], unit='ms')
    global data_collection
    for s_value, group_df in prvdata.groupby('s'):
        data_collection[s_value] = group_df
    end_time_old_data = datetime.now()
    time_diff = end_time_old_data - start_time_old_data
    print(f"end_time_old_data: {end_time_old_data}")
    print(f"Old data done in {time_diff} time..")
    print('Old data done')


# fetch all ref values from db
async def get_ref_values():
    # print("in get_ref_values................")

    loop = asyncio.get_event_loop()
    global ref_values
    ref_values = await db1.get_ref_values_all()
    ref_values = ref_values.sort_values(by='open_time')
    global ref_collection
    for s_value, group_df in ref_values.groupby('strategy'):
        ref_collection[s_value] = group_df


async def cal_basic_indicators(symbol):
    # print("in cal_basic_indicators................")
    # loop = asyncio.get_event_loop()
    global data_collection, newdata, basic_indicators   # data_collection being called once from get_old_data
    if symbol not in data_collection:
        print(f'data not found for {symbol} in data_collection')
        return
    df_data = data_collection[symbol]
    if df_data is None or len(df_data) == 0:
        print(f'data not found for {symbol}')
        return
    df_latest = df_newdata.loc[df_newdata['s'] == symbol]
    if df_latest is None or len(df_latest) == 0:
        print(f'new data not found for {symbol}')
        return

    # new timestamp added
    date_time_val = pd.to_datetime(df_data.iloc[-1]['E'])

    old_data_last_min = date_time_val.minute
    date_time_val_new = pd.to_datetime(df_latest.iloc[-1]['E'])
    new_data_last_min = date_time_val_new.minute

    if old_data_last_min == new_data_last_min:
        print('old_data_last_timestamp', old_data_last_min)
        print('new_data_last_timestamp', new_data_last_min)
        print('Duplicate Data Encountered Ignore', symbol)
    else:
        df_data = pd.concat([df_data, df_latest], axis=0)

    # data_collection[symbol] = df_data   # updating data_collection by adding the new ohlc row in it.
    ohlc_np = df_data[['o', 'h', 'l', 'c']].values.astype(float)
    column_length = ohlc_np.shape[0]
    if column_length < 200:
        print(f"Insufficient data found for {symbol}")
        return
    np_result = get_basic_indicators_np(ohlc_np, symbol)
    basic_indicators[symbol] = np_result
    # print(f"basic indicator calculated for {symbol}.")


# process open orders for stoploss
async def process_orders():
    print("in process_orders................")

    df_open_orders = await db1.select_marketorders(1, -1)  # Getting main short order details
    df_open_orders = df_open_orders.sort_values(by='id', ascending=True)
    print(len(df_open_orders), 'orders to process.')
    df_open_orders_connected = await db1.select_marketorders(0, 1)  # Getting open buy order details
    df_open_orders_connected = df_open_orders_connected.sort_values(by='id', ascending=True)
    print(len(df_open_orders_connected), 'open orders to process.')

    global dfsymbols;
    for index, row in df_open_orders.iterrows():  # main short order (Assuming =-1)
        id = row['id']
        orderId = row['orderId']
        print("id- ", id)
        print("orderId- ", orderId)
        symbol = row['symbol']
        qty = row['qty']
        price_executed = row['price_executed']
        strategy_name = row['strategy']
        stoploss_value = row['stop_loss']
        take_profit_level = row['take_profit']
        fixed_sl = row['fixed_sl']

        connected_order_df = df_open_orders_connected[df_open_orders_connected['main_orderid'] == id]
        print("connected_order_df")
        print(connected_order_df)

        for index, row in connected_order_df.iterrows():  # short open orders (either 1 or 2)
            id_open_orders = row['id']
            orderId_open_orders = row['orderId']
            main_id = row["main_orderid"]
            remarks = row['remarks']
            print("id_open_orders- ", id_open_orders)
            print("orderId_open_orders- ", orderId_open_orders)

            if remarks == 'tp_open_order':  # and (len(connected_order_df) == 2):
                id_tp = id_open_orders
                order_id_tp = orderId_open_orders
                print("order_id_tp- ", order_id_tp)
            elif row['remarks'] == 'sl_open_order':
                id_sl = id_open_orders
                order_id_sl = orderId_open_orders
                print("order_id_sl- ", order_id_sl)

        # Check if position remains, if not cancel orders, if remains check for stoploss and then cancel
        print("checking status and response for open orders.........")
        status_order_tp, priceExecuted_tp, response_open_orders_tp = b.get_order_details(order_id_tp, symbol)
        print("response_status for tp open orders from order details- ", response_open_orders_tp)
        print("priceExecuted_tp for tp open orders from order details- ", priceExecuted_tp)
        print()

        # Access the values for a specific strategy
        atr_mult, tp_percentage, sl_percentage, fixed_sl_percent, atr_mult_exit_value, ema_exit_value = strategy_dict.get(
            strategy_name, [0
                , 0, 0, 0, 0, 0])

        global basic_indicators, newdata, strategies_df, atr_percentage
        strategy_row = strategies_df.loc[strategies_df['strategy_name'] == strategy_name]
        strategy_logic_value = strategy_row['strategy_logic'].iloc[0]

        # df_newdata = await db1.get_filtered_all_tick_last_data(timeframe_val)
        df_price_filtered = df_newdata[df_newdata.s == symbol]

        # if symbol not in basic_indicators:
        #     print(f'process_signal data not found for {symbol} in data_collection')
        #     continue
        await cal_basic_indicators(symbol)  # cannot comment out this line
        bas_ind = basic_indicators[symbol]

        basic_np = basic_indicators[symbol]
        if basic_np is None or len(basic_np) == 0:
            print(f'process_signal data not found for {symbol}')
            continue

        if len(df_price_filtered) > 0:
            close = df_price_filtered.c.iloc[0]
            high = df_price_filtered.h.iloc[0]
            low = df_price_filtered.l.iloc[0]
            close = float(close)
            high = float(high)
            low = float(low)
            # print()
            # print("high (df_price_filtered)- ", high)
            # print("low (df_price_filtered)- ", low)
            # print("close (df_price_filtered)- ", close)
            # print()
            # await cal_basic_indicators(symbol)
            # print("after cal basic ind.................")
            # bas_ind = basic_indicators[symbol]
            # bas_ind_df = pd.DataFrame(bas_ind)
            # print("bas_ind_df..............")
            # print(bas_ind_df)
            # print()
            # print("open- ", bas_ind[-1, 0])
            # print("high- ", bas_ind[-1, 1])
            # print("low- ", bas_ind[-1, 2])
            # print("close- ", bas_ind[-1, 3])
            atr = bas_ind[-1, 13]
            ema25 = bas_ind[-1, 9]
            ema50 = bas_ind[-1, 28]
            if ema_exit_value == 25:
                ema_exit = ema25
            elif ema_exit_value == 50:
                ema_exit = ema50
            print("atr (-1)- ", atr)
            print("ema25 (-1)- ", ema25)
            print("ema50 (-1)- ", ema50)
            print()
            print(f"symbol: {symbol}| strategy: {strategy_name}| ema25: {ema25}| "
                  f"ema_exit (50ema): {ema_exit}| atr: {atr}| close: {close}| low: {low}| "
                  f"atr_mult_exit_value: {atr_mult_exit_value}| stoploss_value: {stoploss_value}| "
                  f"take_profit_level: {take_profit_level}")

            if strategy_logic_value == "short_with_the_trend":
                stoploss_value = close + atr + ((close * sl_percentage) / 100)
                stoploss_value = round(stoploss_value, 8)
                print("stoploss_value- ", stoploss_value)
                await db1.update_order_table(id, stoploss_value, take_profit_level)
                print("updated stoploss in order table..........")
                if response_open_orders_tp == "FILLED":
                    print("orderId_open_orders- ", order_id_tp)
                    await db1.update_symbol_trading_active(symbol, 1, strategy_name, 6)
                    dfsymbols.loc[dfsymbols['s'] == symbol, 'active'] = 1
                    await db1.update_marketorder_status(id, 3)
                    await db1.run_query(
                        f"UPDATE marketorders SET price_executed='{priceExecuted_tp}', status=1 WHERE id={id_tp}")
                elif close > stoploss_value:
                    print("after checking stoploss condition......................")
                    print("close- ", close)
                    order_id_sl = 0   # No sl open orders for short with the trend
                    id_sl = 0         # No sl open orders for short with the trend
                    print("stoploss_value- ", stoploss_value)
                    await place_buy_order(qty, symbol, close, strategy_name,
                                          strategy_logic_value, id, order_id_tp, order_id_sl,
                                          id_tp, id_sl, stoploss_value)
                    print(f"Exit SL {symbol}")
            elif strategy_logic_value == "short_against_the_trend":
                status_order_sl, priceExecuted_sl, response_open_orders_sl = b.get_order_details(order_id_sl, symbol)
                if response_open_orders_tp == "FILLED":
                    print("response_open_orders_tp- ", response_open_orders_tp)
                    b.cancel_open_orders(order_id_sl, symbol)
                    dfsymbols.loc[dfsymbols['s'] == symbol, 'active'] = 1
                    await db1.update_marketorder_status(id, 3)
                    # await db1.update_marketorder_status(id_tp, 3)  # status 3 for closed
                    await db1.update_marketorder_status(id_sl, 2)  # status = 2 for cancelled
                    await db1.update_symbol_trading_active(symbol, 1, strategy_name, 6)
                    await db1.run_query(
                        f"UPDATE marketorders SET price_executed='{priceExecuted_tp}', status=1 WHERE id={id_tp}")

                elif response_open_orders_sl == "FILLED":
                    print("response_open_orders_sl- ", response_open_orders_sl)
                    b.cancel_open_orders(order_id_tp, symbol)  # status = 2 for cancelled
                    await db1.update_marketorder_status(id, 3)
                    await db1.update_marketorder_status(id_tp, 2)  # status = 2 for cancelled
                    # await db1.update_marketorder_status(id_sl, 3)  # status 3 for closed
                    await db1.update_symbol_trading_active(symbol, 1, strategy_name, 6)
                    await db1.run_query(
                        f"UPDATE marketorders SET price_executed='{priceExecuted_sl}', status=1 WHERE id={id_sl}")

                elif close > stoploss_value:
                    print("after checking stoploss condition......................")
                    print("close- ", close)
                    print("stoploss_value- ", stoploss_value)
                    await place_buy_order(qty, symbol, close, strategy_name,
                                          strategy_logic_value, id, order_id_tp, order_id_sl,
                                          id_tp, id_sl, stoploss_value)
                    print(f"Exit SL {symbol}")

async def my_async_function():
    print("in my_async_function................")
    loop = asyncio.get_event_loop()
    await db1.create_pool(loop=loop)
    global df_newdata, outlier_symbols, basic_indicators
    # New timestamp added
    outliers = pd.DataFrame()
    outlier_symbols = []
    df_newdata = await db1.get_all_tick_last_data()
    df_newdata['E'] = pd.to_datetime(df_newdata['E'])
    # New timestamp added
    mins_array = df_newdata['E'].dt.minute.unique().tolist()
    mins_array = sorted(mins_array)
    if len(mins_array) > 1:
        cur_min = mins_array[1]
        outliers = df_newdata[df_newdata['E'].dt.minute < cur_min]
        df_newdata = df_newdata[df_newdata['E'].dt.minute == cur_min]
        outlier_symbols = outliers.s.unique()
    print('outliers', outliers)
    #print('correct data', df_newdata)

    await db1.decrease_trading_symbol_iter()
    await process_orders()
    global dfsymbols
    dfsymbols = await db1.get_symbols_trading()
    symbols = dfsymbols.s.to_list()
    basic_indicators = {}
    for symbol in symbols:
        # new timestamp added
        if symbol not in outlier_symbols:
            await cal_basic_indicators(symbol)

    global stratagies
    for strategy_name in stratagies:
        print('*************', strategy_name, '**************')
        a_symbols = dfsymbols[dfsymbols.active == 1]
        for index, row in a_symbols.iterrows():
            symbol = row['s']
            iter_value = int(row['iter'])
            strategy_name_value = row['strategy_name']
            # new timestamp added
            if strategy_name_value != strategy_name and symbol not in outlier_symbols:
                await process_signal(symbol, strategy_name)
            elif iter_value == 0 and symbol not in outlier_symbols:
                await process_signal(symbol, strategy_name)

    df_newdata = await db1.get_all_tick_last_data()   # changed Query to convert E format at mysql
    df_newdata['E'] = pd.to_datetime(df_newdata['E'])
    for symbol in outlier_symbols:
        print('reprocessing:', symbol)
        await cal_basic_indicators(symbol)
    await asyncio.sleep(1)



# delegate job to async
def job():
    # print("in job................")
    if platform.system() == "Windows":
        os.system('cls')
    else:
        os.system('clear')
    loop.run_until_complete(my_async_function())


# checks signal and takes entry trades
async def process_signal(symbol, strategy_name):
    await process_pending_items()

    # print("in process_signal................")
    # print(f"strategy_name: {strategy_name}|| symbol: {symbol}")

    global basic_indicators, newdata, ref_collection
    if symbol not in basic_indicators:
        print(f'process_signal data not found for {symbol} in data_collection')
        return -1
    basic_np = basic_indicators[symbol]
    if basic_np is None or len(basic_np) == 0:
        print(f'process_signal data not found for {symbol}')
        return -1
    df_strategy = ref_collection[strategy_name]
    if df_strategy is None or len(df_strategy) == 0:
        print(f'process_signal reference data not found for {strategy_name}')
        return -1

    ref_len = float(len(df_strategy) * 2)
    ref_np = df_strategy[
        ['ema_3_ref', 'ema_7_ref', 'upper_band_ref', 'lower_band_ref', 'Upper_Cloud_ref', 'Lower_Cloud_ref',
         'Nmacd_ref', 'Signal_ref']].values.astype(float)

    open = basic_np[-1, 0]
    high = basic_np[-1, 1]
    low = basic_np[-1, 2]
    close = basic_np[-1, 3]
    sma = basic_np[-1, 10]
    atr = basic_np[-1, 13]
    ema25 = basic_np[-1, 9]
    ema50 = basic_np[-1, 28]
    # if symbol == "AVAXUSDT":
    #     print("open1- ", basic_np[-1, 0])
    #     print("high1- ", basic_np[-1, 1])
    #     print("low1- ", basic_np[-1, 2])
    #     print("close1- ", basic_np[-1, 3])
    #     print("atr-1- ", basic_np[-1, 13])
    #     print("sma-1- ", basic_np[-1, 10])
    #     print("ema25-1- ", basic_np[-1, 9])
    #     print("ema50-1- ", basic_np[-1, 28])
    #     print()

    # Access the values for a specific strategy
    atr_mult, tp_percentage, sl_percentage, fixed_sl_percent, atr_mult_exit_value, ema_exit_value = strategy_dict.get(strategy_name,
                                                                                                                      [0, 0, 0, 0,
                                                                                                                       0, 0])

    global atr_percentage
    atr_percentage = (atr / sma) * 100
    percent_diff_ema = ((close - ema25) * 100) / close

    if ema_exit_value == 25:
        ema_exit = ema25
    elif ema_exit_value == 50:
        ema_exit = ema50

    ema3 = basic_np[-1, 7]
    ema7 = basic_np[-1, 8]
    upper_band = basic_np[-1, 11]
    lower_band = basic_np[-1, 12]
    Upper_Cloud = basic_np[-1, 18]
    Lower_Cloud = basic_np[-1, 19]
    Lower_Cloud_25 = basic_np[-25, 19]
    Upper_Cloud_25 = basic_np[-25, 18]
    Nmacd_curr = basic_np[-1, 26]
    Signal = basic_np[-1, 27]

    # if symbol == "AVAXUSDT":
    #     print("upper_band(1)- ", basic_np[-1, 11])
    #     print("lower_band(1)- ", basic_np[-1, 12])
    #     print("Upper_Cloud(1)- ", basic_np[-1, 18])
    #     print("Lower_Cloud(1)- ", basic_np[-1, 19])
    #     print("Nmacd_curr(1)- ", basic_np[-1, 26])
    #     print("Signal(1)- ", basic_np[-1, 27])
    #     print("Lower_Cloud_25- ", Lower_Cloud_25)
    #     print("Upper_Cloud_25- ", Upper_Cloud_25)

    # basic_np = basic_df[['EMA3', 'EMA7', 'upper_band', 'lower_band','Upper_Cloud', 'Lower_Cloud','Nmacd_curr', 'Signal']].values.astype(float)
    selected_columns = [7, 8, 11, 12, 18, 19, 26, 27]
    basic_np_sub = basic_np[:, selected_columns]
    # basic_np_sub = basic_np_sub_1[:-1, :]

    # if symbol == "AVAXUSDT":
    #     print("basic_np_sub..........")
    #     print(pd.DataFrame(basic_np_sub))
    #     print("upper cloud..........")
    #     print(pd.DataFrame(basic_np[:, 18]))
    #     print("lower cloud..........")
    #     print(pd.DataFrame(basic_np[:, 19]))

    ema3_avg, ema7_avg, upper_band_avg, lower_band_avg, upper_cloud_avg, lower_cloud_avg, Nmacd_avg, Signal_avg = get_adv_indicators_npv2(
        ref_len, close, high, low, ref_np, basic_np_sub, symbol)

    # if symbol == "AVAXUSDT":
    #     print(f"ema3_avg- {ema3_avg}, ema7_avg- {ema7_avg}, upper_band_avg- {upper_band_avg}, "
    #           f"lower_band_avg- {lower_band_avg}, upper_cloud_avg- {upper_cloud_avg}, "
    #           f"lower_cloud_avg- {lower_cloud_avg}, Nmacd_avg- {Nmacd_avg}, "
    #           f"Signal_avg- {Signal_avg}")

    # Access the values for a specific strategy
    ema3_avg_value, ema7_avg_value, lower_band_avg_value, upper_band_avg_value, Nmacd_avg_value, Signal_avg_value = avg_values_dict.get(
        strategy_name, [0, 0, 0, 0, 0, 0])

    # Added strategies_df
    global strategies_df
    strategy_row = strategies_df.loc[strategies_df['strategy_name'] == strategy_name]
    strategy_logic_value = strategy_row['strategy_logic'].iloc[0]

    EP = close
    print("symbol- ", symbol)

    # short_condition = False
    # # Forceful entry
    # if strategy_name == 'strategy11' and symbol == "AVAXUSDT":
    #     short_condition = True
    # elif strategy_name == 'strategy12' and symbol == "1000LUNCUSDT":
    #     short_condition = True
    # else:
    #     short_condition = False

    # Assuming short_with_the_trend cond as default
    short_condition = (ema3_avg < ema7_avg and lower_band_avg < upper_band_avg and Nmacd_avg < Signal_avg)
    stoploss_value = close + atr + ((close * sl_percentage) / 100)
    take_profit_level = EP - ((EP * tp_percentage) / 100)
    fixed_sl = 0.0
    if (strategy_logic_value == 'short_against_the_trend'):
        short_condition = (ema3_avg < ema7_avg and lower_band_avg > upper_band_avg)
        stoploss_value = high + atr + ((high * sl_percentage) / 100)
        take_profit_level = EP - ((EP * tp_percentage) / 100)
        fixed_sl = EP + ((EP * fixed_sl_percent) / 100)

    entry_condition = short_condition
    if (entry_condition):
        print(f'Place {strategy_logic_value} order in {symbol} for {strategy_name}')
        print(f"EP: {EP} | atr_percentage: {atr_percentage} | atr_mult: {atr_mult} | tp_percentage: {tp_percentage}")
        print("strategy_name- ", strategy_name)
        print("strategy_logic_value- ", strategy_logic_value)
        print("entry_condition- ", entry_condition)
        print("open- ", basic_np[-1, 0])
        print("high- ", basic_np[-1, 1])
        print("low- ", basic_np[-1, 2])
        print("close- ", basic_np[-1, 3])
        print("atr- ", atr)
        print("sma- ", sma)
        print("ema25- ", ema25)
        print("ema50- ", ema50)
        print("upper_band- ", upper_band)
        print("lower_band- ", lower_band)
        print("Upper_Cloud- ", Upper_Cloud)
        print("Lower_Cloud- ", Lower_Cloud)
        print("Nmacd_curr- ", Nmacd_curr)
        print("Signal- ", Signal)
        # print("lower_band- ", lower_band)
        # print("upper_band- ", upper_band)
        print("Lower_Cloud_25- ", Lower_Cloud_25)
        print("Upper_Cloud_25- ", Upper_Cloud_25)

        take_profit_level = take_profit_level
        stoploss_value = stoploss_value
        fixed_sl = fixed_sl
        take_profit_level = round(take_profit_level, 8)
        stoploss_value = round(stoploss_value, 8)
        fixed_sl = round(fixed_sl, 8)
        print("take_profit_level after rounding- ", take_profit_level)
        print("stoploss_value after rounding- ", stoploss_value)
        print("fixed_sl after rounding- ", fixed_sl)
        global capital
        capital = 10
        price = close
        qty = b.get_qty(symbol, capital, price)

        await db1.insert_trades_indicator_values(strategy=strategy_name, symbol=symbol,
                                                 open_time=datetime.datetime.now(), open=open, high=high,
                                                 low=low, close=close, close_time=datetime.datetime.now(),
                                                 atr=atr, sma=sma, ema_3=ema3, ema_7=ema7,
                                                 upper_band=upper_band, lower_band=lower_band,
                                                 upper_cloud=Upper_Cloud_25,
                                                 lower_cloud=Lower_Cloud_25,
                                                 Nmacd=Nmacd_curr, Signal=Signal,
                                                 ema_3_avg=ema3_avg, ema_7_avg=ema7_avg,
                                                 upper_band_avg=upper_band_avg,
                                                 lower_band_avg=lower_band_avg, Nmacd_avg=Nmacd_avg,
                                                 Signal_avg=Signal_avg,
                                                 Upper_Cloud_avg=upper_cloud_avg,
                                                 Lower_Cloud_avg=lower_cloud_avg)
        # modified by Satya

        # id, priceExecuted = await place_short_order(qty, symbol, close, strategy_name, strategy_logic_value, stoploss_value,
        #                              take_profit_level, fixed_sl)
        await place_short_order(qty, symbol, close, strategy_name, strategy_logic_value, stoploss_value, take_profit_level, fixed_sl)
        return 1
    else:
        # print(f'* * processed {symbol} * *')
        return 0

async def process_pending_items():
    await process_price_info_pending()
    await process_tp_order_pending()
    await process_sl_order_pending()
    print('Number of Short Orders Pending:', len(short_order_pending), '\n')
    sys.stdout.flush()
    await process_short_order_pending()


async def initialize():
    # print("in initialize................")
    loop = asyncio.get_event_loop()
    await db1.create_pool(loop=loop)
    await get_old_data()
    await get_ref_values()
    # Added strategies_df
    global strategies_df
    strategies_df = await db1.get_strategies()
    await db1.close_pool()


loop = asyncio.get_event_loop()
loop.run_until_complete(initialize())

while True:
    current_minute = datetime.now().time().minute
    current_second = datetime.now().time().second
    if run_job == True and (current_minute % 5 == 0) and (current_second == 4):
        # if run_job == True and (current_second == 1):
        current_time = datetime.now()
        job()
        end_time = datetime.now()
        total_time = end_time - current_time
        print("end time- ", end_time)
        print(f'done in {total_time}')
        time.sleep(1)
        # new timestamp added
    elif run_job == True and ((current_minute + 2) % 5 == 0) and (current_second == 1):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(initialize())
        print("from loop to initialize.............")
    elif run_job == True and (current_minute % 5 != 0) and (current_second % 10 == 0):
        loop.run_until_complete(process_pending_items())
    elif run_job == False:
        break

