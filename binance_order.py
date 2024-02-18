import pandas as pd
#import logging
from binance.um_futures import UMFutures
from binance.lib.utils import config_logging
from binance.error import ClientError
#config_logging(logging, logging.DEBUG)
import ast
import math

class binan:
    def __init__(self):
        config_file_path = 'background/security_vps.txt'
        with open(config_file_path, 'r') as file:
            content = file.read()
        security_config = ast.literal_eval(content)
        key = security_config["api_key"]
        secret = security_config["api_secret"]

        self.um_futures_client = UMFutures(key=key, secret=secret, base_url="https://fapi.binance.com")
        # self.um_futures_client = UMFutures(key=key, secret=secret, base_url="https://testnet.binancefuture.com")
        
    def place_order(self, symbol, order_side, qty):
        try:
            response = self.um_futures_client.new_order(
                symbol=symbol,
                side=order_side,
                type="MARKET",
                quantity=qty,
            )
            orderid = response['orderId']
            return True, orderid

        except ClientError as error:
            print(
                "Found error. status: {}, error code: {}, error message: {}".format(
                    error.status_code, error.error_code, error.error_message
                )
            )
            return False, 0


    def place_sl_order(self, symbol, order_side, qty, sl_price):
        try:
            # response contains all info about order
            response = self.um_futures_client.new_order(
                symbol=symbol,
                side=order_side,
                type="STOP_MARKET",
                quantity=qty,
                stopPrice=sl_price,
            )
            # get orderid from response
            orderid = response['orderId']
            return True, orderid

        except ClientError as error:
            print(
                "Found error. status: {}, error code: {}, error message: {}".format(
                    error.status_code, error.error_code, error.error_message
                )
            )
            return False, 0

    def place_tp_order(self, symbol, order_side, qty, tp_price):
        try:
            # response contains all info about order
            response = self.um_futures_client.new_order(
                symbol=symbol,
                side=order_side,
                type="TAKE_PROFIT_MARKET",
                quantity=qty,
                stopPrice=tp_price,
            )
            # get orderid from response
            orderid = response['orderId']
            return True, orderid

        except ClientError as error:
            print(
                "Found error. status: {}, error code: {}, error message: {}".format(
                    error.status_code, error.error_code, error.error_message
                )
            )
            return False, 0

    def get_order_details(self, order_id, symbol):
        try:
            response = self.um_futures_client.query_order(
                symbol=symbol,
                orderId=order_id,
                recvWindow=60000
            )
            if response is not None:
                priceExecuted = response.get("avgPrice")
                response_status = response.get("status")
                if priceExecuted is not None and response_status is not None:
                    return 1, priceExecuted, response_status
                else:
                    return 0, None, None
            else:
                return 0, None, None
        except ClientError as error:
            return 0, None, None


    def get_open_orders(self, order_id, symbol):
        try:
            # Fetch open orders for the specified symbol
            response = self.um_futures_client.get_open_orders(
                symbol=symbol,
                orderId=order_id,
                recvWindow=50000
            )
            if response is not None:
                print(response)
                # priceExecuted = response["avgPrice"]
                response_status = response.get("status")
                if response_status is not None:
                    return response_status
                else:
                    print("Incomplete response received.")
                    return 0
            else:
                print("Order not found.")
                return 0

        except ClientError as error:
            print(
                "Found error. status: {}, error code: {}, error message: {}".format(
                    error.status_code, error.error_code, error.error_message
                )
            )

    def cancel_open_orders(self, order_id, symbol):
        try:
            response = self.um_futures_client.cancel_open_orders(
                symbol=symbol,
                orderId=order_id,
                recvWindow=50000
            )
            print(response)

        except ClientError as error:
            print(
                "Found error. status: {}, error code: {}, error message: {}".format(
                    error.status_code, error.error_code, error.error_message
                )
            )

    def get_pricePrecision(self, symbol):
        try:
            exchange_info = self.um_futures_client.exchange_info()
            # Find the specific symbol information
            symbol_info = next(item for item in exchange_info['symbols'] if item['symbol'] == symbol)
            pricePrecision = symbol_info['pricePrecision']
            return pricePrecision
        except ClientError as error:
            print(
                "Found error. status: {}, error code: {}, error message: {}".format(
                    error.status_code, error.error_code, error.error_message
                )
            )
            return False, 0

    def get_qty(self, symbol, capital, price):
        try:
            quantity = capital / price
            exchange_info = self.um_futures_client.exchange_info()
            # Find the specific symbol information
            symbol_info = next(item for item in exchange_info['symbols'] if item['symbol'] == symbol)

            # symbol_info = client.exchange_info(symbol=symbol)
            step_size = 0.0
            for f in symbol_info['filters']:
                if f['filterType'] == 'LOT_SIZE':
                    step_size = float(f['stepSize'])
                    minQty = float(f['minQty'])
            qtyprecision = int(round(-math.log(step_size, 10), 0))
            # print("precision: ", precision)
            qty = float(round(quantity, qtyprecision))
            if qty == 0:
                qty = minQty
                # print(f"minQty: {minQty}")
            return qty
        except ClientError as error:
            print(
                "Found error. status: {}, error code: {}, error message: {}".format(
                    error.status_code, error.error_code, error.error_message
                )
            )
            return False, 0

    def place_sl_limit_order(self, symbol, order_side, qty, sl_price, stop_sl_price):
        try:
            # response contains all info about order
            response = self.um_futures_client.new_order(
                symbol=symbol,
                side=order_side,
                type="STOP",   # 'STOP'-In stop add stopprice, 'LIMIT'- with limit no stopprice
                timeInForce="GTC",  # Good 'til Cancel
                quantity=qty,
                price=sl_price,
                stopprice=stop_sl_price
            )
            orderid = response['orderId']
            print(f"response_sl: {response}")
            return True, orderid

        except ClientError as error:
            print(
                "Found error. status: {}, error code: {}, error message: {}".format(
                    error.status_code, error.error_code, error.error_message
                )
            )
            return False, 0

    def place_tp_limit_order(self, symbol, order_side, qty, tp_price, stop_tp_price):
        try:
            # response contains all info about order
            response = self.um_futures_client.new_order(
                symbol=symbol,
                side=order_side,
                type='TAKE_PROFIT',     # "TAKE_PROFIT" - In take_profit add stopprice, 'LIMIT'- with limit no stopprice
                timeInForce="GTC",  # Good 'til Cancel
                quantity=qty,
                price=tp_price,
                stopprice=stop_tp_price
            )
            orderid = response['orderId']
            print(f"response_tp: {response}")
            return True, orderid

        except ClientError as error:
            print(
                "Found error. status: {}, error code: {}, error message: {}".format(
                    error.status_code, error.error_code, error.error_message
                )
            )
            return False, 0

