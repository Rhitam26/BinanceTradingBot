# !./generate_models.sh
from sqlalchemy import create_engine, text, desc, and_
from sqlalchemy.orm import sessionmaker
import ast
from background.models import Symbol, RefValue, TickFiveMin, Marketorder, TickLast, SymbolsTrading, MarketorderLong, TradesIndicator
import pandas as pd

class DatabaseConnection:
    def __init__(self):
        config_file_path = 'background/security.txt'
        with open(config_file_path, 'r') as file:
            content = file.read()
        security_config = ast.literal_eval(content)
        username = security_config["username"]
        password = security_config["password"]
        hostname = security_config["hostname"]
        port = security_config["port"]
        database_name = security_config["database_name"]
        connection_url = f"mysql+pymysql://{username}:{password}@{hostname}:{port}/{database_name}"
        self.engine = create_engine(connection_url)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def _start_new_session(self):
        if self.session is not None:
            self.session.close()
        self.session = self.Session()

    def close_session(self):
        if self.session is not None:
            self.session.close()
            

    # def insert_tick_data(self, s_val, E_val, o_val, h_val, l_val, c_val):
    #     try:
    #         self._start_new_session()
    #         new_tick = Tick(s=s_val, E=E_val, o=o_val, h=h_val, l=l_val, c=c_val)
    #         self.session.add(new_tick)
    #         self.session.commit()
    #     except Exception as e:
    #         self.session.rollback()
    #         raise e
    #     finally:
    #         self.close_session()
    def insert_tick_five_min_data(self, s, E, o, h, l, c):
        try:
            self._start_new_session()
            new_tick = TickFiveMin(s=s, E=E, o=o, h=h, l=l, c=c)
            self.session.add(new_tick)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e
        finally:
            self.close_session()


    def insert_ref_values(self, open_time_val, open_val, high_val, low_val, close_val, volume_val, close_time_val, qav_val, num_trades_val, taker_base_vol_val, taker_quote_vol_val, ignore_val, ema_3_ref_val, ema_7_ref_val, upper_band_ref_val, lower_band_ref_val, Upper_Cloud_ref_val, Lower_Cloud_ref_val, Nmacd_ref_val, Signal_ref_val, symbol_val, strategy_val):
        try:
            self._start_new_session()
            new_RefValue = RefValue(open_time=open_time_val, open=open_val, high=high_val, low=low_val, close=close_val, volume=volume_val, close_time=close_time_val, qav=qav_val, num_trades=num_trades_val, taker_base_vol=taker_base_vol_val, taker_quote_vol=taker_quote_vol_val, ignore_=ignore_val, ema_3_ref=ema_3_ref_val, ema_7_ref=ema_7_ref_val, upper_band_ref=upper_band_ref_val, lower_band_ref=lower_band_ref_val, Upper_Cloud_ref=Upper_Cloud_ref_val, Lower_Cloud_ref=Lower_Cloud_ref_val, Nmacd_ref=Nmacd_ref_val, Signal_ref=Signal_ref_val, symbol=symbol_val, strategy=strategy_val)
            self.session.add(new_RefValue)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e
        finally:
            self.close_session()


    def truncate_symbols_table(self):
        self._start_new_session()
        self.session.execute(text('TRUNCATE TABLE symbols'))
        self.session.commit()
        self.close_session()
        print("Table 'symbols' has been truncated.")
    
    def insert_symbol_data(self, s_val, active_val=1):
        self._start_new_session()
        new_symbol = Symbol(s=s_val, active=active_val)
        self.session.add(new_symbol)
        self.session.commit()
        self.close_session()
        print("Data inserted into the 'symbols' table.")

    def insert_symbol_trading_data(self, s_val, active_val=1, iter_val=0, strategy_name_val=""):
        self._start_new_session()
        new_symbol = SymbolsTrading(s=s_val, active=active_val, iter=iter_val, strategy_name=strategy_name_val)
        self.session.add(new_symbol)
        self.session.commit()
        self.close_session()
        print("Data inserted into the 'symbols' table.")
    def get_active_symbols(self):
        self._start_new_session()
        active_symbols = self.session.query(Symbol).filter(Symbol.active == 1).all()
        df = pd.DataFrame([symbol.__dict__ for symbol in active_symbols])
        df.drop(columns=['_sa_instance_state'], inplace=True)
        self.close_session()
        return df
    
    def get_symbols_trading(self):
        self._start_new_session()
        active_symbols = (
            self.session.query(SymbolsTrading)
            .filter(SymbolsTrading.active == 1)
            .distinct()  # Add distinct condition here
            .all()
        )
        df = pd.DataFrame([symbol.__dict__ for symbol in active_symbols])
        df.drop(columns=['_sa_instance_state'], inplace=True)
        self.close_session()
        return df


    def get_latest_tick(self, symbol):
        self._start_new_session()
        query = self.session.query(Tick).filter(Tick.s == symbol).order_by(desc(Tick.E)).limit(200)
        df = pd.read_sql(query.statement, query.session.bind)
        self.close_session()
        return df
    
    def get_latest_tick_five_min(self, symbol):
        self._start_new_session()
        query = self.session.query(TickFiveMin).filter(TickFiveMin.s == symbol).order_by(desc(TickFiveMin.E)).limit(200)
        df = pd.read_sql(query.statement, query.session.bind)
        self.close_session()
        return df
    def get_old_tick_data(self):
        self._start_new_session()
        query = self.session.query(
            TickFiveMin.s,
            TickFiveMin.E,
            TickFiveMin.o,
            TickFiveMin.h,
            TickFiveMin.l,
            TickFiveMin.c
        ).order_by(desc(TickFiveMin.id)).limit(50000)
        
        df = pd.read_sql(query.statement, query.session.bind)
        self.close_session()
        return df
    def get_all_ref_values(self):
        self._start_new_session()
        query = self.session.query(RefValue).order_by(RefValue.open_time)
        df = pd.read_sql(query.statement, query.session.bind)
        self.close_session()
        return df
    def get_all_tick_last_data(self):
        self._start_new_session()
        query = self.session.query(
            TickLast.s,
            TickLast.E,
            TickLast.o,
            TickLast.h,
            TickLast.l,
            TickLast.c
        )
        df = pd.read_sql(query.statement, query.session.bind)
        self.close_session()
        return df

    def get_ref_values(self, strategy_val, symbol_val):
            self._start_new_session()
            query = self.session.query(RefValue).filter(
                and_(
                    RefValue.strategy == strategy_val,
                    RefValue.symbol == symbol_val
                )
            ).order_by(RefValue.open_time)
            
            df = pd.read_sql(query.statement, query.session.bind)
            self.close_session()
            return df

    # def get_unc_values(self):
    #     self._start_new_session()
    #     query = self.session.query(Unc).order_by(Unc.open_time)
    #     df = pd.read_sql(query.statement, query.session.bind)
    #     self.close_session()
    #     return df

    def insert_trades_indicator_values(self, strategy_val, symbol_val, open_time_val, open_val, high_val,
                                       low_val, close_val, close_time_val, atr_val, sma_val, ema3_val, ema7_val,
                                       upper_band_val, lower_band_val, upper_cloud_val, lower_cloud_val,
                                       Nmacd_val, Signal_val, ema_3_avg_val, ema_7_avg_val, upper_band_avg_val,
                                       lower_band_avg_val, Nmacd_avg_val, Signal_avg_val,
                                       Upper_Cloud_avg_val, Lower_Cloud_avg_val):
        try:
            # self._start_new_session()
            new_trade = TradesIndicator(strategy=strategy_val, symbol=symbol_val, open_time=open_time_val,
                                    open=open_val, high=high_val, low=low_val,  close=close_val,
                                    close_time=close_time_val, atr=atr_val, sma=sma_val,
                                    ema3=ema3_val, ema7=ema7_val, upper_band=upper_band_val,
                                    lower_band=lower_band_val, Upper_Cloud=upper_cloud_val,
                                    Lower_Cloud=lower_cloud_val, Nmacd=Nmacd_val, Signal=Signal_val,
                                    ema_3_avg=ema_3_avg_val, ema_7_avg=ema_7_avg_val,
                                    upper_band_avg=upper_band_avg_val, lower_band_avg=lower_band_avg_val,
                                    Nmacd_avg=Nmacd_avg_val, Signal_avg=Signal_avg_val,
                                    Upper_Cloud_avg=Upper_Cloud_avg_val, Lower_Cloud_avg=Lower_Cloud_avg_val )
            self.session.add(new_trade)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e
        finally:
            pass
            # self.close_session()


    def insert_marketorder(self, orderId, transaction_type, price_executed, status, intent, client_price, symbol, StopLossStatus, position, qty, remarks, strategy, stop_loss, take_profit, fixed_sl):
        try:
            #self._start_new_session()
            new_order = Marketorder(
            orderId=orderId,
            transaction_type=transaction_type,
            price_executed=price_executed,
            status=status,
            intent=intent,
            client_price=client_price,
            symbol=symbol,
            StopLossStatus=StopLossStatus,
            Position=position,
            qty=qty,
            remarks=remarks,
            strategy=strategy,
            stop_loss=stop_loss,
            take_profit=take_profit,
            fixed_sl=fixed_sl
          )
          
            self.session.add(new_order)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e
        finally:
            pass
            #self.session.close()

    def insert_marketorderlong(self, orderId, transaction_type, price_executed, status, intent, client_price, symbol,
                           StopLossStatus, position, qty, remarks, strategy, stop_loss, take_profit, fixed_sl):
        try:
            # self._start_new_session()
            new_order = MarketorderLong(
                orderId=orderId,
                transaction_type=transaction_type,
                price_executed=price_executed,
                status=status,
                intent=intent,
                client_price=client_price,
                symbol=symbol,
                StopLossStatus=StopLossStatus,
                Position=position,
                qty=qty,
                remarks=remarks,
                strategy=strategy,
                stop_loss=stop_loss,
                take_profit=take_profit,
                fixed_sl=fixed_sl
            )

            self.session.add(new_order)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e
        finally:
            pass
            # self.session.close()


    def update_symbol_trading_active(self, s, active):
        try:
            self._start_new_session()
            self.session.query(SymbolsTrading)\
                .filter(SymbolsTrading.s == s)\
                .update({'active': active})
    
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e
        finally:
            self.close_session()

    def fetch_marketorder(self, status, transaction_type):
        try:
            self._start_new_session()
            query = self.session.query(
                Marketorder.id,
                Marketorder.symbol, 
                Marketorder.qty,
                Marketorder.price_executed,
                Marketorder.strategy,
                Marketorder.stop_loss,
                Marketorder.take_profit,
                Marketorder.fixed_sl
            ).filter(
                Marketorder.status == status,
                Marketorder.transaction_type == transaction_type
            )
    
            result_df = pd.read_sql(query.statement, self.session.bind)
            return result_df
        except Exception as e:
            raise e
        finally:
            self.close_session()

    def fetch_marketorderlong(self, status, transaction_type):
        try:
            self._start_new_session()
            query = self.session.query(
                MarketorderLong.id,
                MarketorderLong.symbol,
                MarketorderLong.qty,
                MarketorderLong.price_executed,
                MarketorderLong.strategy,
                MarketorderLong.stop_loss,
                MarketorderLong.take_profit,
                MarketorderLong.fixed_sl
            ).filter(
                MarketorderLong.status == status,
                MarketorderLong.transaction_type == transaction_type
            )

            result_df = pd.read_sql(query.statement, self.session.bind)
            return result_df
        except Exception as e:
            raise e
        finally:
            self.close_session()


    def fetch_all_ticklast_data(self):
        try:
            self._start_new_session()
            query = self.session.query(TickLast)
            df = pd.read_sql(query.statement, query.session.bind)
            return df  
        except Exception as e:
            raise e
        finally:
            self.close_session()

    def update_marketorder_status(self, id, new_status):
        try:
            self._start_new_session()
            self.session.query(Marketorder)\
                .filter(Marketorder.id == id)\
                .update({'status': new_status})    
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e
        finally:
            self.close_session()

    def update_marketorderlong_status(self, id, new_status):
        try:
            self._start_new_session()
            self.session.query(MarketorderLong)\
                .filter(MarketorderLong.id == id)\
                .update({'status': new_status})
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e
        finally:
            self.close_session()


    def upsert_tick_data(self, s_val, E_val, o_val, h_val, l_val, c_val):
        try:
            self._start_new_session()
            self.session.execute(
                text("CALL UpsertTickLast(:s_val, :E_val, :o_val, :h_val, :l_val, :c_val)"),
                {
                    "s_val": s_val,
                    "E_val": E_val,
                    "o_val": o_val,
                    "h_val": h_val,
                    "l_val": l_val,
                    "c_val": c_val,
                }
            )
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e
        finally:
            self.close_session()

