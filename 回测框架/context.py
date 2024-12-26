import pandas as pd
import logging
from datetime import datetime
import warnings
warnings.filterwarnings("ignore", category=FutureWarning, message="The behavior of DataFrame concatenation")
class Context:
    def __init__(self, initial_balance=50000):
        self.balance = initial_balance  # 初始账户余额
        self.equity = initial_balance   # 初始账户权益
        self.open_orders = pd.DataFrame(columns=['id', 'symbol', 'price', 'status', 'timestamp','order_type'])
        self.positions = pd.DataFrame(columns=['symbol', 'quantity', 'entry_price', 'side'])  # 添加'side'列
        #self.intermediate_data = pd.DataFrame()
        self.order_id_counter = 0
        self.pnl_history = []
        self.trade_logs = []  # 用于记录每次交易日志


    def add_order(self, symbol, price, time_sec, order_type, status='pending'):
        # 为订单分配唯一 ID
        order_id = self.order_id_counter
        self.order_id_counter += 1
        new_order = pd.DataFrame([[order_id, symbol, price, status, time_sec, order_type]], 
                                 columns=self.open_orders.columns)
        self.open_orders = pd.concat([self.open_orders, new_order], ignore_index=True)
        logging.info(f"Order added: id={order_id}, symbol={symbol}, price={price}, status={status}")
        return order_id

    def update_order_status(self, order_id, new_status):
        # 更新订单状态并记录日志
        self.open_orders.loc[self.open_orders['id'] == order_id, 'status'] = new_status
        logging.info(f"Order status updated: id={order_id}, new_status={new_status}")

    # def add_strategy_signal(self, strategy_name, symbol, signal):
    #     # 将策略信号添加到 intermediate_data
    #     if strategy_name not in self.intermediate_data.columns:
    #         self.intermediate_data[strategy_name] = None  # 添加新的策略列
    #     new_signal = pd.DataFrame({strategy_name: [signal]}, index=[symbol])
    #     self.intermediate_data = pd.concat([self.intermediate_data, new_signal], ignore_index=True)

    def add_position(self, symbol, quantity, price, side): 
        """
        添加持仓记录
        """
        new_position = pd.DataFrame([[symbol, quantity, price, side]], columns=self.positions.columns)
        self.positions = pd.concat([self.positions, new_position], ignore_index=True)
        logging.info(f"Position added: symbol={symbol}, quantity={quantity}, entry_price={price}, side={side}")

    def close_position(self, symbol, exit_price, side):
        """
        平仓指定symbol的所有持仓，并计算盈亏
        """
        position = self.positions[(self.positions['symbol'] == symbol) & (self.positions['side'] == side)]
        
        if not position.empty:
            for _, row in position.iterrows():
                entry_price = row['entry_price']
                quantity = row['quantity']
                side = row['side']
                # 根据仓位方向计算盈亏
                pnl = (exit_price - entry_price) * quantity if side == 'long' else (entry_price - exit_price) * quantity
                self.pnl_history.append(pnl)
                # 更新账户余额和权益
                self.balance += pnl
                # 记录日志
                logging.info(f"Position closed: symbol={symbol}, exit_price={exit_price}, PnL={pnl}, side={side}")
            # 从持仓中移除该symbol的所有记录
            self.positions = self.positions[(self.positions['symbol'] != symbol) & (self.positions['side'] != side)]
    
    def record_trade_log(self, symbol, price, time_sec, pnl, equity, order_type):
        """
        记录每次交易的日志
        """
        log = {
            "symbol": symbol,
            "price": price,
            "time_sec": time_sec,
            "pnl": pnl,
            "equity": equity,
            "order_type": order_type
        }
        self.trade_logs.append(log)        

    def get_open_positions(self):
        return self.positions
    
    def get_orders(self):
        # 获取所有当前订单
        return self.open_orders