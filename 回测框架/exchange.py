import pandas as pd
import logging
from datetime import datetime
import warnings
warnings.filterwarnings("ignore", category=FutureWarning, message="The behavior of DataFrame concatenation")
class Exchange:
    def __init__(self, context):
        """
        初始化Exchange类
        """
        self.context = context  # 引入Context对象以便管理账户资金和持仓
        self.current_price = {}  # 用于存储每个symbol的最新价格
    def set_price(self, symbol, price):
        """
        设置symbol的当前价格
        """
        self.current_price[symbol] = price
        logging.info(f"Set price for {symbol}: {price}")
    def open_position(self, symbol, quantity, side):
        """
        开仓：处理买入或卖出开仓订单
        """
        if symbol not in self.current_price:
            logging.warning(f"Cannot open position for {symbol}: price not set.")
            return
        price = self.current_price[symbol]
        cost = quantity * price
        # 判断是否有足够的资金开仓
        if cost > self.context.balance:
            logging.warning(f"Insufficient balance to open {side} position for {symbol}. Required: {cost}, Available: {self.context.balance}")
            return
        # 根据方向调整数量的正负，并记录仓位方向
        if side == "long":
            # 开多仓
            quantity_signed = quantity  # 多头数量为正
            self.context.add_position(symbol, quantity_signed, price, "long")
            logging.info(f"Opened long position for {symbol}: quantity={quantity}, price={price}")
        elif side == "short":
            # 开空仓
            quantity_signed = -quantity  # 空头数量为负
            self.context.add_position(symbol, quantity_signed, price, "short")
            logging.info(f"Opened short position for {symbol}: quantity={-quantity}, price={price}")

    def close_position(self, symbol, side):
        """
        平仓：处理指定symbol的所有多空持仓，并更新资金
        """
        if symbol not in self.current_price:
            logging.warning(f"Cannot close position for {symbol}: price not set.")
            return
        
        exit_price = self.current_price[symbol]
        self.context.close_position(symbol, exit_price, side)
        logging.info(f"Closed all positions for {symbol} at price {exit_price}")


    def update_equity(self):
        """
        根据当前价格更新账户权益（Equity）
        """
        
        # 对每个持仓计算浮动盈亏并累加到equity上
        for _, pos in self.context.positions.iterrows():
            symbol = pos['symbol']
            if symbol in self.current_price:
                entry_price = pos['entry_price']
                quantity = pos['quantity']
                side = pos['side']
                current_price = self.current_price[symbol]
                
                # 根据持仓方向计算浮动盈亏
                if side == 'long':
                    floating_pnl = (current_price - entry_price) * quantity
                elif side == 'short':
                    floating_pnl = (entry_price - current_price) * quantity
                
                # 累加浮动盈亏到equity
                self.context.equity += floating_pnl


    def get_equity(self):
        """
        获取当前账户的权益
        """
        return self.context.equity

    def get_balance(self):
        """
        获取当前账户余额
        """
        return self.context.balance

    def get_open_positions(self):
        """
        返回当前所有持仓
        """
        return self.context.get_open_positions()

