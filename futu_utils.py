from futu import *
import pandas as pd
from datetime import datetime, timedelta
import time
import logging

# 配置日志
logger = logging.getLogger('FutuAPI')

def get_change_percentage(kline_data):
    """计算涨跌幅"""
    return ((kline_data['close'] - kline_data['open']) / kline_data['open'] * 100).round(2)

def get_volume_change(kline_data):
    """计算成交量变化"""
    return ((kline_data['volume'] - kline_data['volume'].shift(1)) / kline_data['volume'].shift(1) * 100).round(2)



class FutuAPI:
    def __init__(self, host='127.0.0.1', port=11111):
        self.logger = logging.getLogger('Trade')
        self.logger.setLevel(logging.INFO)
        self.logger.info(f"初始化 FutuAPI，连接 {host}:{port}")
        self.quote_ctx = OpenQuoteContext(host=host, port=port)
        self.trade_ctx = OpenSecTradeContext(host=host, port=port, security_firm=SecurityFirm.FUTUSECURITIES)
        self.subscribed_stocks = set()
        
    def __del__(self):
        self.logger.info("关闭 FutuAPI 连接")
        if hasattr(self, 'quote_ctx'):
            self.quote_ctx.close()
        if hasattr(self, 'trade_ctx'):
            self.trade_ctx.close()
            
    def subscribe_stock(self, stock_code):
        """订阅股票行情"""
        try:
            if stock_code not in self.subscribed_stocks:
                self.logger.info(f"尝试订阅股票: {stock_code}")
                result = self.quote_ctx.subscribe([stock_code], [SubType.QUOTE, SubType.TICKER, SubType.K_1M])
                self.logger.info(f"订阅结果: {result}")
                
                if not isinstance(result, tuple) or len(result) != 2:
                    self.logger.error(f"订阅返回格式错误: {result}")
                    return False
                    
                ret, data = result
                if ret == RET_OK:
                    self.subscribed_stocks.add(stock_code)
                    self.logger.info(f"成功订阅股票: {stock_code}")
                else:
                    self.logger.error(f"订阅股票失败: {stock_code}, 错误: {data}")
                return ret == RET_OK
            return True
        except Exception as e:
            self.logger.error(f"订阅股票时发生异常: {str(e)}", exc_info=True)
            return False

    def get_stock_quote(self, stock_code):
        """获取股票实时行情"""
        try:
            if not self.subscribe_stock(stock_code):
                return None
                
            self.logger.info(f"获取股票行情: {stock_code}")
            result = self.quote_ctx.get_stock_quote([stock_code])
            self.logger.info(f"获取行情结果: {result}")
            
            if not isinstance(result, tuple) or len(result) != 2:
                self.logger.error(f"获取行情返回格式错误: {result}")
                return None
                
            ret, data = result
            if ret == RET_OK:
                return data
            else:
                self.logger.error(f"获取股票行情失败: {data}")
                return None
        except Exception as e:
            self.logger.error(f"获取股票行情时发生异常: {str(e)}", exc_info=True)
            return None

    def get_market_snapshot(self, stock_list):
        """获取市场快照"""
        try:
            self.logger.info(f"获取市场快照: {stock_list}")
            # 确保所有股票都已订阅
            for stock in stock_list:
                self.subscribe_stock(stock)
                
            result = self.quote_ctx.get_market_snapshot(stock_list)
            self.logger.info(f"获取市场快照结果: {result}")
            
            if not isinstance(result, tuple) or len(result) != 2:
                self.logger.error(f"获取市场快照返回格式错误: {result}")
                return None
                
            ret, data = result
            if ret == RET_OK:
                return data
            else:
                self.logger.error(f"获取市场快照失败: {data}")
                return None
        except Exception as e:
            self.logger.error(f"获取市场快照时发生异常: {str(e)}", exc_info=True)
            return None

    def get_history_kline(self, stock_code, start_date, end_date, ktype=KLType.K_DAY, autype=AuType.QFQ):
        """获取历史K线数据"""
        try:
            self.logger.info(f"获取历史K线: {stock_code}, {start_date} 到 {end_date}")
            result = self.quote_ctx.request_history_kline(
                code=stock_code,
                start=start_date,
                end=end_date,
                ktype=ktype,
                autype=autype,
                max_count=1000
            )
            if not isinstance(result, tuple) or len(result) < 2:
                self.logger.error(f"获取历史K线返回格式错误: {result}")
                return None
            ret, data = result[:2]
            if ret == RET_OK:
                if data is not None and not data.empty:
                    return data
                else:
                    self.logger.warning(f"获取历史K线无数据: {stock_code}, {start_date} 到 {end_date}")
                    return None
            else:
                self.logger.error(f"获取历史K线失败: {data}")
                return None
        except Exception as e:
            self.logger.error(f"获取历史K线时发生异常: {str(e)}", exc_info=True)
            return None

    def get_account_funds(self):
        """获取账户资金"""
        try:
            self.logger.info("获取账户列表")
            result = self.trade_ctx.get_acc_list()
            self.logger.info(f"获取账户列表结果: {result}")
            if not isinstance(result, tuple) or len(result) != 2:
                self.logger.error(f"获取账户列表返回格式错误: {result}")
                return None
            ret, data = result
            if ret != RET_OK:
                self.logger.error(f"获取账户列表失败: {data}")
                return None
            if len(data) == 0:
                self.logger.error("未找到交易账户")
                return None
            acc_id = data['acc_id'][0]
            self.logger.info(f"获取账户资金: acc_id={acc_id}")
            # 修正资金查询方法
            result = self.trade_ctx.accinfo_query(trd_env=TrdEnv.SIMULATE, acc_id=acc_id)
            self.logger.info(f"获取账户资金结果: {result}")
            if not isinstance(result, tuple) or len(result) != 2:
                self.logger.error(f"获取账户资金返回格式错误: {result}")
                return None
            ret, data = result
            if ret == RET_OK:
                return data
            else:
                self.logger.error(f"获取账户资金失败: {data}")
                return None
        except Exception as e:
            self.logger.error(f"获取账户资金时发生异常: {str(e)}", exc_info=True)
            return None

    def get_positions(self):
        """获取持仓信息"""
        try:
            self.logger.info("获取账户列表")
            result = self.trade_ctx.get_acc_list()
            self.logger.info(f"获取账户列表结果: {result}")
            if not isinstance(result, tuple) or len(result) != 2:
                self.logger.error(f"获取账户列表返回格式错误: {result}")
                return None
            ret, data = result
            if ret != RET_OK:
                self.logger.error(f"获取账户列表失败: {data}")
                return None
            if len(data) == 0:
                self.logger.error("未找到交易账户")
                return None
            acc_id = data['acc_id'][0]
            self.logger.info(f"获取持仓信息: acc_id={acc_id}")
            # 只用 position_list_query
            result = self.trade_ctx.position_list_query(trd_env=TrdEnv.SIMULATE, acc_id=acc_id)
            self.logger.info(f"获取持仓信息结果: {result}")
            if not isinstance(result, tuple) or len(result) != 2:
                self.logger.error(f"获取持仓信息返回格式错误: {result}")
                return None
            ret, data = result
            if ret == RET_OK and data is not None and not data.empty:
                return data
            else:
                self.logger.error(f"获取持仓信息失败: {data}")
                return None
        except Exception as e:
            self.logger.error(f"获取持仓信息时发生异常: {str(e)}", exc_info=True)
            return None

    def place_order(self, stock_code, price, qty, trd_side=TrdSide.BUY, order_type=OrderType.NORMAL):
        """下单"""
        try:
            self.logger.info("获取账户列表")
            result = self.trade_ctx.get_acc_list()
            self.logger.info(f"获取账户列表结果: {result}")
            
            if not isinstance(result, tuple) or len(result) != 2:
                self.logger.error(f"获取账户列表返回格式错误: {result}")
                return None
                
            ret, data = result
            if ret != RET_OK:
                self.logger.error(f"获取账户列表失败: {data}")
                return None
                
            if len(data) == 0:
                self.logger.error("未找到交易账户")
                return None
                
            acc_id = data['acc_id'][0]
            self.logger.info(f"下单: stock_code={stock_code}, price={price}, qty={qty}, acc_id={acc_id}")
            result = self.trade_ctx.place_order(
                price=price,
                qty=qty,
                code=stock_code,
                trd_side=trd_side,
                order_type=order_type,
                acc_id=acc_id
            )
            self.logger.info(f"下单结果: {result}")
            
            if not isinstance(result, tuple) or len(result) != 2:
                self.logger.error(f"下单返回格式错误: {result}")
                return None
                
            ret, data = result
            if ret == RET_OK:
                return data
            else:
                self.logger.error(f"下单失败: {data}")
                return None
        except Exception as e:
            self.logger.error(f"下单时发生异常: {str(e)}", exc_info=True)
            return None 