import sys
from futu import *
import pandas as pd
from datetime import datetime, timedelta
import time
from futu_utils import FutuAPI
import argparse
from futu import OptionType
import socket
import logging
import os
from collections import deque
import traceback

# 全局变量
CODE = 'HK.09988'  # 股票代码
KLINE_10M_COUNT = 30  # 10分钟K线数量
SLEEP_INTERVAL = 1  # 轮询间隔改为1秒
PROFIT_THRESHOLD = 0.20  # 止盈阈值
LOSS_THRESHOLD = -0.05  # 止损阈值
MAX_QUEUE_SIZE = 100  # 订阅数据队列最大长度

# 用于存储订阅数据的队列
quote_queue = deque(maxlen=MAX_QUEUE_SIZE)
ticker_queue = deque(maxlen=MAX_QUEUE_SIZE)

def setup_logger():
    """配置并返回一个 logger 对象"""
    logger = logging.getLogger('Trade')
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler(f'trade_{datetime.now().strftime("%Y%m%d")}.log', encoding='utf-8')
        fh.setLevel(logging.INFO)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        logger.addHandler(fh)
        logger.addHandler(ch)
    return logger

def subscribe_option_quotes(quote_ctx, option_codes):
    """订阅期权实时行情"""
    ret, data, *_ = quote_ctx.subscribe(option_codes, [SubType.QUOTE, SubType.TICKER, SubType.ORDER_BOOK])
    if ret != RET_OK:
        logger.info("订阅期权行情失败: %s", data)
        return False
    return True

class QuoteHandler(StockQuoteHandlerBase):
    def __init__(self, logger):
        super().__init__()
        self.logger = logger
        
    def on_recv_rsp(self, rsp_pb):
        result = super().on_recv_rsp(rsp_pb)
        logger.info('[DEBUG] QuoteHandler super().on_recv_rsp 返回: %s %s', type(result), result)
        # 临时只返回 result，便于观察
        return result

class OrderBookHandler(OrderBookHandlerBase):
    def __init__(self, logger):
        super().__init__()
        self.logger = logger
        
    def on_recv_rsp(self, rsp_pb):
        result = super().on_recv_rsp(rsp_pb)
        logger.info('[DEBUG] OrderBookHandler super().on_recv_rsp 返回: %s %s', type(result), result)
        return result

class TickerHandler(TickerHandlerBase):
    def __init__(self, logger):
        super().__init__()
        self.logger = logger
        
    def on_recv_rsp(self, rsp_pb):
        result = super().on_recv_rsp(rsp_pb)
        logger.info('[DEBUG] TickerHandler super().on_recv_rsp 返回: %s %s', type(result), result)
        return result

def get_latest_quote_data():
    """从订阅队列中获取最新的行情数据"""
    if not quote_queue:
        return None
    
    # 获取最新的数据
    latest_data = quote_queue[-1]
    latest_time = pd.to_datetime(latest_data['svr_recv_time_bid'])  # 使用服务器推送时间
    
    # 检查队列中是否有更新的数据
    for data in reversed(list(quote_queue)[:-1]):
        current_time = pd.to_datetime(data['svr_recv_time_bid'])
        if current_time > latest_time:
            latest_data = data
            latest_time = current_time
    
    return latest_data

def get_latest_ticker_data():
    """从订阅队列中获取最新的逐笔成交数据"""
    if not ticker_queue:
        return None
    
    try:
        # 获取最新的数据
        latest_data = ticker_queue[-1]
        if 'time' not in latest_data:
            logger.info("逐笔成交数据缺少时间字段")
            return None
            
        latest_time = pd.to_datetime(latest_data['time'])
        
        # 检查队列中是否有更新的数据
        for data in reversed(list(ticker_queue)[:-1]):
            if 'time' not in data:
                continue
            current_time = pd.to_datetime(data['time'])
            if current_time > latest_time:
                latest_data = data
                latest_time = current_time
        
        return latest_data
    except Exception as e:
        logger.info("获取最新逐笔成交数据时发生错误: %s", str(e))
        return None

def is_reversal(kline_1m, kline_10m):
    """判断是否出现反转信号"""
    if kline_1m.empty or kline_10m.empty:
        return False
    
    # 获取最近1分钟K线数据
    last_1m = kline_1m.iloc[-1]
    
    # 获取最近10分钟K线数据
    last_10m = kline_10m.iloc[-1]
    
    # 判断条件：
    # 1. 1分钟K线收盘价高于开盘价（上涨）
    # 2. 1分钟K线收盘价高于10分钟K线收盘价（突破）
    # 3. 1分钟K线成交量大于10分钟K线平均成交量2倍，1分钟K线成交量并且大于前一分钟成交量两倍
    avg_volume_10m = kline_10m['volume'].mean()

     # 新增：最近1分钟涨幅 > 前1分钟跌幅2倍
    if len(kline_1m) >= 2:
        last_up = (kline_1m['close'].iloc[-1] - kline_1m['open'].iloc[-1]) / kline_1m['open'].iloc[-1]
        prev_down = (kline_1m['open'].iloc[-2] - kline_1m['close'].iloc[-2]) / kline_1m['open'].iloc[-2]
        if not (last_up > 2 * prev_down and last_up > 0):
            return False
    
    return (last_1m['close'] > last_1m['open'] and 
            last_1m['close'] > last_10m['close'] and 
            last_1m['volume'] > avg_volume_10m * 2 and last_1m['volume'] > kline_1m['volume'].iloc[-2] * 2)

def monitor_option_chain(code, option_type):
    """监控期权链实时数据"""
    # 设置日志记录器
    logger = setup_logger()
    logger.info("开始监控期权 %s", code)
    
    # 创建行情上下文
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    
    try:
        # 获取期权链
        ret, data, *_ = quote_ctx.get_option_chain(code, option_type=option_type)
        if ret != RET_OK:
            logger.error("获取期权链失败: %s", data)
            return
            
        # 提取期权代码
        option_codes = []
        if isinstance(data, pd.DataFrame):
            # 记录数据结构
            logger.info("期权链数据结构: %s", data.columns.tolist())
            logger.info("数据示例: %s", data.head().to_dict())
            
            # 只选择前10个期权代码（避免超出订阅限制）
            codes = data['code'].dropna().tolist()[:10]
            option_codes.extend(codes)
        
        if not option_codes:
            logger.error("未找到可用的期权代码")
            return
            
        logger.info("选择监控以下 %d 个期权代码: %s", len(option_codes), option_codes)
            
        # 订阅期权行情
        subscribe_option_quotes(quote_ctx, option_codes)
        
        # 设置回调处理器
        quote_ctx.set_handler(QuoteHandler(logger))
        quote_ctx.set_handler(OrderBookHandler(logger))
        quote_ctx.set_handler(TickerHandler(logger))
        
        logger.info("开始监控期权行情...")
        while True:
            time.sleep(1)  # 保持主线程运行，等待回调
            
    except KeyboardInterrupt:
        logger.info("停止监控")
    finally:
        quote_ctx.close()

def get_option_to_buy(quote_ctx, code):
    # 获取当前股票价格
    ret, stock_quote, *_ = quote_ctx.get_stock_quote([code])
    if ret != RET_OK or stock_quote.empty:
        logger.info("获取股票报价失败: %s", stock_quote)
        return None, None, None, None
    stock_price = stock_quote.iloc[0]['last_price']
    
    # 获取期权链，选取行权价最接近当前股价的看涨期权
    ret, data, *_ = quote_ctx.get_option_chain(code, option_type=OptionType.CALL)
    if ret != RET_OK or data.empty:
        logger.info("期权链获取失败: %s", data)
        return None, None, None, None
    data['strike_diff'] = (data['strike_price'] - stock_price).abs()
    data = data.sort_values('strike_diff')
    for idx, row in data.iterrows():
        option_code = row['code']
        strike_price = row['strike_price']
        ret, quote, *_ = quote_ctx.get_stock_quote([option_code])
        if ret == RET_OK and not quote.empty:
            lot_size = quote.iloc[0]['lot_size']
            logger.info("期权合约乘数: %s", lot_size)
            # 买入张数=1000/当前行权价取整，并确保是lot_size的整数倍
            qty = int(1000 // strike_price)
            qty = max(qty, 1) * lot_size
            logger.info("计算得到的买入数量: %d (考虑了合约乘数 %s)", qty, lot_size)
            return option_code, strike_price, qty, lot_size
    return None, None, None, None

def buy_option(trade_ctx, option_code, qty):
    logger.info("市价买入期权: %s, 数量: %d", option_code, qty)
    # 确保数量是lot_size的整数倍
    ret, quote, *_ = trade_ctx.get_stock_quote([option_code])
    if ret == RET_OK and not quote.empty:
        lot_size = quote.iloc[0]['lot_size']
        qty = (qty // lot_size) * lot_size  # 确保是lot_size的整数倍
        print("调整后的买入数量: %d (合约乘数: %s)", qty, lot_size)
    
    ret, data, *_ = trade_ctx.place_order(price=0, qty=qty, code=option_code, trd_side=TrdSide.BUY, order_type=OrderType.MARKET, adjust_limit=0, trd_env=TrdEnv.SIMULATE)
    print("买入结果", ret, data)
    # 获取实际成交价
    if ret == RET_OK and 'deal_price' in data:
        deal_price = data['deal_price']
    else:
        deal_price = None
    return ret == RET_OK, deal_price

def monitor_profit_loss(trade_ctx, option_code, buy_price, qty):
    """监控期权盈亏"""
    if buy_price is None:
        return
    
    while True:
        try:
            # 获取当前价格
            ret, quote, *_ = trade_ctx.get_stock_quote([option_code])
            if ret != RET_OK or quote.empty:
                time.sleep(1)
                continue
                
            current_price = quote.iloc[0]['last_price']
            profit_ratio = (current_price - buy_price) / buy_price
            
            # 止盈止损
            if profit_ratio >= PROFIT_THRESHOLD or profit_ratio <= LOSS_THRESHOLD:
                logger.info("触发止盈止损: 当前价格 %.2f, 买入价格 %.2f, 盈亏比例 %.2f%%", 
                          current_price, buy_price, profit_ratio * 100)
                sell_all(trade_ctx, option_code, qty, buy_price)
                break
                
            time.sleep(1)
        except Exception as e:
            logger.error("监控盈亏时发生错误: %s", str(e))
            time.sleep(1)

def is_up_trend(kline_10m):
    """判断是否处于上涨趋势"""
    if len(kline_10m) < 2:
        return False
        
    # 计算上涨和下跌的K线数量
    up_count = 0
    down_count = 0
    up_sum = 0
    down_sum = 0
    
    for i in range(1, len(kline_10m)):
        change = (kline_10m['close'].iloc[i] - kline_10m['close'].iloc[i-1]) / kline_10m['close'].iloc[i-1]
        if change > 0:
            up_count += 1
            up_sum += change
        else:
            down_count += 1
            down_sum += change
    
    logger.info("[趋势判定] 10分钟上涨K线数: %d, 下跌K线数: %d, 上涨幅和: %.4f, 下跌幅和: %.4f", 
               up_count, down_count, up_sum, down_sum)
    
    # 上涨K线数量大于下跌K线数量，且上涨幅度和大于下跌幅度和
    return up_count > down_count and up_sum > abs(down_sum)

def sell_all(trade_ctx, option_code, qty, buy_price):
    # 确保卖出数量是lot_size的整数倍
    ret, quote, *_ = trade_ctx.get_stock_quote([option_code])
    if ret == RET_OK and not quote.empty:
        lot_size = quote.iloc[0]['lot_size']
        qty = (qty // lot_size) * lot_size  # 确保是lot_size的整数倍
    
    ret, data, *_ = trade_ctx.place_order(price=0, qty=qty, code=option_code, trd_side=TrdSide.SELL, order_type=OrderType.MARKET, adjust_limit=0, trd_env=TrdEnv.SIMULATE)
    if ret == RET_OK:
        logger.info("成功卖出期权: %s, 数量: %d", option_code, qty)
    else:
        logger.error("卖出期权失败: %s", data)

def trend_reversal_strategy(futu_api, stock_code):
    """趋势反转策略"""
    try:
        logger.info("\n[流程图策略] 开始分析股票 %s", stock_code)
        
        # 获取历史K线数据
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        
        # 获取1分钟和10分钟K线
        kline_1m = futu_api.get_history_kline(stock_code, start_date, end_date, KLType.K_1M)
        kline_10m = futu_api.get_history_kline(stock_code, start_date, end_date, KLType.K_10M)
        
        if kline_1m is None or kline_10m is None:
            logger.warning("[流程图策略] K线数据不足: %s", stock_code)
            return
            
        # 判断是否处于上涨趋势
        if not is_up_trend(kline_10m):
            logger.info("[流程图策略] 不满足上涨趋势条件，跳过 %s", stock_code)
            return
            
        # 判断是否出现反转信号
        if not is_reversal(kline_1m, kline_10m):
            logger.info("[流程图策略] 不满足反转条件，跳过 %s", stock_code)
            return
            
        logger.info("[流程图策略] 触发买入信号: %s", stock_code)
        
        # 获取当前价格
        current_price = None
        quote_time = None
        kline_time = None
        
        # 尝试从订阅数据获取最新价格
        quote_data = get_latest_quote_data()
        if quote_data is not None and 'last_price' in quote_data:
            current_price = quote_data['last_price']
            quote_time = pd.to_datetime(quote_data['svr_recv_time_bid'])
            
        # 从K线数据获取最新价格
        if not kline_1m.empty:
            kline_price = kline_1m['close'].iloc[-1]
            kline_time = kline_1m['time_key'].iloc[-1]
            
            if quote_time is None or quote_time < kline_time:
                current_price = kline_price
                
        if current_price is None:
            logger.info("无订阅数据，使用K线数据作为当前价格: %s", current_price)
            return
            
        logger.info("使用%s数据作为当前价格: %s", '订阅' if quote_time > kline_time else 'K线', current_price)
        
        # 获取期权链
        quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
        option_code, strike_price, qty, lot_size = get_option_to_buy(quote_ctx, stock_code)
        quote_ctx.close()
        
        if option_code is None:
            logger.warning("[流程图策略] 获取期权链失败: %s", stock_code)
            return
            
        # 买入期权
        trade_ctx = OpenSecTradeContext(host='127.0.0.1', port=11111)
        success, buy_price = buy_option(trade_ctx, option_code, qty)
        
        if not success:
            logger.error("[流程图策略] 买入期权失败: %s", option_code)
            return
            
        logger.info("[流程图策略] 选择期权: %s, 行权价: %.2f", option_code, strike_price)
        logger.info("[流程图策略] 成功买入期权: %s", option_code)
        
        # 监控盈亏
        monitor_profit_loss(trade_ctx, option_code, buy_price, qty)
        
    except Exception as e:
        logger.error("[流程图策略] 分析股票 %s 时发生错误: %s", stock_code, str(e), exc_info=True)

def main():
    """主函数"""
    logger = setup_logger()
    logger.info("启动程序...")
    
    # 创建 FutuAPI 实例
    futu_api = FutuAPI()
    
    try:
        while True:
            logger.info("\n" + "="*50)
            logger.info("开始新一轮分析 - %s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            
            # 获取账户资金状况
            logger.info("\n获取账户资金状况...")
            account_funds = futu_api.get_account_funds()
            logger.info("\n账户资金状况:")
            if account_funds is not None and not account_funds.empty:
                logger.info(account_funds.to_string())
            else:
                logger.warning("账户资金数据为空")
            
            # 获取持仓信息
            logger.info("\n获取持仓信息...")
            positions = futu_api.get_positions()
            logger.info("\n当前持仓:")
            if positions is not None and not positions.empty:
                logger.info(positions.to_string())
            else:
                logger.warning("当前持仓为空")
            
            # 分析股票
            stock_list = ['HK.00700']  # 腾讯、阿里巴巴、美团
            for stock_code in stock_list:
                if stock_code == 'HK.00700':
                    monitor_option_chain(stock_code, OptionType.CALL)
                
                # MACD策略
                logger.info("\n[MACD策略] 分析股票: %s", stock_code)
                result_macd = analyze_stock(futu_api, stock_code)
                if result_macd is not None:
                    logger.info("[MACD策略] 当前价格: %s", result_macd['current_price'])
                    logger.info("[MACD策略] 趋势: %s", result_macd['trend'])
                    logger.info("[MACD策略] MACD信号: %s", result_macd['macd_signal'])
                    logger.info("[MACD策略] MA5: %.2f", result_macd['ma5'])
                    logger.info("[MACD策略] MA10: %.2f", result_macd['ma10'])
                    logger.info("[MACD策略] MA20: %.2f", result_macd['ma20'])
                    logger.info("[MACD策略] MACD: %.2f", result_macd['macd'])
                    logger.info("[MACD策略] Signal: %.2f", result_macd['signal'])
                    logger.info("[MACD策略] Hist: %.2f", result_macd['hist'])
                    
                    if result_macd['macd_signal'] == 'BUY':
                        logger.info("[MACD策略] 发现买入信号: %s", stock_code)
                        # order_result = futu_api.place_order(stock_code, 0, 100, TrdSide.BUY, OrderType.MARKET)
                        # logger.info("下单结果: %s", order_result)
                
                # 流程图策略
                logger.info("\n[流程图策略] 分析股票: %s", stock_code)
                result_trend = trend_reversal_strategy(futu_api, stock_code)
                logger.info("[流程图策略] 结果: %s", result_trend)
            
            logger.info("\n等待60秒后进行下一轮分析...")
            time.sleep(60)
            
    except KeyboardInterrupt:
        logger.info("\n程序被用户中断")
    except Exception as e:
        logger.error("主循环发生错误: %s", str(e), exc_info=True)
    finally:
        logger.info("程序结束，清理资源...")
        futu_api.close()

def calculate_ma(data, period):
    """计算移动平均线"""
    return data['close'].rolling(window=period).mean()

def calculate_macd(data, fast=12, slow=26, signal=9):
    """计算MACD指标"""
    exp1 = data['close'].ewm(span=fast, adjust=False).mean()
    exp2 = data['close'].ewm(span=slow, adjust=False).mean()
    macd = exp1 - exp2
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    hist = macd - signal_line
    return macd, signal_line, hist

def analyze_stock(futu_api, stock_code):
    """分析股票"""
    try:
        logger.info("\n开始分析股票 %s", stock_code)
        
        # 获取历史K线数据
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        logger.info("获取历史K线数据: %s 到 %s", 
                   start_date.strftime('%Y-%m-%d'), 
                   end_date.strftime('%Y-%m-%d'))
        
        kline_data = futu_api.get_history_kline(stock_code, start_date, end_date, KLType.K_1M)
        if kline_data is None or kline_data.empty:
            logger.error("无法获取 %s 的历史数据", stock_code)
            return None
            
        logger.info("成功获取历史K线数据，共 %d 条记录", len(kline_data))
        
        # 计算技术指标
        logger.info("计算技术指标...")
        kline_data['ma5'] = calculate_ma(kline_data, 5)
        kline_data['ma10'] = calculate_ma(kline_data, 10)
        kline_data['ma20'] = calculate_ma(kline_data, 20)
        macd, signal, hist = calculate_macd(kline_data)
        kline_data['macd'] = macd
        kline_data['signal'] = signal
        kline_data['hist'] = hist
        
        # 获取实时行情
        logger.info("获取实时行情...")
        quote_data = futu_api.get_stock_quote(stock_code)
        if quote_data is None or quote_data.empty:
            logger.error("无法获取 %s 的实时行情", stock_code)
            return None
            
        logger.info("行情字段: %s", list(quote_data.columns))
        
        # 获取当前价格
        current_price = None
        for price_field in ['last_price', 'cur_price', 'price']:
            if price_field in quote_data.columns:
                current_price = quote_data[price_field].iloc[0]
                break
                
        if current_price is None:
            logger.error("%s 行情数据无可用价格字段: %s", stock_code, list(quote_data.columns))
            return None
            
        # 判断趋势
        trend = 'UP' if current_price > kline_data['ma20'].iloc[-1] else 'DOWN'
        
        # 判断MACD信号
        macd_signal = 'BUY' if (kline_data['macd'].iloc[-1] > kline_data['signal'].iloc[-1] and 
                               kline_data['macd'].iloc[-2] <= kline_data['signal'].iloc[-2]) else 'SELL'
        
        result = {
            'current_price': current_price,
            'trend': trend,
            'macd_signal': macd_signal,
            'ma5': kline_data['ma5'].iloc[-1],
            'ma10': kline_data['ma10'].iloc[-1],
            'ma20': kline_data['ma20'].iloc[-1],
            'macd': kline_data['macd'].iloc[-1],
            'signal': kline_data['signal'].iloc[-1],
            'hist': kline_data['hist'].iloc[-1]
        }
        
        logger.info("分析完成: %s", result)
        return result
        
    except Exception as e:
        logger.error("分析股票 %s 时发生错误: %s", stock_code, str(e), exc_info=True)
        return None

if __name__ == '__main__':
    main()