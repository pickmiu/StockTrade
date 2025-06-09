from futu import *
import pandas as pd
from datetime import datetime, timedelta
import time

def get_change_percentage(kline_data):
    """计算涨跌幅"""
    return ((kline_data['close'] - kline_data['open']) / kline_data['open'] * 100).round(2)

def get_volume_change(kline_data):
    """计算成交量变化"""
    return ((kline_data['volume'] - kline_data['volume'].shift(1)) / kline_data['volume'].shift(1) * 100).round(2)

def fetch_history_kline(stock_code, days=5):
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    try:
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)
        end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
        start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
        ret, data, page_req_key = quote_ctx.request_history_kline(stock_code, 
            start=start_time_str,
            end=end_time_str, 
            ktype=KLType.K_DAY,
            max_count=100,
            fields=[KL_FIELD.ALL],
            extended_time=False)
        if ret == RET_OK and len(data) > 0:
            data['change_percentage'] = get_change_percentage(data)
            data['volume_change'] = get_volume_change(data)
            return data
        else:
            return None
    finally:
        quote_ctx.close()

def print_kline_stats(data, stock_code, start_time_str, end_time_str):
    print(f"\n{stock_code} {start_time_str} 至 {end_time_str} 股价变动：")
    print("=" * 80)
    for _, row in data.iterrows():
        date_str = row['time_key'].split()[0]
        print(f"日期: {date_str}")
        print(f"开盘价: {row['open']:.2f}")
        print(f"最高价: {row['high']:.2f}")
        print(f"最低价: {row['low']:.2f}")
        print(f"收盘价: {row['close']:.2f}")
        print(f"涨跌幅: {row['change_percentage']:+.2f}%")
        print(f"成交量: {row['volume']:,.0f}")
        print(f"成交额: {row['turnover']:,.2f}")
        if not pd.isna(row['volume_change']):
            print(f"成交量变化: {row['volume_change']:+.2f}%")
        print("-" * 40)
    print("\n统计信息：")
    print("=" * 40)
    print(f"平均成交量: {data['volume'].mean():,.0f}")
    print(f"最大成交量: {data['volume'].max():,.0f}")
    print(f"最小成交量: {data['volume'].min():,.0f}")
    print(f"平均成交额: {data['turnover'].mean():,.2f}")
    print(f"最高价: {data['high'].max():.2f}")
    print(f"最低价: {data['low'].min():.2f}")
    print(f"平均涨跌幅: {data['change_percentage'].mean():+.2f}%")

def get_account_list():
    trade_ctx = OpenSecTradeContext(host='127.0.0.1', port=11111)
    try:
        ret, data = trade_ctx.get_acc_list()
        if ret == RET_OK:
            print("\n账户列表：")
            print("=" * 80)
            for _, row in data.iterrows():
                for col in data.columns:
                    print(f"{col}: {row[col]}")
                print("-" * 40)
        else:
            print(f"获取账户列表失败: {data}")
    finally:
        trade_ctx.close()

def get_option_chain(stock_code, option_type=OptionType.CALL, cond_type=OptionCondType.ALL, strike_price=None):
    """
    获取期权链数据
    :param stock_code: 股票代码，如 'HK.09988'
    :param option_type: 期权类型，可选值：
        - OptionType.CALL: 看涨期权
        - OptionType.PUT: 看跌期权
        - OptionType.ALL: 所有期权
    :param cond_type: 期权条件类型，可选值：
        - OptionCondType.WITHIN: 在指定行权价范围内
        - OptionCondType.OUTSIDE: 在指定行权价范围外
        - OptionCondType.ALL: 所有行权价
    :param strike_price: 行权价，当 cond_type 为 WITHIN 或 OUTSIDE 时需要指定
    :return: 期权链数据
    """
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    try:
        print(f"\n正在获取 {stock_code} 的期权链数据...")
        print(f"期权类型: {option_type}")
        print(f"条件类型: {cond_type}")
        if strike_price:
            print(f"行权价: {strike_price}")
        # 显式传递index_option_type参数，避免类型错误
        ret, data = quote_ctx.get_option_chain(stock_code, index_option_type=IndexOptionType.NORMAL, option_type=option_type, option_cond_type=cond_type)
        if ret == RET_OK:
            print(f"\n{stock_code} 期权链数据：")
            print("=" * 80)
            print("字段名:", list(data.columns))
            for _, row in data.iterrows():
                print(f"期权代码: {row['code']}")
                print(f"期权名称: {row['name']}")
                print(f"期权类型: {row['option_type']}")
                print(f"行权价: {row['strike_price']}")
                # 安全打印可选字段
                if 'expiry_date' in data.columns:
                    print(f"到期日: {row['expiry_date']}")
                if 'last_price' in data.columns:
                    print(f"最新价: {row['last_price']}")
                if 'volume' in data.columns:
                    print(f"成交量: {row['volume']}")
                if 'open_interest' in data.columns:
                    print(f"持仓量: {row['open_interest']}")
                print("-" * 40)
            return data
        else:
            print(f"获取期权链数据失败: {data}")
            return None
    except Exception as e:
        print(f"发生错误: {str(e)}")
        return None
    finally:
        quote_ctx.close()

def monitor_option_chain(stock_code, interval=5, option_type=OptionType.ALL):
    """
    持续监控期权链数据
    :param stock_code: 股票代码，如 'HK.09988'
    :param interval: 刷新间隔（秒）
    :param option_type: 期权类型，可选值：
        - OptionType.CALL: 看涨期权
        - OptionType.PUT: 看跌期权
        - OptionType.ALL: 所有期权
    """
    print(f"开始监控 {stock_code} 的期权链数据，刷新间隔 {interval} 秒...")
    print(f"期权类型: {option_type}")
    try:
        while True:
            data = get_option_chain(stock_code, option_type)
            if data is not None:
                print(f"\n更新时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n停止监控")
    except Exception as e:
        print(f"监控过程中发生错误: {str(e)}") 