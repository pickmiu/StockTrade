import sys
from futu import *
import pandas as pd
from datetime import datetime, timedelta
import time
from futu_utils import fetch_history_kline, print_kline_stats, get_account_list, get_option_chain, monitor_option_chain
import argparse
from futu import OptionType
import socket

def check_opend_connection():
    """检查OpenD服务是否可连接"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('127.0.0.1', 11111))
    sock.close()
    return result == 0

def get_change_percentage(kline_data):
    """计算涨跌幅"""
    return ((kline_data['close'] - kline_data['open']) / kline_data['open'] * 100).round(2)

def get_volume_change(kline_data):
    """计算成交量变化"""
    return ((kline_data['volume'] - kline_data['volume'].shift(1)) / kline_data['volume'].shift(1) * 100).round(2)

def get_account_list(trade_ctx):
    """获取账户列表"""
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

def main():
    # 检查OpenD服务是否可连接
    if not check_opend_connection():
        print("错误: 无法连接到富途OpenD服务（127.0.0.1:11111）")
        print("请确保：")
        print("1. 已安装富途牛牛客户端")
        print("2. 已登录富途牛牛客户端")
        print("3. OpenD服务已启动")
        return

    # 创建交易上下文
    trade_ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.HK, host='127.0.0.1', port=11111)
    if not trade_ctx:
        print("错误: 无法创建交易上下文")
        return

    parser = argparse.ArgumentParser(description='富途API工具')
    parser.add_argument('--mode', type=str, required=True, choices=['kline', 'account', 'option', 'monitor'],
                      help='运行模式: kline(获取K线数据), account(获取账户信息), option(获取期权链), monitor(监控期权链)')
    parser.add_argument('--code', type=str, help='股票代码，例如: HK.09988')
    parser.add_argument('--days', type=int, default=5, help='获取K线数据的天数')
    parser.add_argument('--interval', type=int, default=5, help='监控期权链的刷新间隔（秒）')
    parser.add_argument('--option-type', type=str, choices=['call', 'put', 'all'], default='call',
                      help='期权类型: call(看涨期权), put(看跌期权), all(所有期权)')
    
    args = parser.parse_args()
    
    # 转换期权类型参数
    option_type_map = {
        'call': OptionType.CALL,
        'put': OptionType.PUT,
        'all': OptionType.ALL
    }
    option_type = option_type_map[args.option_type]
    
    if args.mode == 'kline':
        if not args.code:
            print("错误: 获取K线数据需要指定股票代码")
            return
        data = fetch_history_kline(args.code, args.days)
        if data is not None:
            end_time = datetime.now()
            start_time = end_time - timedelta(days=args.days)
            print_kline_stats(data, args.code, start_time.strftime('%Y-%m-%d'), end_time.strftime('%Y-%m-%d'))
    
    elif args.mode == 'account':
        get_account_list(trade_ctx)
    
    elif args.mode == 'option':
        if not args.code:
            print("错误: 获取期权链需要指定股票代码")
            return
        get_option_chain(args.code, option_type)
    
    elif args.mode == 'monitor':
        if not args.code:
            print("错误: 监控期权链需要指定股票代码")
            return
        monitor_option_chain(args.code, args.interval, option_type)

    # 关闭交易上下文
    trade_ctx.close()

if __name__ == '__main__':
    main()