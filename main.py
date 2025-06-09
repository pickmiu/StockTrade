from futu import *
import pandas as pd
from datetime import datetime, timedelta
import time

def get_change_percentage(kline_data):
    """计算涨跌幅"""
    return ((kline_data['close'] - kline_data['open']) / kline_data['open'] * 100).round(2)

def main():
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    
    try:
        # 计算时间范围
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=10)
        
        # 格式化时间字符串
        end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
        start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
        
        # 获取小米（HK.01810）指定时间范围的K线数据
        ret, data, page_req_key = quote_ctx.request_history_kline('HK.01810', 
            start=start_time_str,
            end=end_time_str, 
            ktype=KLType.K_1M,  # 1分钟K线
            max_count=10,       # 最近10根K线
            fields=[KL_FIELD.ALL],
            extended_time=False)
        
        if ret == RET_OK:
            if len(data) == 0:
                print("\n当前为非交易时段，无法获取数据")
                print("港股交易时间：09:30-12:00, 13:00-16:00（香港时间）")
                return
                
            # 计算涨跌幅
            data['change_percentage'] = get_change_percentage(data)
            
            # 格式化输出
            print(f"\n小米（HK.01810）{start_time_str} 至 {end_time_str} 股价变动：")
            print("=" * 50)
            for _, row in data.iterrows():
                time_str = row['time_key'].split()[1][:5]  # 只取时:分
                print(f"时间: {time_str}")
                print(f"开盘价: {row['open']:.2f}")
                print(f"收盘价: {row['close']:.2f}")
                print(f"涨跌幅: {row['change_percentage']:+.2f}%")
                print("-" * 30)
        else:
            print(f"获取K线数据失败: {data}")
            
    except Exception as e:
        print(f"发生错误: {str(e)}")
    
    finally:
        quote_ctx.close()  # 确保关闭连接

if __name__ == "__main__":
    main()