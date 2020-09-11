import requests
import json
import re
import pandas as pd
import numpy as np
from datetime import datetime
import time


def define_basic_infom():
    cookies = {
        'qgqp_b_id': '297950ac21997d0770ff1c36c30dc9f7',
        '_qddaz': 'QD.vsea88.n8e9al.ke89kfpo',
        'pgv_pvi': '5407696896',
        'cowCookie': 'true',
        'st_si': '28335634614582',
        'cowminicookie': 'true',
        'st_asi': 'delete',
        'intellpositionL': '483px',
        'st_pvi': '43322278274401',
        'st_sp': '2020-08-20%2014%3A30%3A53',
        'st_inirUrl': 'http%3A%2F%2Fdata.eastmoney.com%2Fhsgtcg%2FStockStatistics.aspx',
        'st_sn': '10',
        'st_psi': '20200909094128813-113300303606-1318002988',
        'intellpositionT': '973.545px',
    }

    headers = {
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36',
        'Accept': '*/*',
        'Referer': 'http://data.eastmoney.com/hsgt/index.html',
        'Accept-Language': 'zh-CN,zh;q=0.9',
    }

    params = (
        ('fields1', 'f1,f2,f3,f4'),
        ('fields2', 'f51,f52,f53,f54,f55,f56'),
        ('ut', 'b2884a393a59ad64002292a3e90d46a5'),
        ('cb', 'jQuery183021065377894684523_1599615610093'),
        ('_', '1599616390274'),
    )
    return cookies, headers, params


def data_graping_solving():
    cookies, headers, params = define_basic_infom()
    try:
        response = requests.get('http://push2.eastmoney.com/api/qt/kamt.rtmin/get', headers=headers, params=params,
                                cookies=cookies, verify=False)
    except requests.exceptions.ConnectionError:
        while True:
            response = requests.get('http://push2.eastmoney.com/api/qt/kamt.rtmin/get', headers=headers, params=params,
                                    cookies=cookies, verify=False)
            if response.status_code == 200:
                break
    # 获取南向分钟数据
    pattern = re.compile(r'"data":[{](.*?)[}]', re.S)
    data = pattern.findall(response.text)[0]
    data = data.split(']')[0]  # 获取分割后的第一组北向资金数据
    data = [x.replace('"', '').replace('[', '') for x in data.split('"s2n":')[-1].split('",')]
    dataframe = pd.DataFrame([x.split(',')[1:] for x in data],
                             index=[datetime.today().strftime('%Y-%m-%d') + ' ' + x.split(',')[0] for x in data],
                             columns=['HK2SH', 'HK2SH_Remained_CF', 'HK2SZ', 'HK2SZ_Remained_CF', 'WHOLE_NORTHERN_CF'])
    dataframe.index = pd.to_datetime(dataframe.index)
    return dataframe.replace('-', np.nan).dropna()


def circylly_grabing_northern_data(old_dataframe):
    # 除去空白数据-->如果程序为每分钟读取一次数据，则采取此行为
    # 如果程序为每天仅抓取一次，则无任何
    if old_dataframe is not None:
        dataframe = data_graping_solving()
        dataframe=dataframe.replace('-', np.nan)
        dataframe.dropna(inplace=True)
        old_dataframe.dropna(inplace=True)
        old_dataframe = old_dataframe.append(
            dataframe.loc[[x for x in dataframe.index if x not in old_dataframe.index], :])
        return old_dataframe
    else:
        raise ValueError('Original Data is empty!')


def save_to_database(dataframe, update=False):
    pass


def t_clock():
    import time
    while True:
        s = time.asctime()
        print(s[11:19], end='')
        time.sleep(1)
        print('\r', end="", flush=True)


if __name__ == "__main__":
    old_dataframe = data_graping_solving()
    i = 1
    while True:
        if datetime.today().hour == 15 and datetime.today().minute == 00:
            break
        s = time.asctime()
        print('数据第%d次更新时间是:' % i, s[11:19])
        time.sleep(70)
        old_dataframe = circylly_grabing_northern_data(old_dataframe)
        print(old_dataframe)
        i+=1
