import requests
import json
from datetime import datetime,timedelta
import os

from omk.toolkit.mail import send_email
from omk.toolkit.mail import FUND_ISSUE_LIST, PROD_EMAIL_LIST, DEBUG_MAIL_LIST, EMAIL_TEST,NORTH_GROUP

import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
import numpy as np

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns

import rqdatac as rq
from WindPy import w

from omk.core.vendor.RQData import RQData
RQData.init()
w.start()

# 沪股通10大
def get_top_ten_of_sh(date=None):
    if date is None:
        raise ValueError('Data cannot be None!')
    date = pd.to_datetime(date).strftime('%Y-%m-%d')
    cookies = {
        'intellpositionL': '617.2px',
        'st_si': '73375257205011',
        'cowminicookie': 'true',
        'waptgshowtime': '2020824',
        'cowCookie': 'true',
        'qgqp_b_id': '297950ac21997d0770ff1c36c30dc9f7',
        'st_asi': 'delete',
        'st_pvi': '43322278274401',
        'st_inirUrl': 'http%3A%2F%2Fdata.eastmoney.com%2Fhsgtcg%2FStockStatistics.aspx',
        'st_sn': '245',
        'st_psi': '20200824130719478-113300300994-2968478229',
        'intellpositionT': '1079px',
    }

    headers = {
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36',
        'Accept': '*/*',
        'Referer': 'http://data.eastmoney.com/hsgt/top10.html',
        'Accept-Language': 'zh-CN,zh;q=0.9',
    }

    params = (
        ('type', 'HSGTCJB'),
        ('token', '70f12f2f4f091e459a279469fe49eca5'),
        ('sty', 'hgt'),
        ('filter', '(MarketType=1)(DetailDate=^%s^)' % date),
        ('js', '{"data":(x)}'),
        ('pagesize', '50'),
        ('page', '1'),
        ('sr', '-1'),
        ('st', 'HGTCJJE'),
        ('rt', '53274854'),
    )

    try:
        print('开始抓取沪股通%s日期下十大北向持股!' % date)
        response_sh = requests.get('http://dcfm.eastmoney.com//EM_MutiSvcExpandInterface/api/js/get', headers=headers,
                                   params=params, cookies=cookies, verify=False)
        temp = json.loads(response_sh.text)['data']
        final_data = pd.DataFrame()
        for data in temp:
            final_data = pd.concat([final_data, pd.DataFrame([data])])
    except requests.ConnectionError:
        print('URL无响应,尝试重新抓取数据!')
        while True:
            response_sh = requests.get('http://dcfm.eastmoney.com//EM_MutiSvcExpandInterface/api/js/get',
                                       headers=headers,
                                       params=params, cookies=cookies, verify=False)
            if response_sh.status_code == 200:
                break
        temp = json.loads(response_sh.text)['data']
        final_data = pd.DataFrame()
        for data in temp:
            final_data = pd.concat([final_data, pd.DataFrame([data])])
    print('%s日期数据抓取结束!' % date)
    return final_data

def get_top_ten_of_sz(date=None):
    if date is None:
        raise ValueError('Data cannot be None!')
    date = pd.to_datetime(date).strftime('%Y-%m-%d')
    cookies = {
        'intellpositionL': '617.2px',
        'st_si': '73375257205011',
        'cowminicookie': 'true',
        'waptgshowtime': '2020824',
        'cowCookie': 'true',
        'qgqp_b_id': '297950ac21997d0770ff1c36c30dc9f7',
        'st_asi': 'delete',
        'intellpositionT': '1079px',
        'st_pvi': '43322278274401',
        'st_sp': '2020-08-20%2014%3A30%3A53',
        'st_inirUrl': 'http%3A%2F%2Fdata.eastmoney.com%2Fhsgtcg%2FStockStatistics.aspx',
        'st_sn': '246',
        'st_psi': '2020082413072652-113300300994-8191285993',
    }

    headers = {
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36',
        'Accept': '*/*',
        'Referer': 'http://data.eastmoney.com/hsgt/top10.html',
        'Accept-Language': 'zh-CN,zh;q=0.9',
    }

    params = (
        ('type', 'HSGTCJB'),
        ('token', '70f12f2f4f091e459a279469fe49eca5'),
        ('sty', 'sgt'),
        ('filter', '(MarketType=3)(DetailDate=^%s^)' % date),
        ('js', '{"data":(x)}'),
        ('pagesize', '50'),
        ('page', '1'),
        ('sr', '-1'),
        ('st', 'SGTJME'),
        ('rt', '53274858'),
    )
    try:
        print('开始抓取深股通%s日期下十大北向持股!' % date)
        response_sz = requests.get('http://dcfm.eastmoney.com//EM_MutiSvcExpandInterface/api/js/get', headers=headers,
                                   params=params, cookies=cookies, verify=False)
        temp = json.loads(response_sz.text)['data']
        final_data = pd.DataFrame()
        for data in temp:
            final_data = pd.concat([final_data, pd.DataFrame([data])])
    except requests.ConnectionError:
        print('URL无响应,尝试重新抓取数据!')
        while True:
            response_sz = requests.get('http://dcfm.eastmoney.com//EM_MutiSvcExpandInterface/api/js/get',
                                       headers=headers,
                                       params=params, cookies=cookies, verify=False)
            if response_sz.status_code == 200:
                break
        temp = json.loads(response_sz.text)['data']
        final_data = pd.DataFrame()
        for data in temp:
            final_data = pd.concat([final_data, pd.DataFrame([data])])
    print('%s日期数据抓取结束!' % date)
    return final_data


def mkdir(file_path):
    folder=os.path.exists(file_path)
    if not folder:
        try:
            os.mkdir(file_path,mode=777)
        except Exception:
            return False
    else:
        return False
    return True

# 根据时间和模块名字创建当日子文件夹
module_file_path='C:\\Users\\omaka\\SynologyDrive\\north_plot_save\\north_act'
if mkdir(module_file_path):
    print('模块一级子目录创建成功!')
else:
    print('模块一级子目录创建失败/已经存在!')
# 创建二级子目录
sub_file_path=module_file_path+'\\%s' % datetime.today().date().strftime('%Y-%m-%d')
if mkdir(sub_file_path):
    print('模块二级子目录创建成功!')
else:
    print('模块二级子目录创建失败/已经存在')

def _local_conn():
    dbinfo = {#'user': 'jinxi',
              #'password': 'omaka8080',
              'user': 'hpqin_omk',
              'password': 'ufhixm6omc+m7svramma',
              'host': 'sh-cdb-oarey71m.sql.tencentcdb.com',
              'port': 60993,
              'database': 'jarvis'}
    engine = create_engine('mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)s/%(database)s' % dbinfo)
    conn = engine.connect()
    return conn
w.start()
conn = _local_conn()
date_end = rq.get_previous_trading_date(datetime.today().date(), 1)
date_start = rq.get_previous_trading_date(date_end, 20)
date = date_end.strftime('%Y-%m-%d')

sql = f'''
SELECT * FROM jarvis.lgt_inst_north_cap_em
JOIN jarvis.lgt_inst_info_em
USING (inst_code)
WHERE as_of_date = '{date_end}'
ORDER by stock_cap DESC
'''
info = pd.read_sql(sql, conn)
inst_code = tuple(info[:20]['inst_code'])
inst_code_all = tuple(info['inst_code'])

sql = f'''
SELECT * FROM jarvis.lgt_inst_north_holding_em
WHERE inst_code in {str(inst_code)}
AND as_of_date BETWEEN '{date_start}' AND '{date_end}'
'''
holding = pd.read_sql(sql, conn)
holding['stock_code'] = holding['stock_code'].apply(rq.id_convert)

#全部
sql = f'''
SELECT * FROM jarvis.lgt_inst_north_holding_em
WHERE inst_code in {str(inst_code_all)}
AND as_of_date BETWEEN '{date_start}' AND '{date_end}'
'''
holding_all = pd.read_sql(sql, conn)
holding_all['stock_code'] = holding_all['stock_code'].apply(rq.id_convert)

#所有股票代码
instruments = holding['stock_code'].drop_duplicates().sort_values().to_list()
#历史涨跌幅
pct_change = rq.get_price_change_rate(instruments, start_date=date_start, end_date=date_end)
#获取历史价格
md = rq.get_price(instruments, start_date=date_start, end_date=date_end, fields=['close'])

act_vol_sh_rank = get_top_ten_of_sh(date).drop(['MarketType', 'Rank1','HGTJME', 'HGTMRJE', 'HGTMCJE', 'HGTCJJE'],axis =1).sort_values('Rank',ascending = True)
act_vol_sh_rank['Code'] = act_vol_sh_rank['Code'].apply(rq.id_convert)
act_vol_sz_rank = get_top_ten_of_sz(date).drop(['MarketType', 'Rank1', 'SGTJME', 'SGTMRJE', 'SGTMCJE', 'SGTCJJE'],axis =1).sort_values('Rank',ascending = True)
act_vol_sz_rank['Code'] = act_vol_sz_rank['Code'].apply(rq.id_convert)
act_top = act_vol_sh_rank.append(act_vol_sz_rank).set_index('Code')
act_top=act_top[['DetailDate', 'Rank', 'Name', 'Close', 'ChangePercent']]

#绘图
t_lim = 0
b_lim = 0
# top 5家
k = 5
fig = plt.figure(figsize = (60, 60))
# axs_all =[]
rowNum = 4
colNum = 5

for j in range(20):
    act_top_stk = holding_all[holding_all.stock_code == act_top.index[j]]
    # 计算cashflow(千万元），每只股票对应一个pivot图
    act_stock_holding_vol = act_top_stk.pivot('as_of_date', 'inst_code', 'holding_vol')
    act_stock_holding_vol = act_stock_holding_vol.reindex(md.index).fillna(method = 'ffill')
    act_stock_holding_mv = (act_stock_holding_vol.mul(md.loc[:, act_top.index[j]], axis = 0).fillna(0))

    # 特定股票的cf表 日期//instc_code
    cash_flow_act_stock = (act_stock_holding_mv.diff() - (
        act_stock_holding_mv.shift().mul(pct_change.loc[:, act_top.index[j]], axis = 0))) / 1e8

    # 按照最后一天的资金排序
    inst_stock_rank_abs = cash_flow_act_stock.iloc[-1, :].abs().sort_values(ascending = False)
    cash_flow_act_stock['total'] = cash_flow_act_stock.apply(lambda x: x.sum(), axis = 1)

    a = []
    b = []
    c = []
    for i in range(k):
        a.append(inst_stock_rank_abs.index[i])
        b.append(i + 1)

    for i in a:
        c.append(info.query('inst_code == @i')['inst_name'].iloc[0])
    top_inst_stock = pd.DataFrame({'inst_code': a, 'inst_name': c, 'rank': b}).set_index('inst_code')

    temp = cash_flow_act_stock.T
    temp.columns = temp.columns.strftime('%Y-%m-%d')
    summary = temp.reindex(top_inst_stock.index).iloc[:, -1:]
    summary['inst_name'] = top_inst_stock['inst_name']
    summary.loc['total'] = temp.loc['total']
    summary.set_index(['inst_name'], inplace = True)
    summary = summary.T

    # 设置绘图上下限
    if summary.max(axis = 1)[0] > 10:
        t_10 = summary.max(axis = 1)[0]
    elif summary.max(axis = 1)[0] > 5:
        t_5 = summary.max(axis = 1)[0]
    elif summary.max(axis = 1)[0] > t_lim:
        t_lim = summary.max(axis = 1)[0]
    else:
        t_lim = t_lim

    if summary.min(axis = 1)[0] < -10:
        b_10 = summary.min(axis = 1)[0]
    elif summary.min(axis = 1)[0] < -5:
        b_5 = summary.min(axis = 1)[0]
    elif summary.min(axis = 1)[0] < b_lim:
        b_lim = summary.min(axis = 1)[0]
    else:
        b_lim = b_lim

fig = plt.figure(figsize = (60, 50))
rowNum = 4
colNum = 5

for j in range(20):
    act_top_stk = holding_all[holding_all.stock_code == act_top.index[j]]
    # 计算cashflow(千万元），每只股票对应一个pivot图
    act_stock_holding_vol = act_top_stk.pivot('as_of_date', 'inst_code', 'holding_vol')
    act_stock_holding_vol = act_stock_holding_vol.reindex(md.index).fillna(method = 'ffill')
    act_stock_holding_mv = (act_stock_holding_vol.mul(md.loc[:, act_top.index[j]], axis = 0).fillna(0))

    # 特定股票的cf表 日期//instc_code
    cash_flow_act_stock = (act_stock_holding_mv.diff() - (
        act_stock_holding_mv.shift().mul(pct_change.loc[:, act_top.index[j]], axis = 0))) / 1e8

    # 按照最后一天的资金排序
    inst_stock_rank_abs = cash_flow_act_stock.iloc[-1, :].abs().sort_values(ascending = False)
    cash_flow_act_stock['total'] = cash_flow_act_stock.apply(lambda x: x.sum(), axis = 1)

    a = []
    b = []
    c = []
    for i in range(k):
        a.append(inst_stock_rank_abs.index[i])
        b.append(i + 1)

    for i in a:
        c.append(info.query('inst_code == @i')['inst_name'].iloc[0])
    top_inst_stock = pd.DataFrame({'inst_code': a, 'inst_name': c, 'rank': b}).set_index('inst_code')

    temp = cash_flow_act_stock.T
    temp.columns = temp.columns.strftime('%Y-%m-%d')
    summary = temp.reindex(top_inst_stock.index).iloc[:, -1:]
    summary['inst_name'] = top_inst_stock['inst_name']
    summary.loc['total'] = temp.loc['total']
    summary.set_index(['inst_name'], inplace = True)
    summary = summary.T

    if summary.max(axis = 1)[0] > 10:
        t_limit = t_10 + 0.5
    elif summary.max(axis = 1)[0] > 5:
        t_limit = t_5 + 0.5
    else:
        t_limit = t_lim + 0.5

    if summary.min(axis = 1)[0] < -10:
        b_limit = b_10 - 0.5
    elif summary.min(axis = 1)[0] < -5:
        b_limit = b_5 - 0.5
    else:
        b_limit = b_lim - 0.5

        # 绘图
    plt.rcParams['axes.unicode_minus'] = False
    sns.set(font_scale = 1.5, font = 'SimHei')

    ax = fig.add_subplot(rowNum, colNum, j + 1)
    summary.iloc[:, :-1].plot(kind = 'bar', stacked = True, legend = False, ax = ax, sharex = True, width = 0.3)
    plt.legend(loc = 'lower right', fontsize = 14)
    ax2 = ax.twinx()
    summary.iloc[:, -1].plot(kind = 'bar', legend = False, ax = ax2, edgecolor = 'r', linewidth = 3, facecolor = 'none')
    ylim = (b_limit, t_limit)
    ax.set_ylim(ylim)
    ax2.set_ylim(ylim)
    ax2.axis(False)
    ax2.grid(False)
    plt.gcf().autofmt_xdate()
    plt.title(act_top.index[j] + ' ' + act_top.iloc[j, 2] + ' (' + '%.2f%%' % act_top.iloc[j, -1].round(2) + ')',
              size = 25)
fig.suptitle(date + ' 活跃个股TOP10（按市场）', fontsize = 50)
plt.savefig(sub_file_path+'\\%s' % '活跃个股TOP10(按市场)')

today = datetime.today().date().strftime('%Y-%m-%d')
attachment_list=sub_file_path+'\\活跃个股TOP10(按市场).png'
send_email('%s:北向资金成交活跃个股TOP10' % today, '成交活跃个股TOP10',
           images=attachment_list,
           receivers=NORTH_GROUP)