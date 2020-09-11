import datetime
import time

import os

import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
import numpy as np

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns

# from datafat.engine import rq
from omk.core.vendor.RQData import RQData
from WindPy import w

from datetime import datetime, timedelta

from omk.toolkit.mail import send_email
from omk.toolkit.mail import FUND_ISSUE_LIST, PROD_EMAIL_LIST, DEBUG_MAIL_LIST, EMAIL_TEST, NORTH_GROUP
import rqdatac as rq

RQData.init()


def _local_conn():
    dbinfo = {  # 'user': 'jinxi',
        # 'password': 'omaka8080',
        'user': 'root',
        'password': '123456789',
        'host': '192.168.2.90',
        'port': 3306,
        'database': 'jarvis'}
    engine = create_engine('mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)s/%(database)s' % dbinfo)
    conn = engine.connect()
    return conn, engine


w.start()
conn, engine = _local_conn()


def mkdir(file_path):
    folder = os.path.exists(file_path)
    if not folder:
        try:
            os.mkdir(file_path, mode=777)
        except Exception:
            return False
    else:
        return False
    return True


module_file_path = 'C:\\Users\\Administrator\\SynologyDrive\\south_plot_save'
if mkdir(module_file_path):
    print('模块一级子目录创建成功!')
else:
    print('模块一级子目录创建失败/已经存在!')
# 创建二级子目录
sub_file_path = module_file_path + '\\%s' % datetime.today().date().strftime('%Y-%m-%d')
if mkdir(sub_file_path):
    print('模块二级子目录创建成功!')
else:
    print('模块二级子目录创建失败/已经存在!')

date_end = rq.get_previous_trading_date(datetime.today().date(), 1)
date_start = rq.get_previous_trading_date(date_end, 20)
date = date_end.strftime('%Y-%m-%d')

# 全部
sql = f'''
SELECT * FROM jarvis.southern_cash_flow_infom
WHERE  HDDATE BETWEEN '{date_start}' AND '{date_end}'
'''
southbound_all = pd.read_sql(sql, conn)
southbound_all['date'] = southbound_all['HDDATE'].dt.date
southbound_all['CF'] = southbound_all['ShareHoldSumChg'] * southbound_all['CLOSEPRICE'] / 1e8

southbound_cf = pd.DataFrame(southbound_all,
                             columns=['HDDATE', 'SCODE', 'SNAME', 'ShareHoldSumChg', 'CLOSEPRICE', 'ZDF']).set_index(
    'SNAME')
southbound_cf = southbound_cf[southbound_cf.HDDATE == date]
southbound_cf['CF'] = southbound_cf['ShareHoldSumChg'] * southbound_cf['CLOSEPRICE'] / 1e8
# 查看TOP K家
k = 15
southbound_rank = southbound_cf.sort_values('CF', ascending=False)[:k].append(
    southbound_cf.sort_values('CF', ascending=False)[-k:])
southbound_rank['color'] = (southbound_rank.CF * southbound_rank.ZDF).apply(lambda x: 1 if x > 0 else 0)
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False
sns.set(font_scale=1.5, font='SimHei')
fig, ax = plt.subplots(figsize=(24, 18))
ax1 = ax.twinx()

y = (int(max(southbound_rank['ZDF'].max(), abs(southbound_rank['ZDF'].min())) / 5) + 1) * 5
ylim = (-y, y)

y1 = (int(max(southbound_rank['CF'].max(), abs(southbound_rank['CF'].min())) / 5) + 1) * 5
ylim1 = (-y1, y1)

text_loc = y-3
title_size = 20
txt_size = 14

color = {0: 'orange', 1: 'blue'}
line1 = ax.bar(southbound_rank.index, southbound_rank['ZDF'],
               width=0.7,
               color=[color[0] if i == 0 else color[1] for i in southbound_rank['color']],
               alpha=0.35,
               label='涨跌幅（%）')
line2 = ax1.plot(southbound_rank['CF'], 'ko', label='资金流动净值（亿元）')

for i in range(southbound_rank.shape[0]):
    height = southbound_rank.iloc[i, 5] - 0.7
    num = str(southbound_rank.iloc[i, 5].round(2))
    plt.text(i - 0.3, height, num, size=txt_size)

ax.set_ylabel('涨跌幅（%）', size=txt_size)
ax1.set_ylabel('资金流动净值（亿元）', size=txt_size, )
ax1.legend(loc=0)
ax1.grid(False)

ax.set_ylim(ylim)
ax1.set_ylim(ylim1)
ax.xaxis.set_ticks_position('bottom')
ax.spines['bottom'].set_position(('data', 0))
ax.set_xticklabels(southbound_rank.index,rotation=90, fontsize=15,color ='b' )

plt.axvline(k - 0.5, alpha=0.6)

plt.title('Southbound Cash Flow TOP' + str(k) + ' ' + date, size=title_size)

plt.text((k-4)/2, text_loc,
         'Top '+str(k)+' Inflow: ' +str(southbound_rank['CF'][:k].sum().round(2)) + '亿元\n\n'
         +'Total Inflow: '+str(southbound_cf.query('CF>0')['CF'].sum().round(2))+'亿元',
         size = txt_size+5, alpha = 2,bbox=dict(facecolor='g', edgecolor='blue', alpha=0.35 ))
plt.text((k-4)/2+k, text_loc,
         'Top '+str(k)+' Outflow: ' +str(southbound_rank['CF'][-k:].sum().round(2)) + '亿元\n\n'
         +'Total Outflow: '+str(southbound_cf.query('CF<0')['CF'].sum().round(2))+'亿元',
         size = txt_size+5, alpha = 2,bbox=dict(facecolor='g', edgecolor='blue', alpha=0.35 ))

plt.savefig(sub_file_path + '\\南向资金净流入净流出个股_%s' % date_end.strftime('%Y-%m-%d'))

today = datetime.today().date().strftime('%Y-%m-%d')
attachment_list = [sub_file_path+'\\南向资金净流入净流出个股_%s.png' % date_end]
send_email('%s:南向资金净流入净流出个股' % today,'南向资金净流入净流出个股',images=attachment_list,
           receivers=NORTH_GROUP)
