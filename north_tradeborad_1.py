from datetime import datetime,timedelta

import os
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
import numpy as np

from omk.toolkit.mail import send_email
from omk.toolkit.mail import FUND_ISSUE_LIST, PROD_EMAIL_LIST, DEBUG_MAIL_LIST, EMAIL_TEST,NORTH_GROUP

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns

#from datafat.engine import rq
import rqdatac as rq
from WindPy import w

from omk.core.vendor.RQData import RQData
# username = "license"
# #password = "BJ9NjozEuioXiqB01KCkVzgWj8TojhnSLo3_PifT_Ep7oJAU8P_8U9Zrc5ezfFS6T674dBgiYO8eKzxw4TX70WSU9UdyqwsZ3Ypjer1n9yiYTAIjwTkJbJZdQ2PUw9Wm-eCsqyEpt9QqydMaWRhsF3f4c1xIyxVxtZwbumCmk1s=iQSeCekd_kkCEQqDdVZUflt2K2xpFLFvGgnbGF15Et0MrJ9iDl_uLE4g2fE0etfHH05Kli3pEqz-geoI4jxYs4AG1pkwcEY5ipReSQP_ehcbX0sRFw_haN2UmRMz-4a2Da3Ot18ZBhB5xPO1WX8efv4GZxgQlLuGC2t-_YliIis="
# password = "O9gRNWpuHPB462Gs2vmPC09-8NgMwJTppO7XbXIhYtWNetKOOxpsxDlLTO7w7Vdp12nf926XR4xnVfC1ecfj4KOKXc9oVXnUxhPhqe3_V1N6cVor7wgQhX2J8WOl6V7-7ac5oE0n0BRzTHJOL3Wjf2EdFWpBFgh76_z4NGMIdBQ=dQ_nVmEGyBLZ97FuSiU32IUks0CSpzB3hqDihlHhpEjkunWJ4YPjrBr6aRB8WbUBUjAxgcah1u9ZmIQzc8rvgVB3o1nLw5VDC8RX0h3vECcN5QvCXaq7TygeQmdOPMvKyKRP2xpOpCdIxSjNIyLUx4zd67TMh0XF28JqnlM7hQU="
# host= "rqdatad-pro.ricequant.com"
# port = "16011"
# rq.init(username, password, (host, port))
RQData.init()


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
module_file_path='C:\\Users\\omaka\\SynologyDrive\\north_plot_save\\north_tradeboard'
if mkdir(module_file_path):
    print('模块一级子目录创建成功!')
else:
    print('模块一级子目录创建失败/已经存在!')
# 创建二级子目录
sub_file_path=module_file_path+'\\%s' % datetime.today().date().strftime('%Y-%m-%d')
if mkdir(sub_file_path):
    print('模块二级子目录创建成功!')
else:
    print('模块二级子目录创建失败/已经存在!')

# 设定时间
date_end=rq.get_previous_trading_date(datetime.today().date(), 1)
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

sql = f'''
SELECT * FROM jarvis.lgt_inst_north_holding_em
WHERE inst_code in {str(inst_code)}
AND as_of_date BETWEEN '{date_start}' AND '{date_end}'
'''
holding = pd.read_sql(sql, conn)
holding['stock_code'] = holding['stock_code'].apply(rq.id_convert)

#所有股票代码
instruments = holding['stock_code'].drop_duplicates().sort_values().to_list()
#历史涨跌幅
pct_change = rq.get_price_change_rate(instruments, start_date=date_start, end_date=date_end)
#获取历史价格
md = rq.get_price(instruments, start_date=date_start, end_date=date_end, fields=['close'])

cash_flow_sz = pd.DataFrame()
cash_flow_sh = pd.DataFrame()

# stock_code = '600519.XSHG'
for inst in inst_code:
    holding_vol = holding.query('inst_code == @inst').pivot('as_of_date', 'stock_code', 'holding_vol')
    inst_name = info.query('inst_code == @inst')['inst_name'].iloc[0]

    # 补充缺失数据，仅外资银行可用
    holding_vol = holding_vol.reindex(md.index).fillna(method='ffill')
    # 持仓市值
    holding_mv = (holding_vol * md).fillna(0)
    cash_flow_stock = (holding_mv.diff() - (holding_mv.shift() * pct_change)) / 1e8
    cash_flow_sz[inst_name] = cash_flow_stock.loc[:, cash_flow_stock.columns.str[-4:] == 'XSHE'].sum(axis=1)
    cash_flow_sh[inst_name] = cash_flow_stock.loc[:, cash_flow_stock.columns.str[-4:] == 'XSHG'].sum(axis=1)
cash_flow_sz.index = cash_flow_sz.index.date
cash_flow_sh.index = cash_flow_sh.index.date
cash_flow = cash_flow_sh + cash_flow_sz

temp = w.wset("shhktransactionstatistics",f"startdate={date_start};enddate={date_end};cycle=day;currency=cny;field=date,sh_net_purchases")
hk_to_sh = pd.DataFrame(temp.Data, index=temp.Fields).T.set_index('date')
temp = w.wset("szhktransactionstatistics",f"startdate={date_start};enddate={date_end};cycle=day;currency=cny;field=date,sz_net_purchases")
hk_to_sz = pd.DataFrame(temp.Data, index=temp.Fields).T.set_index('date')
hk_to_all = hk_to_sh.join(hk_to_sz).sort_index()
hk_to_all.index = hk_to_all.index.date
hk_to_all_total = hk_to_all.sum(axis=1)

#### 总体

plt.rcParams['axes.unicode_minus'] = False
sns.set(font_scale=1.5, font='SimHei')

ax=cash_flow.iloc[:, :10].plot(kind='bar', stacked=True, figsize=(30,20), legend=True)
plt.legend(bbox_to_anchor=(0.5, -0.15), loc='upper center', ncol=3)
ax2 = ax.twinx()
hk_to_all.sum(axis=1).plot(kind='bar', legend=False, ax=ax2, edgecolor='r', linewidth=3, facecolor='none')

ylim = (min(ax.get_ylim()[0], ax2.get_ylim()[0]), max(ax.get_ylim()[1], ax2.get_ylim()[1]))
ax.set_ylim(ylim)
ax2.set_ylim(ylim)
ax2.grid(False)
plt.gcf().autofmt_xdate()
plt.title(f'Northbound Cash Flow 1-10, {cash_flow.index[-1]:%Y-%m-%d}')
plt.savefig(sub_file_path+'\\%s.png' % f'NorthboundCashFlow1-10_{cash_flow.index[-1]:%Y-%m-%d}')

ax3 = cash_flow.iloc[:, 10:].plot(kind='bar', stacked=True, figsize=(30,20), legend=True)
ax3.set_ylim(ax.get_ylim())
plt.legend(bbox_to_anchor=(0.5, -0.15), loc='upper center', ncol=3)
ax4 = ax3.twinx()
hk_to_all.sum(axis=1).plot(kind='bar', legend=False, ax=ax4, edgecolor='r', linewidth=3, facecolor='none')

ax3.set_ylim(ylim)
ax4.set_ylim(ylim)
ax4.grid(False)
plt.gcf().autofmt_xdate()
plt.title(f'Northbound Cash Flow 11-20, {cash_flow.index[-1]:%Y-%m-%d}')
plt.savefig(sub_file_path+'\\%s.png'%f'NorthboundCashFlow11-20_{cash_flow.index[-1]:%Y-%m-%d}')


#### 沪市

plt.rcParams['axes.unicode_minus'] = False
sns.set(font_scale=1.5, font='SimHei')

fig_, ax_ = plt.subplots(figsize=(30, 20))
cash_flow_sh.iloc[:, :10].plot(kind='bar', stacked=True, figsize=(30, 20), legend=True, ax=ax_)
# fig=cash_flow_sh.iloc[:, 10:].plot(kind='bar', stacked=True, figsize=(20, 10), legend=True).get_figure()
ax_.set_ylim(ax.get_ylim())
plt.legend(bbox_to_anchor=(0.5, -0.15), loc='upper center', ncol=3)
ax__ = ax_.twinx()
hk_to_all['sh_net_purchases'].plot(kind='bar', legend=False, ax=ax__, edgecolor='r', linewidth=3, facecolor='none')

ax_.set_ylim(ylim)
ax__.set_ylim(ylim)
ax__.grid(False)
plt.gcf().autofmt_xdate()
plt.title(f'SH: Northbound Cash Flow 1-10, {cash_flow.index[-1]:%Y-%m-%d}')
fig_.savefig(sub_file_path+'\\%s.png' % f'SHNorthboundCashFlow11-20_{cash_flow.index[-1]:%Y-%m-%d}')



fig_, ax_ = plt.subplots(figsize=(30, 20))
cash_flow_sh.iloc[:, 10:].plot(kind='bar', stacked=True, figsize=(30, 20), legend=True, ax=ax_)
# fig=cash_flow_sh.iloc[:, 10:].plot(kind='bar', stacked=True, figsize=(20, 10), legend=True).get_figure()
ax_.set_ylim(ax.get_ylim())
plt.legend(bbox_to_anchor=(0.5, -0.15), loc='upper center', ncol=3)
ax__ = ax_.twinx()
hk_to_all['sh_net_purchases'].plot(kind='bar', legend=False, ax=ax__, edgecolor='r', linewidth=3, facecolor='none')

ax_.set_ylim(ylim)
ax__.set_ylim(ylim)
ax__.grid(False)
plt.gcf().autofmt_xdate()
plt.title(f'SH: Northbound Cash Flow 11-20, {cash_flow.index[-1]:%Y-%m-%d}')
fig_.savefig(sub_file_path+'\\%s.png' % f'SHNorthboundCashFlow1-10_{cash_flow.index[-1]:%Y-%m-%d}')




#### 深市
plt.rcParams['axes.unicode_minus'] = False

sns.set(font_scale=1.5, font='SimHei')
fig_, ax_ = plt.subplots(figsize=(30, 20))
cash_flow_sz.iloc[:, :10].plot(kind='bar', stacked=True, figsize=(30, 20), legend=True, ax=ax_)
# fig=cash_flow_sh.iloc[:, 10:].plot(kind='bar', stacked=True, figsize=(20, 10), legend=True).get_figure()
ax_.set_ylim(ax.get_ylim())
plt.legend(bbox_to_anchor=(0.5, -0.15), loc='upper center', ncol=3)
ax__ = ax_.twinx()
hk_to_all['sz_net_purchases'].plot(kind='bar', legend=False, ax=ax__, edgecolor='r', linewidth=3, facecolor='none')

ax_.set_ylim(ylim)
ax__.set_ylim(ylim)
ax__.grid(False)
plt.gcf().autofmt_xdate()
plt.title(f'SZ: Northbound Cash Flow 1-10, {cash_flow.index[-1]:%Y-%m-%d}')
plt.savefig(sub_file_path+'\\%s.png' % f'SZNorthboundCashFlow1-10_{cash_flow.index[-1]:%Y-%m-%d}')


fig_, ax_ = plt.subplots(figsize=(30, 20))
cash_flow_sz.iloc[:, 10:].plot(kind='bar', stacked=True, figsize=(30, 20), legend=True, ax=ax_)
# fig=cash_flow_sh.iloc[:, 10:].plot(kind='bar', stacked=True, figsize=(20, 10), legend=True).get_figure()
ax_.set_ylim(ax.get_ylim())
plt.legend(bbox_to_anchor=(0.5, -0.15), loc='upper center', ncol=3)
ax__ = ax_.twinx()
hk_to_all['sz_net_purchases'].plot(kind='bar', legend=False, ax=ax__, edgecolor='r', linewidth=3, facecolor='none')

ax_.set_ylim(ylim)
ax__.set_ylim(ylim)
ax__.grid(False)
plt.gcf().autofmt_xdate()
plt.title(f'SZ: Northbound Cash Flow 11-20, {cash_flow.index[-1]:%Y-%m-%d}')
plt.savefig(sub_file_path+'\\%s.png' % f'SZNorthboundCashFlow11-20_{cash_flow.index[-1]:%Y-%m-%d}')


#### 两室整体比较
sns.set(font_scale=1.5)
fig = plt.figure(figsize=(20, 10))
ax = plt.subplot(211)
hk_to_all.sum(axis=1).plot(kind='bar', legend=False, ax=ax)
ax2 = ax.twinx()
ax2.set_ylim(ax.get_ylim())
plt.grid(False)
ax.set_title(f'Northbound Cash Flow, {hk_to_all.index[-1]:%Y-%m-%d}')

ax3 = plt.subplot(212)
hk_to_all.plot(kind='bar', stacked=True, legend=True, ax=ax3, sharex=True)
ax4 = ax3.twinx()
ax4.set_ylim(ax3.get_ylim())
plt.grid(False)
ax3.set_title(f'SH vs. SZ')
plt.savefig(sub_file_path+'\\%s.png' % f'SHvsSZ')




today = datetime.today().date().strftime('%Y-%m-%d')
attachment_list=[sub_file_path+'\\NorthboundCashFlow1-10_%s.png' % date_end,
                 sub_file_path+'\\NorthboundCashFlow11-20_%s.png' % date_end,
                 sub_file_path+'\\SHNorthboundCashFlow1-10_%s.png' % date_end,
                 sub_file_path+'\\SHNorthboundCashFlow11-20_%s.png' % date_end,
                 sub_file_path+'\\SZNorthboundCashFlow1-10_%s.png' % date_end,
                 sub_file_path+'\\SZNorthboundCashFlow11-20_%s.png' % date_end,
                 sub_file_path+'\\SHvsSZ.png'
                 ]
send_email('%s:机构北向资金流动对比（按市场）' % today, '机构北向资金流动对比（按市场）',
           images=attachment_list,
           receivers=NORTH_GROUP)

