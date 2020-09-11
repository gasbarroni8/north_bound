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

#from datafat.engine import rq
import rqdatac as rq
from WindPy import w
from omk.core.vendor.RQData import RQData
RQData.init()
w.start()

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
module_file_path='C:\\Users\\omaka\\SynologyDrive\\north_plot_save\\north_top_stock'
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
inst_code_all = tuple(info['inst_code'])

sql = f'''
SELECT * FROM jarvis.lgt_inst_north_holding_em
WHERE inst_code in {str(inst_code)}
AND as_of_date BETWEEN '{date_start}' AND '{date_end}'
'''
holding = pd.read_sql(sql, conn)
holding['stock_code'] = holding['stock_code'].apply(rq.id_convert)

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


ind_data=w.wsd("CI005003.WI,CI005005.WI,CI005006.WI,CI005001.WI,CI005002.WI,CI005012.WI,CI005013.WI,CI005010.WI,CI005011.WI,CI005016.WI,CI005018.WI,CI005009.WI,CI005017.WI,CI005014.WI,CI005015.WI,CI005019.WI,CI005020.WI,CI005027.WI,CI005025.WI,CI005026.WI,CI005028.WI,CI005022.WI,CI005030.WI,CI005021.WI,CI005007.WI,CI005023.WI,CI005008.WI,CI005024.WI,CI005004.WI,CI005029.WI", "pct_chg", date, date, "")
citics_group = {
    '交通运输': '基地',
    '传媒': '科技',
    '农林牧渔': '消费',
    '医药': '医药',
    '商贸零售': '消费',
    '国防军工': '制造',
    '基础化工': '周期',
    '家电': '制造',
    '建材': '基地',
    '建筑': '基地',
    '房地产': '基地',
    '有色金属': '周期',
    '机械': '制造',
    '汽车': '制造',
    '消费者服务': '消费',
    '煤炭': '周期',
    '电力及公用事业': '基地',
    '电力设备及新能源': '制造',
    '电子': '科技',
    '石油石化': '周期',
    '纺织服装': '消费',
    '综合': '综合',
    '综合金融': '金融',
    '计算机': '科技',
    '轻工制造': '消费',
    '通信': '科技',
    '钢铁': '周期',
    '银行': '金融',
    '非银行金融': '金融',
    '食品饮料': '消费',
}
citics_group_code = {
    'CI005003.WI': '有色金属',
    'CI005005.WI': '钢铁',
    'CI005006.WI': '基础化工',
    'CI005001.WI': '石油石化',
    'CI005002.WI': '煤炭',
    'CI005012.WI': '国防军工',
    'CI005013.WI': '汽车',
    'CI005010.WI': '机械',
    'CI005011.WI': '电力设备及新能源',
    'CI005016.WI': '家电',
    'CI005018.WI': '医药',
    'CI005009.WI': '轻工制造',
    'CI005017.WI': '纺织服装',
    'CI005014.WI': '商贸零售',
    'CI005015.WI': '消费者服务',
    'CI005019.WI': '食品饮料',
    'CI005020.WI': '农林牧渔',
    'CI005027.WI': '计算机',
    'CI005025.WI': '电子',
    'CI005026.WI': '通信',
    'CI005028.WI': '传媒',
    'CI005022.WI': '非银行金融',
    'CI005030.WI': '综合金融',
    'CI005021.WI': '银行',
    'CI005007.WI': '建筑',
    'CI005023.WI': '房地产',
    'CI005008.WI': '建材',
    'CI005024.WI': '交通运输',
    'CI005004.WI': '电力及公用事业',
    'CI005029.WI': '综合',
}
group_list = ['周期','制造','医药','消费','科技','金融','基地','综合']

temp = w.wset("shstockholdings",f"date={date_end};field=wind_code,sec_name,hold_stocks,change_holdstocks")
sh_holdings = pd.DataFrame(temp.Data, index=temp.Fields).T
temp = w.wset("szstockholdings",f"date={date_end};field=wind_code,sec_name,hold_stocks,change_holdstocks")
sz_holdings = pd.DataFrame(temp.Data, index=temp.Fields).T
sh_holdings['rq_code'] = rq.id_convert(sh_holdings['wind_code'].tolist())
sz_holdings['rq_code'] = rq.id_convert(sz_holdings['wind_code'].tolist())
sh_holdings = sh_holdings.set_index('rq_code')
sz_holdings = sz_holdings.set_index('rq_code')

hk_to_all_stock = (sh_holdings['change_holdstocks'].append(sz_holdings['change_holdstocks']) * md.loc[date_end]).dropna() / 1e8
citics = rq.get_instrument_industry(hk_to_all_stock.index, source='citics_2019', level=1, date=date_end)

temp_sh = w.wset("shszhkstockholdings","date="+ date+";tradeboard=shn")
temp_sz = w.wset("shszhkstockholdings","date="+ date+";tradeboard=szn")
sh_holding_detail = pd.DataFrame(temp_sh.Data, index=temp_sh.Fields).T
sh_holding_detail['wind_code'] = sh_holding_detail['wind_code'].apply(rq.id_convert)
sz_holding_detail = pd.DataFrame(temp_sz.Data, index=temp_sz.Fields).T
sz_holding_detail['wind_code'] = sz_holding_detail['wind_code'].apply(rq.id_convert)
all_holding = pd.concat([sh_holding_detail,sz_holding_detail]).set_index('wind_code')
md_t = md.T
all_holding = all_holding.reindex(md_t.index).fillna(0)
all_holding['cash_flow'] = all_holding['change_holdstocks'].mul(md_t[date])
stock_rank = all_holding['cash_flow'].sort_values(ascending = False)
stock_rank.dropna(axis=0, how='any', inplace=True)


#绘制流入top20图
# 查看top20
stock_top_num = 20
# 分析k家inst
key_inst_num = 5
# 得到流入top20的信息
a = []
b = []
c = []
for i in range(stock_top_num):
    a.append(stock_rank.index[i])
    b.append(i + 1)
for i in a:
    c.append(all_holding.query('index == @i')['sec_name'].iloc[0])
top_flow_in = pd.DataFrame({'wind_code':a,'sec_name':c,'rank':b})
pct_change.index=pd.to_datetime(pct_change.index)
top_flow_in['pct_change'] = top_flow_in['wind_code'].apply(lambda x: pct_change.loc[date,x])
top_flow_in=top_flow_in.set_index('wind_code')


# 计算绘图上下限
t_lim = b_lim = 0
t_10 = 10
b_10 = -10
t_5 = 5
b_5 = -5

# 先统计20家公司主要5家投资机构的数据上下限
for j in range(stock_top_num):
    top_flow_stock = holding_all[holding_all.stock_code == top_flow_in.index[j]]
    # 计算cashflow(千万元），每只股票对应一个pivot图
    flow_stock_holding_vol = top_flow_stock.pivot('as_of_date', 'inst_code', 'holding_vol')
    flow_stock_holding_vol = flow_stock_holding_vol.reindex(md.index).fillna(method = 'ffill')
    flow_stock_holding_mv = (flow_stock_holding_vol.mul(md.loc[:, top_flow_in.index[j]], axis = 0).fillna(0))

    # 特定股票的cf表 日期//instc_code
    cash_flow_top_stock = (flow_stock_holding_mv.diff() - (
        flow_stock_holding_mv.shift().mul(pct_change.loc[:, top_flow_in.index[j]], axis = 0))) / 1e8

    # 按照最后一天的资金排序
    inst_stock_rank_abs = cash_flow_top_stock.iloc[-1, :].abs().sort_values(ascending = False)

    # 统计想要分析的主要投资inst
    a = []
    b = []
    c = []
    for i in range(key_inst_num):
        a.append(inst_stock_rank_abs.index[i])
        b.append(i + 1)

    for i in a:
        c.append(info.query('inst_code == @i')['inst_name'].iloc[0])
    top_inst_stock = pd.DataFrame({'inst_code': a, 'inst_name': c, 'rank': b}).set_index('inst_code')

    temp = cash_flow_top_stock.T
    temp.columns = temp.columns.strftime('%Y-%m-%d')
    summary = temp.reindex(top_inst_stock.index).iloc[:, -1:]
    summary['inst_name'] = top_inst_stock['inst_name']
    summary.set_index(['inst_name'], inplace = True)
    summary = summary.T
    summary['total'] = all_holding.at[top_flow_in.index[j], 'cash_flow'] / 1e8

    # 设置绘图上下限

    if summary.max(axis = 1)[0] > t_10:
        t_10 = summary.max(axis = 1)[0]
    elif summary.max(axis = 1)[0] > t_5:
        t_5 = summary.max(axis = 1)[0]
    elif summary.max(axis = 1)[0] > t_lim:
        t_lim = summary.max(axis = 1)[0]
    else:
        t_lim = t_lim

    if summary.min(axis = 1)[0] < b_10:
        b_10 = summary.min(axis = 1)[0]
    elif summary.min(axis = 1)[0] < b_5:
        b_5 = summary.min(axis = 1)[0]
    elif summary.min(axis = 1)[0] < b_lim:
        b_lim = summary.min(axis = 1)[0]
    else:
        b_lim = b_lim

fig = plt.figure(figsize = (40, 40))
rowNum = 4
colNum = 5

# 先统计20家公司主要5家投资机构的数据
for j in range(stock_top_num):
    top_flow_stock = holding_all[holding_all.stock_code == top_flow_in.index[j]]
    # 计算cashflow(千万元），每只股票对应一个pivot图
    flow_stock_holding_vol = top_flow_stock.pivot('as_of_date', 'inst_code', 'holding_vol')
    flow_stock_holding_vol = flow_stock_holding_vol.reindex(md.index).fillna(method = 'ffill')
    flow_stock_holding_mv = (flow_stock_holding_vol.mul(md.loc[:, top_flow_in.index[j]], axis = 0).fillna(0))

    # 特定股票的cf表 日期//instc_code
    cash_flow_top_stock = (flow_stock_holding_mv.diff() - (
        flow_stock_holding_mv.shift().mul(pct_change.loc[:, top_flow_in.index[j]], axis = 0))) / 1e8

    # 按照最后一天的资金排序
    inst_stock_rank_abs = cash_flow_top_stock.iloc[-1, :].abs().sort_values(ascending = False)

    # 统计主要投资inst数据
    a = []
    b = []
    c = []
    for i in range(key_inst_num):
        a.append(inst_stock_rank_abs.index[i])
        b.append(i + 1)

    for i in a:
        c.append(info.query('inst_code == @i')['inst_name'].iloc[0])
    top_inst_stock = pd.DataFrame({'inst_code': a, 'inst_name': c, 'rank': b}).set_index('inst_code')

    temp = cash_flow_top_stock.T
    temp.columns = temp.columns.strftime('%Y-%m-%d')
    summary = temp.reindex(top_inst_stock.index).iloc[:, -1:]
    summary['inst_name'] = top_inst_stock['inst_name']
    summary.set_index(['inst_name'], inplace = True)
    summary = summary.T
    summary['total'] = all_holding.at[top_flow_in.index[j], 'cash_flow'] / 1e8

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
    summary.iloc[:, :-1].plot(kind = 'bar', stacked = True, legend = True, ax = ax, sharex = True, width = 0.3)

    plt.legend(loc = 'lower right', fontsize = 14)
    ax2 = ax.twinx()
    summary.iloc[:, -1].plot(kind = 'bar', legend = False, ax = ax2, edgecolor = 'r', linewidth = 3, facecolor = 'none')
    ylim = (b_limit, t_limit)
    ax.set_ylim(ylim)
    ax2.set_ylim(ylim)
    ax2.axis(False)
    ax2.grid(False)
    plt.gcf().autofmt_xdate()
    plt.title(top_flow_in.index[j]+' '+top_flow_in.iloc[j,0] +' ('+'%.2f%%' % (top_flow_in.iloc[j,2] * 100).round(2)+')',size = 30)

fig.suptitle(date + ' 北向净流入TOP20\n', fontsize = 60)
plt.savefig(sub_file_path+'\\%s' % '北向净流入TOP20')

#绘制流出top20图
# 查看top20
stock_top_num = 20
# 分析k家inst
key_inst_num = 5
# 得到流入top20的信息
a = []
b = []
c = []
for i in range(stock_top_num):
    a.append(stock_rank.index[-i - 1])
    b.append(-i - 1)
for i in a:
    c.append(all_holding.query('index == @i')['sec_name'].iloc[0])
top_flow_out = pd.DataFrame({'wind_code': a, 'sec_name': c, 'rank': b})
top_flow_out['pct_change'] = top_flow_out['wind_code'].apply(lambda x: pct_change.loc[date, x])
top_flow_out = top_flow_out.set_index('wind_code')

# 计算绘图上下限
t_lim = b_lim = 0
t_10 = 10
b_10 = -10
t_5 = 5
b_5 = -5

for j in range(stock_top_num):
    top_flow_stock = holding_all[holding_all.stock_code == top_flow_out.index[j]]
    # 计算cashflow(千万），每只股票对应一个pivot图
    flow_stock_holding_vol = top_flow_stock.pivot('as_of_date', 'inst_code', 'holding_vol')
    flow_stock_holding_vol = flow_stock_holding_vol.reindex(md.index).fillna(method = 'ffill')
    flow_stock_holding_mv = (flow_stock_holding_vol.mul(md.loc[:, top_flow_out.index[j]], axis = 0).fillna(0))

    # 特定股票的cf表 日期//instc_code
    cash_flow_top_stock = (flow_stock_holding_mv.diff() - (
        flow_stock_holding_mv.shift().mul(pct_change.loc[:, top_flow_out.index[j]], axis = 0))) / 1e8

    # 按照最后一天的资金排序
    inst_stock_rank_abs = cash_flow_top_stock.iloc[-1, :].abs().sort_values(ascending = False)

    # 统计想要分析的主要投资inst
    a = []
    b = []
    c = []
    for i in range(key_inst_num):
        a.append(inst_stock_rank_abs.index[i])
        b.append(i + 1)

    for i in a:
        c.append(info.query('inst_code == @i')['inst_name'].iloc[0])
    top_inst_stock = pd.DataFrame({'inst_code': a, 'inst_name': c, 'rank': b}).set_index('inst_code')

    temp = cash_flow_top_stock.T
    temp.columns = temp.columns.strftime('%Y-%m-%d')
    summary = temp.reindex(top_inst_stock.index).iloc[:, -1:]
    summary['inst_name'] = top_inst_stock['inst_name']
    summary.set_index(['inst_name'], inplace = True)
    summary = summary.T
    summary['total'] = all_holding.at[top_flow_out.index[j], 'cash_flow'] / 1e8

    # 设置绘图上下限

    if summary.max(axis = 1)[0] > t_10:
        t_10 = summary.max(axis = 1)[0]
    elif summary.max(axis = 1)[0] > t_5:
        t_5 = summary.max(axis = 1)[0]
    elif summary.max(axis = 1)[0] > t_lim:
        t_lim = summary.max(axis = 1)[0]
    else:
        t_lim = t_lim

    if summary.min(axis = 1)[0] < b_10:
        b_10 = summary.min(axis = 1)[0]
    elif summary.min(axis = 1)[0] < b_5:
        b_5 = summary.min(axis = 1)[0]
    elif summary.min(axis = 1)[0] < b_lim:
        b_lim = summary.min(axis = 1)[0]
    else:
        b_lim = b_lim

fig = plt.figure(figsize = (40, 40))
rowNum = 4
colNum = 5

for j in range(stock_top_num):
    top_flow_stock = holding_all[holding_all.stock_code == top_flow_out.index[j]]
    # 计算cashflow(千万），每只股票对应一个pivot图
    flow_stock_holding_vol = top_flow_stock.pivot('as_of_date', 'inst_code', 'holding_vol')
    flow_stock_holding_vol = flow_stock_holding_vol.reindex(md.index).fillna(method = 'ffill')
    flow_stock_holding_mv = (flow_stock_holding_vol.mul(md.loc[:, top_flow_out.index[j]], axis = 0).fillna(0))

    # 特定股票的cf表 日期//instc_code
    cash_flow_top_stock = (flow_stock_holding_mv.diff() - (
        flow_stock_holding_mv.shift().mul(pct_change.loc[:, top_flow_out.index[j]], axis = 0))) / 1e8

    # 按照最后一天的资金排序
    inst_stock_rank_abs = cash_flow_top_stock.iloc[-1, :].abs().sort_values(ascending = False)

    # 统计想要分析的主要投资inst
    a = []
    b = []
    c = []
    for i in range(key_inst_num):
        a.append(inst_stock_rank_abs.index[i])
        b.append(i + 1)

    for i in a:
        c.append(info.query('inst_code == @i')['inst_name'].iloc[0])
    top_inst_stock = pd.DataFrame({'inst_code': a, 'inst_name': c, 'rank': b}).set_index('inst_code')

    temp = cash_flow_top_stock.T
    temp.columns = temp.columns.strftime('%Y-%m-%d')
    summary = temp.reindex(top_inst_stock.index).iloc[:, -1:]
    summary['inst_name'] = top_inst_stock['inst_name']
    summary.set_index(['inst_name'], inplace = True)
    summary = summary.T
    summary['total'] = all_holding.at[top_flow_out.index[j], 'cash_flow'] / 1e8

    if summary.max(axis = 1)[0] > 10:
        t_limit = t_10 + 0.5
    elif summary.max(axis = 1)[0] > 5:
        t_limit = t_5 + 0.5
    else:
        t_limit = t_lim + 1

    if summary.min(axis = 1)[0] < -10:
        b_limit = b_10 - 0.5
    elif summary.min(axis = 1)[0] < -5:
        b_limit = b_5 - 0.5
    else:
        b_limit = b_lim - 1

        # 绘图
    plt.rcParams['axes.unicode_minus'] = False
    sns.set(font_scale = 1.5, font = 'SimHei')

    ax = fig.add_subplot(rowNum, colNum, j + 1)
    summary.iloc[:, :-1].plot(kind = 'bar', stacked = True, legend = True, ax = ax, sharex = True, width = 0.3)
    plt.legend(loc = 'lower right', fontsize = 14)
    ax2 = ax.twinx()
    summary.iloc[:, -1].plot(kind = 'bar', legend = False, ax = ax2, edgecolor = 'r', linewidth = 3, facecolor = 'none')
    ylim = (b_limit, t_limit)
    ax.set_ylim(ylim)
    ax2.set_ylim(ylim)
    ax2.axis(False)
    ax2.grid(False)
    plt.gcf().autofmt_xdate()
    plt.title(
        top_flow_out.index[j] + ' ' + top_flow_out.iloc[j, 0] + ' (' + '%.2f%%' % (top_flow_out.iloc[j, 2] * 100).round(
            2) + ')', size = 30)

fig.suptitle(date + ' 北向净流出TOP20', fontsize = 60)
plt.savefig(sub_file_path+'\\%s' %'北向净流出T20')

#绘制top个股整体
rank = []
for i in range(stock_top_num):
    rank.append(i+1)
inflow_top = pd.DataFrame({'rank':rank})
outflow_top = pd.DataFrame({'rank':rank})
inflow_top['代码'] = top_flow_in.index
inflow_top['名称']= inflow_top['rank'].apply(lambda x : top_flow_in['sec_name'][x-1])
inflow_top['涨跌幅'] = inflow_top['代码'].apply(lambda x :pct_change.loc[date,x])
inflow_top['净流动（亿元）'] = inflow_top['代码'].apply(lambda x :all_holding.loc[x,'cash_flow']/1e8)
inflow_top['中信一级'] = inflow_top['代码'].apply(lambda x :citics.loc[x,'first_industry_name'])
inflow_top['产业板块'] = inflow_top['中信一级'].map(citics_group)
outflow_top['代码'] = top_flow_out.index
outflow_top['名称']= outflow_top['rank'].apply(lambda x : top_flow_out['sec_name'][x-1])
outflow_top['涨跌幅'] = outflow_top['代码'].apply(lambda x :pct_change.loc[date,x])
outflow_top['净流动（亿元）'] = outflow_top['代码'].apply(lambda x :all_holding.loc[x,'cash_flow']/1e8)
outflow_top['中信一级'] = outflow_top['代码'].apply(lambda x :citics.loc[x,'first_industry_name'])
outflow_top['产业板块'] = outflow_top['中信一级'].map(citics_group)
outflow_top['rank'] = outflow_top['rank'].apply(lambda x: -x)

#关注top15只个股
k = 20
northbound_rank = inflow_top[:k].append(outflow_top.sort_values('rank')[-k:]).set_index('名称')
northbound_rank['color'] = (northbound_rank['净流动（亿元）']*northbound_rank['涨跌幅']).apply(lambda x: 1 if x>0  else 0)
northbound_rank['涨跌幅'] = northbound_rank['涨跌幅'].apply(lambda x: x*100)

plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False
sns.set(font_scale = 1.5, font = 'SimHei')
fig, ax = plt.subplots(figsize = (30, 15))
ax1 = ax.twinx()

if max(northbound_rank['涨跌幅'].max(), abs(northbound_rank['涨跌幅'].min())) < 5:
    y_lim = (-5, 5)
    text_loc = 3.5
else:
    ylim = (-10.5, 10.5)
    text_loc = 9

y1 = (int(max(northbound_rank['净流动（亿元）'].max(), abs(northbound_rank['净流动（亿元）'].min())) / 5) + 1) * 5
ylim1 = (-y1, y1)
title_size = 20
txt_size = 16

color = {0: 'orange', 1: 'blue'}
line1 = ax.bar(northbound_rank.index, northbound_rank['涨跌幅'],
               width = 0.7,
               color = [color[0] if i == 0 else color[1] for i in northbound_rank['color']],
               alpha = 0.35,
               label = '涨跌幅（%）')
line2 = ax1.plot(northbound_rank['净流动（亿元）'], 'k^', label = '资金流动净值（亿元）')

for i in range(northbound_rank.shape[0]):
    height = northbound_rank.iloc[i, 3] - 0.7
    num = str(northbound_rank.iloc[i, 3].round(2))
    plt.text(i - 0.3, height, num, size = txt_size)

ax.set_ylabel('涨跌幅（%）', size = txt_size)
ax1.set_ylabel('资金流动净值（亿元）', size = txt_size, )
ax1.legend(loc = 0)
ax1.grid(False)

ax.set_ylim(ylim)
ax1.set_ylim(ylim1)
ax.xaxis.set_ticks_position('bottom')
ax.spines['bottom'].set_position(('data', 0))
plt.axvline(k - 0.5, alpha = 0.6)

plt.title('Northbound Cash Flow TOP' + str(k) + ' ' + date, size = title_size)

plt.text((k - 4) / 2, text_loc, 'Top Inflow', size = title_size, alpha = 2,
         bbox = dict(facecolor = 'g', edgecolor = 'blue', alpha = 0.35))
plt.text((k - 4) / 2 + k, text_loc, 'Top Outflow', size = title_size, alpha = 2,
         bbox = dict(facecolor = 'g', edgecolor = 'blue', alpha = 0.35))
ax.set_xticklabels(northbound_rank.index,rotation=90, fontsize=txt_size,color ='red')
plt.savefig(sub_file_path+'\\%s' % 'Northbound CF TOP')



today = datetime.today().date().strftime('%Y-%m-%d')
attachment_list=[sub_file_path+'\\Northbound CF TOP.png',
                 sub_file_path+'\\北向净流出T20.png',
                 sub_file_path+'\\北向净流入TOP20.png']
send_email('%s:北向资金流入流出TOP个股' % today, '流入流出TOP个股',
           images=attachment_list,
           receivers=NORTH_GROUP)