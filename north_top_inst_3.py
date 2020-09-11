from datetime import datetime, timedelta

from omk.toolkit.mail import send_email
from omk.toolkit.mail import FUND_ISSUE_LIST, PROD_EMAIL_LIST, DEBUG_MAIL_LIST, EMAIL_TEST,NORTH_GROUP

import os
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
import numpy as np

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns

# from datafat.engine import rq
import rqdatac as rq
from WindPy import w

from omk.core.vendor.RQData import RQData

RQData.init()


def _local_conn():
    dbinfo = {  # 'user': 'jinxi',
        # 'password': 'omaka8080',
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
    folder = os.path.exists(file_path)
    if not folder:
        try:
            os.mkdir(file_path, mode=777)
        except Exception:
            return False
    else:
        return False
    return True


# 根据时间和模块名字创建当日子文件夹
module_file_path = 'C:\\Users\\omaka\\SynologyDrive\\north_plot_save\\north_top_inst'
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

# 所有股票代码
instruments = holding['stock_code'].drop_duplicates().sort_values().to_list()
# 历史涨跌幅
pct_change = rq.get_price_change_rate(instruments, start_date=date_start, end_date=date_end)
# 获取历史价格
md = rq.get_price(instruments, start_date=date_start, end_date=date_end, fields=['close'])

cash_flow_sz = pd.DataFrame()
cash_flow_sh = pd.DataFrame()
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

ind_data = w.wsd(
    "CI005003.WI,CI005005.WI,CI005006.WI,CI005001.WI,CI005002.WI,CI005012.WI,CI005013.WI,CI005010.WI,CI005011.WI,CI005016.WI,CI005018.WI,CI005009.WI,CI005017.WI,CI005014.WI,CI005015.WI,CI005019.WI,CI005020.WI,CI005027.WI,CI005025.WI,CI005026.WI,CI005028.WI,CI005022.WI,CI005030.WI,CI005021.WI,CI005007.WI,CI005023.WI,CI005008.WI,CI005024.WI,CI005004.WI,CI005029.WI",
    "pct_chg", date, date, "")
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
group_list = ['周期', '制造', '医药', '消费', '科技', '金融', '基地', '综合']

temp = w.wset("shstockholdings", f"date={date_end};field=wind_code,sec_name,hold_stocks,change_holdstocks")
sh_holdings = pd.DataFrame(temp.Data, index=temp.Fields).T
temp = w.wset("szstockholdings", f"date={date_end};field=wind_code,sec_name,hold_stocks,change_holdstocks")
sz_holdings = pd.DataFrame(temp.Data, index=temp.Fields).T
sh_holdings['rq_code'] = rq.id_convert(sh_holdings['wind_code'].tolist())
sz_holdings['rq_code'] = rq.id_convert(sz_holdings['wind_code'].tolist())
sh_holdings = sh_holdings.set_index('rq_code')
sz_holdings = sz_holdings.set_index('rq_code')

hk_to_all_stock = (sh_holdings['change_holdstocks'].append(sz_holdings['change_holdstocks']) * md.loc[
    date_end]).dropna() / 1e8
citics = rq.get_instrument_industry(hk_to_all_stock.index, source='citics_2019', level=1, date=date_end)
citics['cash_flow'] = hk_to_all_stock
cash_flow_citics = citics.groupby('first_industry_name')['cash_flow'].sum().to_frame()
cash_flow_citics['group'] = cash_flow_citics.index.map(citics_group)
cash_flow_ind = cash_flow_citics.groupby('group')['cash_flow'].sum().to_frame()
ind_change = pd.DataFrame(ind_data.Data, columns=ind_data.Codes, index=ind_data.Times)
ind_change = ind_change.T
ind_change['first_industry_name'] = ind_change.index.map(citics_group_code)
ind_change.set_index(["first_industry_name"], inplace=True)
ind_change.columns = ['price_change']
summary_citics = pd.merge(cash_flow_citics, ind_change, on=['first_industry_name']).groupby('group').apply(
    lambda x: x.sort_values('price_change', ascending=False)).reindex(index=group_list, level=0)
summary_citics['color'] = (summary_citics.price_change * summary_citics.cash_flow).apply(lambda x: 1 if x > 0 else 0)
summary_citics.index = summary_citics.index.droplevel(0)

top_north = cash_flow.iloc[-1, :].sort_values(ascending=False)
cf_inst = pd.DataFrame({'inst_name': top_north.index, 'cash_flow': top_north.values})
top_order = [0, 1, 2, 19, 18, 17]

top = []
for i in top_order:
    top.append(str(cf_inst.loc[i, 'inst_name']))
today = cash_flow.index[-1]

top_code = []
for i in top:
    top_code.append(int(info.loc[info['inst_name'] == i,'inst_code' ]) )
# for i in top:
#     top_code.append(int(info.loc[info['inst_name'] == i,'inst_code' ]) )

top_range = [' 资金流入top1 ', ' 资金流入top2 ', ' 资金流入top3 ', ' 资金流出top1 ', ' 资金流出top2 ', ' 资金流出top3 ']
key_inst_num = 6
for i in range(key_inst_num):
    # 获取该机构的持仓数量（个股数据）
    top_holding = holding[(holding['inst_code'] == top_code[i])]
    top_holding_vol = top_holding.pivot('as_of_date', 'stock_code', 'holding_vol')
    top_holding_vol = top_holding_vol.reindex(md.index).fillna(method='ffill')
    # 获取该机构的持仓市值（个股数据）
    top_holding_mv = (top_holding_vol * md).fillna(0)
    # 计算资金流动额（个股数据）（日期\\股票）
    top_cash_flow_stock = (top_holding_mv.diff() - (top_holding_mv.shift() * pct_change)) / 1e8

    # 获取中信行业分类
    citics = rq.get_instrument_industry(hk_to_all_stock.index, source='citics_2019', level=1, date=date_end)
    # 资金流动额（个股数据）（股票code\\日期）
    top_cash_flow = top_cash_flow_stock.T
    top_cash_flow.index.name = 'order_book_id'
    # 合并数据得到top机构投资个股的资金流动以及个股对应行业
    top_cities = pd.merge(citics, top_cash_flow, on=['order_book_id'])
    # 按照行业加总得到group资金流动数据
    top_cities_cf = top_cities.groupby('first_industry_name')[top_cities.columns[3:23]].sum()
    # 投资金额排序，流入流出各取前三
    rank = top_cities_cf.iloc[:, -1].sort_values(ascending=False)
    a = []
    b = []
    c = []
    for j in range(key_inst_num):
        if j < 3:
            a.append(rank.index[j])
            b.append(j + 1)
        else:
            a.append(rank.index[-j + 2])
            b.append(-j + 2)
    top_ind = pd.DataFrame({'first_industry_name': a, 'rank': b})
    summary = top_cities_cf.loc[[top_ind.iloc[0, 0], top_ind.iloc[1, 0], top_ind.iloc[2, 0],
                                 top_ind.iloc[3, 0], top_ind.iloc[4, 0], top_ind.iloc[5, 0]], :].T
    summary.index = summary.index.strftime('%Y-%m-%d')

    # top机构的净资金流动top_range[i]
    top_cities_cf.loc['total'] = top_cities_cf.apply(lambda x: x.sum())
    top_total_cf = top_cities_cf.loc["total", :]
    top_total_cf.index = top_total_cf.index.strftime('%Y-%m-%d')

    # 绘图
    plt.rcParams['axes.unicode_minus'] = False
    sns.set(font_scale=1.5, font='SimHei')

    ax = summary.plot(kind='bar', stacked=True, figsize=(30, 15), legend=True)
    plt.legend(bbox_to_anchor=(0.5, -0.15), loc='upper center', ncol=3)
    ax2 = ax.twinx()
    top_total_cf.plot(kind='bar', legend=False, ax=ax2, edgecolor='r', linewidth=3, facecolor='none')

    ylim = (min(ax.get_ylim()[0], ax2.get_ylim()[0]), max(ax.get_ylim()[1], ax2.get_ylim()[1]))
    ax.set_ylim(ylim)
    ax2.set_ylim(ylim)
    ax2.grid(False)
    plt.gcf().autofmt_xdate()
    plt.title(date + top_range[i] + top[i] + ' 北向资金流动（亿元）', size=20, loc='center')
    plt.savefig(sub_file_path + '\\%s.png' % (date + top_range[i] + top[i] + '北向资金流动(亿)'))

###个股
all_instruments = rq.all_instruments()
all_instruments = all_instruments.set_index('order_book_id')
for inst in top_code:
    holding_vol = holding.query('inst_code == @inst').pivot('as_of_date', 'stock_code', 'holding_vol')
    inst_name = info.query('inst_code == @inst')['inst_name'].iloc[0]
    inst_name=inst_name.replace('.','')
    # 补充缺失数据，仅外资银行可用
    holding_vol = holding_vol.reindex(md.index).fillna(method='ffill')
    # 持仓市值
    holding_mv = (holding_vol * md).fillna(0)
    cash_flow_stock = (holding_mv.diff() - (holding_mv.shift() * pct_change)) / 1e8
    cash_flow_stock.index = cash_flow_stock.index.date
    cash_flow_stock.columns = all_instruments.loc[cash_flow_stock.columns, 'symbol']
    cash_flow_in_name = pd.DataFrame()
    cash_flow_out_name = pd.DataFrame()
    cash_flow_in = pd.DataFrame()
    cash_flow_out = pd.DataFrame()
    for i, row in cash_flow_stock.iterrows():
        temp_out = row.dropna().sort_values()[:10]
        temp_in = row.dropna().sort_values()[-10:]
        cash_flow_in_name[i] = temp_in.index
        cash_flow_out_name[i] = temp_out.index
        cash_flow_in[i] = temp_in.values
        cash_flow_out[i] = temp_out.values

    # 绘图
    plt.rcParams['axes.unicode_minus'] = False
    sns.set(font_scale=1.5, font='SimHei')
    ax = cash_flow_stock[temp_in.index].plot(kind='bar', stacked=True, figsize=(30, 15), legend=True)
    plt.legend(bbox_to_anchor=(0.5, -0.15), loc='upper center', ncol=3)
    ax2 = ax.twinx()
    cash_flow_stock.sum(axis=1).plot(kind='bar', legend=False, ax=ax2, edgecolor='r', linewidth=3,
                                     facecolor='none')

    ylim = (min(ax.get_ylim()[0], ax2.get_ylim()[0]), max(ax.get_ylim()[1], ax2.get_ylim()[1]))
    ax.set_ylim(ylim)
    ax2.set_ylim(ylim)
    ax2.grid(False)
    plt.gcf().autofmt_xdate()
    plt.title(f'Cash Flow Inflow Top 10 ({inst_name}), {cash_flow.index[-1]:%Y-%m-%d}')
    plt.savefig(sub_file_path + '\\%s' % (f'CashFlowInflowTop10({inst_name}){cash_flow.index[-1]:%Y-%m-%d}'))

    ax3 = cash_flow_stock[temp_out.index].plot(kind='bar', stacked=True, figsize=(30, 15), legend=True)
    ax3.set_ylim(ax.get_ylim())
    plt.legend(bbox_to_anchor=(0.5, -0.15), loc='upper center', ncol=3)
    ax4 = ax3.twinx()
    cash_flow_stock.sum(axis=1).plot(kind='bar', legend=False, ax=ax4, edgecolor='r', linewidth=3,
                                     facecolor='none')

    ax3.set_ylim(ylim)
    ax4.set_ylim(ylim)
    ax4.grid(False)
    plt.gcf().autofmt_xdate()
    plt.title(f'Cash Flow Outflow Top 10 ({inst_name}), {cash_flow.index[-1]:%Y-%m-%d}')
    plt.savefig(sub_file_path + '\\%s' % (f'CashFlowOutflowTop10({inst_name}){cash_flow.index[-1]:%Y-%m-%d}'))

today = datetime.today().date().strftime('%Y-%m-%d')
attachment_list1 = [
    sub_file_path + '\\%s 资金流入top%d %s' % (date_end.strftime('%Y-%m-%d'), i, comapny_name) + '北向资金流动(亿)' for
    i, comapny_name in
    zip(range(1, 4), top[:3])]

attachment_list2 = [
    sub_file_path + '\\%s 资金流出top%d %s' % (date_end.strftime('%Y-%m-%d'), i, comapny_name) + '北向资金流动(亿)' for
    i, comapny_name in
    zip(range(1, 4), top[3:])]


attachment_list3 = [
    sub_file_path + '\\CashFlowInflowTop10(' + company_name.replace('.','') + ')%s' % date_end.strftime('%Y-%m-%d')
    for company_name in top[:3]]

attachment_list4 = [
    sub_file_path + '\\CashFlowOutflowTop10(' + company_name.replace('.','') + ')%s' % date_end.strftime('%Y-%m-%d')
    for company_name in top[:3]]

attachment_list5 = [
    sub_file_path + '\\CashFlowInflowTop10(' + company_name.replace('.','') + ')%s' % date_end.strftime('%Y-%m-%d')
    for company_name in top[3:]]

attachment_list6 = [
    sub_file_path + '\\CashFlowOutflowTop10(' + company_name.replace('.','') + ')%s' % date_end.strftime('%Y-%m-%d')
    for company_name in top[3:]]


attachment_list7=[]
attachment_list8=[]

for file1,file2,file3,file4 in zip(attachment_list3,attachment_list4,attachment_list5,attachment_list6):
    attachment_list7.extend([file1,file2])
    attachment_list8.extend([file3,file4])

attachment_list1=[x+'.png' for x in attachment_list1]
attachment_list2=[x+'.png' for x in attachment_list2]
attachment_list3=[x+'.png' for x in attachment_list3]
attachment_list4=[x+'.png' for x in attachment_list4]
attachment_list7=[x+'.png' for x in attachment_list7]
attachment_list8=[x+'.png' for x in attachment_list8]


send_email('%s:北向资金净流入TOP3机构资金去向(个股)' % today, '北向资金净流入TOP机构资金去向(个股)',
           images=attachment_list7,
           receivers=NORTH_GROUP)

send_email('%s:北向资金净流出TOP3机构资金去向(个股)' % today, '北向资金净流出TOP机构资金去向(个股)',
           images=attachment_list8,
           receivers=NORTH_GROUP)

send_email('%s:北向资金净流入TOP3机构资金去向(行业)' % today, '北向资金净流入TOP机构资金去向(行业)',
           images=attachment_list1,
           receivers=NORTH_GROUP)

send_email('%s:北向资金净流出TOP3机构资金去向(行业)' % today, '北向资金净流出TOP机构资金去向(行业)',
           images=attachment_list2,
           receivers=NORTH_GROUP)