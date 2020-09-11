from datetime import datetime,timedelta
import os

from omk.toolkit.mail import send_email
from omk.toolkit.mail import FUND_ISSUE_LIST, PROD_EMAIL_LIST, DEBUG_MAIL_LIST,EMAIL_TEST,NORTH_GROUP

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
module_file_path='C:\\Users\\omaka\\SynologyDrive\\north_plot_save\\north_ind'
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
conn=_local_conn()
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
citics['cash_flow'] = hk_to_all_stock
cash_flow_citics = citics.groupby('first_industry_name')['cash_flow'].sum().to_frame()
cash_flow_citics['group'] = cash_flow_citics.index.map(citics_group)
cash_flow_ind = cash_flow_citics.groupby('group')['cash_flow'].sum().to_frame()
ind_change = pd.DataFrame(ind_data.Data, columns = ind_data.Codes, index =ind_data.Times)
ind_change = ind_change.T
ind_change['first_industry_name'] = ind_change.index.map(citics_group_code)
ind_change.set_index(["first_industry_name"], inplace=True)
ind_change.columns = ['price_change']
summary_citics = pd.merge(cash_flow_citics,ind_change, on = ['first_industry_name']).groupby('group').apply(lambda x: x.sort_values('price_change', ascending=False)).reindex(index=group_list, level=0)
summary_citics['color'] = (summary_citics.price_change*summary_citics.cash_flow).apply(lambda x: 1 if x>0  else 0)
summary_citics.index = summary_citics.index.droplevel(0)

plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False
sns.set(font_scale=1.5, font='SimHei')
fig, ax = plt.subplots(figsize=(30, 15))
ax1 = ax.twinx()

top_price = abs(int(summary_citics['price_change'].max(axis=0)))
bottom_price = abs(int(summary_citics['price_change'].min(axis=0)))
top_cash = abs(int(summary_citics['cash_flow'].max(axis=0)))
bottom_cash = abs(int(summary_citics['cash_flow'].min(axis=0)))
ylim = (-abs(max(top_price, bottom_price) + 1), abs(max(top_price, bottom_price) + 1))
ylim1 = (-abs(max(top_cash, bottom_cash) + 1), abs(max(top_cash, bottom_cash) + 1))
text_loc = -max(top_cash, bottom_cash) + 2
title_size = 20
txt_size = 22

color = {0: 'orange', 1: 'blue'}
line1 = ax.bar(summary_citics.index, summary_citics['price_change'],
               width=0.7,
               color=[color[0] if i == 0 else color[1] for i in summary_citics['color']],
               alpha=0.35,
               label='行业涨跌幅（%）')
line2 = ax1.plot(summary_citics['cash_flow'], 'k^', label='北向资金流动净值（亿元）')
for i in range(summary_citics.shape[0]):
    height = summary_citics.iloc[i, 0] - 1
    num = str(summary_citics.iloc[i, 0].round(2))
    plt.text(i - 0.3, height, num, size=txt_size)

# ax.set_xlabel('Industry', size = txt_size)
ax.set_ylabel('行业涨跌幅（%）', size=txt_size)
ax1.set_ylabel('北向资金流动净值（亿元）', size=txt_size, )
ax1.legend(loc=0)

ax.set_ylim(ylim)
ax1.set_ylim(ylim1)
ax.xaxis.set_ticks_position('bottom')
ax.spines['bottom'].set_position(('data', 0))
# plt.gcf().autofmt_xdate()
ax.set_xticklabels(summary_citics.index, rotation=90, fontsize=13, color='red')

plt.title(date + ' 中信一级行业涨跌幅与北向资金净流入对比（%）（亿元）', size=title_size, loc='center')
plt.grid(axis="y")
plt.axvline(4.5, alpha=0.6)
plt.axvline(9.5, alpha=0.6)
plt.axvline(10.5, alpha=0.6)
plt.axvline(16.5, alpha=0.6)
plt.axvline(20.5, alpha=0.6)
plt.axvline(23.5, alpha=0.6)
plt.axvline(28.5, alpha=0.6)
plt.text(1, text_loc, "周期\n" + str(cash_flow_ind.at['周期', 'cash_flow'].round(2)), size=txt_size, alpha=2,
         bbox=dict(facecolor='g', edgecolor='blue', alpha=0.35))
plt.text(6.5, text_loc, "制造\n" + str(cash_flow_ind.at['制造', 'cash_flow'].round(2)), size=txt_size, alpha=2,
         bbox=dict(facecolor='g', edgecolor='blue', alpha=0.35))
plt.text(9.6, text_loc, "医药\n" + str(cash_flow_ind.at['医药', 'cash_flow'].round(2)), size=txt_size, alpha=2,
         bbox=dict(facecolor='g', edgecolor='blue', alpha=0.35))
plt.text(13, text_loc, "消费\n" + str(cash_flow_ind.at['消费', 'cash_flow'].round(2)), size=txt_size, alpha=2,
         bbox=dict(facecolor='g', edgecolor='blue', alpha=0.35))
plt.text(18, text_loc, "科技\n" + str(cash_flow_ind.at['科技', 'cash_flow'].round(2)), size=txt_size, alpha=2,
         bbox=dict(facecolor='g', edgecolor='blue', alpha=0.35))
plt.text(21, text_loc, "金融\n" + str(cash_flow_ind.at['金融', 'cash_flow'].round(2)), size=txt_size, alpha=2,
         bbox=dict(facecolor='g', edgecolor='blue', alpha=0.35))
plt.text(25, text_loc, "基地\n" + str(cash_flow_ind.at['基地', 'cash_flow'].round(2)), size=txt_size, alpha=2,
         bbox=dict(facecolor='g', edgecolor='blue', alpha=0.35))
plt.text(29, text_loc, "综合\n" + str(cash_flow_ind.at['综合', 'cash_flow'].round(2)), size=txt_size, alpha=2,
         bbox=dict(facecolor='g', edgecolor='blue', alpha=0.35))
plt.savefig(sub_file_path+'\\%s' % "中信一级行业涨跌幅与北向资金净流入对比")


today=datetime.today().date().strftime('%Y-%m-%d')
attachment_list=[sub_file_path+"\\中信一级行业涨跌幅与北向资金净流入对比.png"]
send_email('%s:中信一级行业分类北向资金数据' % today,'中信一级行业分类北向资金数据',
           images=attachment_list,
           receivers=NORTH_GROUP)
