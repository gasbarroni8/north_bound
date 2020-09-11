import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import rqdatac as rq
from sqlalchemy.orm import Query, sessionmaker
from sqlalchemy import func
from omk.core.orm_db.jarvis import SouthernCashFlow
from omk.core.orm_db import EnginePointer

# 南向资金抓取程序
cookies = {
    'st_si': '54969188708347',
    'st_pvi': '43322278274401',
    'st_sp': '2020-08-20%2014%3A30%3A53',
    'st_inirUrl': 'http%3A%2F%2Fdata.eastmoney.com%2Fhsgtcg%2FStockStatistics.aspx',
    'st_sn': '1',
    'st_psi': '20200820151907490-113300302015-2279010267',
    'st_asi': 'delete',
}

headers = {
    'Connection': 'keep-alive',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36',
    'Accept': '*/*',
    'Referer': 'http://data.eastmoney.com/hsgtcg/StockStatistics.aspx?tab=3',
    'Accept-Language': 'zh-CN,zh;q=0.9',
}


def set_daily_params(p, today=datetime.today() - timedelta(1)):
    if not p:
        raise ValueError('Page number must be set up!')
    today = today.strftime('%Y-%m-%d')
    params = (
        ('type', 'HSGTHDSTA'),
        ('token', '70f12f2f4f091e459a279469fe49eca5'),
        ('st', 'HDDATE,SHAREHOLDPRICE'),
        ('sr', '3'),
        ('p', '%d' % int(p)),
        ('ps', '50'),
        ('js', '/{"data":(x)/}'),
        ('filter', '(MARKET=\'S\')(HDDATE=^%s^)' % today),
        ('rt', '53263598'),
    )
    return params


def set_30days_params(p, today=datetime.today() - timedelta(1), previous_days=30):
    if not p:
        raise ValueError('Page number must be set up!')
    print('正在抓取第%d页的数据' % p)
    start_day = rq.get_trading_dates(today - timedelta(previous_days), today)[0]
    today = today.strftime('%Y-%m-%d')
    start_day = start_day.strftime('%Y-%m-%d')
    params = (
        ('type', 'HSGTHDSTA'),
        ('token', '70f12f2f4f091e459a279469fe49eca5'),
        ('st', 'HDDATE,SHAREHOLDPRICE'),
        ('sr', '3'),
        ('p', '%d' % int(p)),
        ('ps', '50'),
        ('js', '/{"data":(x)/}'),
        ('filter', '(MARKET=\'S\')(HDDATE<=^%s^ and HDDATE>=^%s^)' % (today, start_day)),
        ('rt', '53266223'),
    )
    return params


def set_specific_period_params(p, today=datetime.today(), previous_days=3, current_day=None):
    if not p:
        raise ValueError('Page number must be set up!')
    start_day = rq.get_trading_dates(today - timedelta(previous_days), today)[0]
    today = today.strftime('%Y-%m-%d')
    start_day = start_day.strftime('%Y-%m-%d')
    print('正在抓取%s日期数据' % current_day)
    params = (
        ('type', 'HSGTHDSTA'),
        ('token', '70f12f2f4f091e459a279469fe49eca5'),
        ('st', 'HDDATE,SHAREHOLDPRICE'),
        ('sr', '3'),
        ('p', '%d' % int(p)),
        ('ps', '50'),
        ('js', '/{"data":(x)/}'),
        ('filter', '(MARKET=\'S\')(HDDATE<^%s^ and HDDATE>=^%s^)' % (today, start_day)),
        ('rt', '53266223'),
    )
    return params


def main(spider_type='daily', today=None, previous_days=None, end_rounds=10000, engine=None):
    if engine is None or session is None:
        engine = EnginePointer.get_engine()
    p = 1
    if spider_type == 'daily':
        part_data = pd.DataFrame()
        for p in range(1, end_rounds):
            try:
                response = requests.get('http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get',
                                        headers=headers,
                                        params=set_daily_params(p), cookies=cookies, verify=False)
                temp = response.text.replace('/', '').replace("'", '"')
                data = json.loads(temp)['data']
                if len(data) == 0:
                    break
                print(data)
                for i in data:
                    part_data = pd.concat([part_data, pd.DataFrame([i])])

            except requests.exceptions.ConnectionError:
                while True:
                    response = requests.get('http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get',
                                            headers=headers,
                                            params=set_daily_params(p), cookies=cookies, verify=False)
                    if response.status_code == 200:
                        break
                temp = response.text.replace('/', '').replace("'", '"')
                data = json.loads(temp)['data']
                if len(data) == 0:
                    break
                print(data)
                for i in data:
                    part_data = pd.concat([part_data, pd.DataFrame([i])])
        save_date = datetime.today() - timedelta(1)
        if part_data.shape[0] > 0:
            print('开始存储第%s日期数据!' % save_date.strftime('%Y-%m-%d'))
            part_data['SHAREHOLDPRICE'] = [round(x, 6) for x in part_data['SHAREHOLDPRICE']]
            part_data['SHAREHOLDPRICEFIVE'] = [round(x, 6) for x in part_data['SHAREHOLDPRICEFIVE']]
            part_data['SHAREHOLDPRICEONE'] = [round(x, 6) for x in part_data['SHAREHOLDPRICEONE']]
            part_data['SHAREHOLDPRICETEN'] = [round(x, 6) for x in part_data['SHAREHOLDPRICETEN']]
            part_data['SHAREHOLDSUM'] = [round(x, 6) for x in part_data['SHAREHOLDSUM']]
            part_data.replace('-', 0.0).to_sql(SouthernCashFlow.table_name(), engine,
                                               schema=SouthernCashFlow.schema(),
                                               index=False,
                                               if_exists='append')
            print('存储第%s日期数据结束!' % save_date.strftime('%Y-%m-%d'))
        else:
            print('数据抓取为空,不进行数据保存!')


    elif spider_type == '30days':
        for p in range(1, end_rounds):
            part_data = pd.DataFrame()
            response = None
            try:
                response = requests.get('http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get',
                                        headers=headers,
                                        params=set_30days_params(p), cookies=cookies, verify=False)
                temp = response.text.replace('/', '').replace("'", '"')
                data = json.loads(temp)['data']
                if len(data) == 0:
                    break
                print(data)
                for i in data:
                    part_data = pd.concat([part_data, pd.DataFrame([i])])

            except requests.exceptions.ConnectionError:
                print('第%d页抓取出错!重新抓取第%d页的数据' % (p, p))
                while True:
                    response = requests.get('http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get',
                                            headers=headers,
                                            params=set_30days_params(p), cookies=cookies, verify=False)
                    if response.status_code == 200:
                        break
                temp = response.text.replace('/', '').replace("'", '"')
                data = json.loads(temp)['data']
                if len(data) == 0:
                    break
                print(data)
                for i in data:
                    part_data = pd.concat([part_data, pd.DataFrame([i])])
            print('第%d页数据抓取结束!' % p)
            print('*' * 300)
            if part_data.shape[0] != 0:
                print('开始存储第%d页数据!' % p)
                part_data['SHAREHOLDPRICE'] = [round(x, 6) for x in part_data['SHAREHOLDPRICE']]
                part_data['SHAREHOLDPRICEFIVE'] = [round(x, 6) for x in part_data['SHAREHOLDPRICEFIVE']]
                part_data['SHAREHOLDPRICEONE'] = [round(x, 6) for x in part_data['SHAREHOLDPRICEONE']]
                part_data['SHAREHOLDPRICETEN'] = [round(x, 6) for x in part_data['SHAREHOLDPRICETEN']]
                part_data['SHAREHOLDSUM'] = [round(x, 6) for x in part_data['SHAREHOLDSUM']]
                part_data.replace('-', 0.0).to_sql(SouthernCashFlow.table_name(), engine,
                                                   schema=SouthernCashFlow.schema(),
                                                   index=False,
                                                   if_exists='append')
                print('存储第%d页数据结束!' % p)
                print('*' * 300)
            else:
                print('数据抓取为空,不进行数据保存!')

    elif spider_type == 'specific' and today is not None and previous_days is not None:
        periods = rq.get_trading_dates((today - timedelta(previous_days)).strftime('%Y-%m-%d'),
                                       today.strftime('%Y-%m-%d'))
        previous_days_set = list(range(len(periods)+1, 0, -1))
        for period, previous_days in zip(periods, previous_days_set):
            period = period.strftime('%Y-%m-%d')
            for p in range(1, end_rounds):
                part_data = pd.DataFrame()
                try:
                    response = requests.get('http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get',
                                            headers=headers,
                                            params=set_specific_period_params(p, today=today, current_day=period,
                                                                              previous_days=previous_days),
                                            cookies=cookies, verify=False)
                    temp = response.text.replace('/', '').replace("'", '"')
                    data = json.loads(temp)['data']
                    if len(data) == 0:
                        print('%s全部数据抓取结束,进入下一个日期轮询!' % period)
                        break
                    print(data)
                    for i in data:
                        part_data = pd.concat([part_data, pd.DataFrame([i])])

                except requests.exceptions.ConnectionError:
                    print('%s日期第%d页抓取出错!重新抓取%s日期第%d页期的数据' % (period, p
                                                             , period, p))
                    while True:
                        response = requests.get('http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get',
                                                headers=headers,
                                                params=set_specific_period_params(p, today=today,
                                                                                  previous_days=previous_days),
                                                cookies=cookies, verify=False)
                        if response.status_code == 200:
                            break
                    temp = response.text.replace('/', '').replace("'", '"')
                    data = json.loads(temp)['data']
                    if len(data) == 0:
                        break
                    print(data)
                    for i in data:
                        part_data = pd.concat([part_data, pd.DataFrame([i])])
                print('%s日期第%d页数据抓取结束!' % (period, p))
                print('*' * 300)
                if part_data.shape[0] != 0:
                    print('开始存储%s日期第%d页数据!' % (period, p))
                    part_data['SHAREHOLDPRICE'] = [round(x, 6) for x in part_data['SHAREHOLDPRICE']]
                    part_data['SHAREHOLDPRICEFIVE'] = [round(x, 6) for x in part_data['SHAREHOLDPRICEFIVE']]
                    part_data['SHAREHOLDPRICEONE'] = [round(x, 6) for x in part_data['SHAREHOLDPRICEONE']]
                    part_data['SHAREHOLDPRICETEN'] = [round(x, 6) for x in part_data['SHAREHOLDPRICETEN']]
                    part_data['SHAREHOLDSUM'] = [round(x, 6) for x in part_data['SHAREHOLDSUM']]
                    part_data.replace('-', 0.0).to_sql(SouthernCashFlow.table_name(), engine,
                                                       schema=SouthernCashFlow.schema(),
                                                       index=False,
                                                       if_exists='append')
                    print('存储%s日期第%d页数据结束!' % (period, p))
                    print('*' * 300)
        print('全部日期区间下的数据抓取完成!')
    else:
        raise ValueError('You can only input "daily"/"30days"/"specific"!')


if __name__ == "__main__":
    # part_data = main('daily')
    # part_data.to_csv(
    #     'C:\\Users\\omaka\\Desktop\\temp\\%s_HK_stock_change.csv' % (datetime.today() - timedelta(1)).strftime(
    #         '%Y-%m-%d'), encoding='gb18030')
    rq.init()
    engine = EnginePointer.get_engine()
    session = sessionmaker(bind=engine)()
    last_record = pd.to_datetime(Query(func.max(SouthernCashFlow.HDDATE), session).one_or_none()[0])
    if last_record is None:
        end_date = datetime.today() - timedelta(1)
        start_day = rq.get_trading_dates(end_date - timedelta(30), end_date)[0].strftime('%Y-%m-%d')
        end_date = end_date.strftime('%Y-%m-%d')
        print('由于数据库为空,开始抓取%s-%s之间的历史数据' % (start_day, end_date))
        main('30days', engine=engine)
    else:
        end_date = datetime.today().date()-timedelta(1)
        if end_date.weekday() >= 4 and last_record.date()!=end_date:
            end_date=datetime.today().date()-timedelta(3)
            print('前一天是周末,将爬取上周五的数据')
            main('specific', today=datetime.today(), previous_days=3)
        elif last_record.date() != end_date:
            print('开始抓取和保存%s日期下的数据' % (end_date.strftime('%Y-%m-%d')))
            main('daily', engine=engine)
        else:
            print('数据库中已经存在%s日期下的数据,将不执行爬虫程序!' % end_date)

    # 爬取指定时间区间内的数据
    # main('specific',today=datetime.today(),previous_day=3)
