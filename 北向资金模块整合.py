import pandas as pd
import json
import requests
from bs4 import BeautifulSoup
import re
import urllib
from datetime import datetime, timedelta
import time

def set_signal_institution_cookies_and_headers(jgcode, today):
    today = pd.to_datetime(today).strftime('%Y-%m-%d')
    cookies = {
        'intellpositionL': '617.2px',
        'cowCookie': 'true',
        'qgqp_b_id': 'e9d32f3f05c5e642392711e751ca0d2c',
        'st_si': '73375257205011',
        'cowminicookie': 'true',
        'st_asi': 'delete',
        'st_pvi': '43322278274401',
        'st_sp': '2020-08-20%2014%3A30%3A53',
        'st_inirUrl': 'http%3A%2F%2Fdata.eastmoney.com%2Fhsgtcg%2FStockStatistics.aspx',
        'st_sn': '61',
        'st_psi': '20200821113059898-113300301532-5423142304',
        'intellpositionT': '1015px',
    }

    headers = {
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36',
        'Accept': '*/*',
        'Referer': 'http://data.eastmoney.com/hsgtcg/InstitutionHdDetail.aspx?jgCode=%s&date=%s' % (jgcode, today),
        'Accept-Language': 'zh-CN,zh;q=0.9',
    }

    return cookies, headers


def set_signal_institution_params(p, jgcode, today=datetime.today() - timedelta(1)):
    if not p:
        raise ValueError('Page number must be set up!')
    today = pd.to_datetime(today).strftime('%Y-%m-%d')
    params = (
        ('token', '70f12f2f4f091e459a279469fe49eca5'),
        ('st', 'HDDATE,SHAREHOLDPRICE'),
        ('sr', '3'),
        ('p', '%d' % int(p)),
        ('ps', '50'),
        ('js', '{"data":(x)}'),
        ('filter', '(PARTICIPANTCODE=\'%s\')(MARKET in (\'001\',\'003\'))(HDDATE=^%s^)' % (jgcode, today)),
        ('type', 'HSGTNHDDET'),
        ('rt', '53266022'),
    )
    return params


# 单个机构近30日北向持股收集
def set_cookies_and_headers_and_params(jgcode):
    cookies = {
        'intellpositionL': '617.2px',
        'cowCookie': 'true',
        'qgqp_b_id': 'e9d32f3f05c5e642392711e751ca0d2c',
        'st_si': '73375257205011',
        'cowminicookie': 'true',
        'st_asi': 'delete',
        'st_pvi': '43322278274401',
        'st_sp': '2020-08-20%2014%3A30%3A53',
        'st_inirUrl': 'http%3A%2F%2Fdata.eastmoney.com%2Fhsgtcg%2FStockStatistics.aspx',
        'st_sn': '58',
        'st_psi': '20200821111732473-0-3177437708',
        'intellpositionT': '1155px',
    }

    headers = {
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36',
        'Accept': '*/*',
        'Referer': 'http://data.eastmoney.com/hsgtcg/InstitutionhdStatistics.aspx?jgCode=%s' % jgcode,
        'Accept-Language': 'zh-CN,zh;q=0.9',
    }
    params = (
        ('type', 'HSGTCOMSTA'),
        ('token', '70f12f2f4f091e459a279469fe49eca5'),
        ('st', 'HDDATE'),
        ('sr', '-1'),
        ('p', '1'),
        ('ps', '50'),
        ('js', '{"data":(x)/}'),
        ('filter', '(PARTICIPANTCODE=\'%s\')(MARKET=\'N\')' % jgcode),
        ('rt', '53265996'),
    )
    return cookies, headers, params


class NorthernCF(object):
    @staticmethod
    def get_html_company_link_names(url, encoding_method='gb18030', label='a', attr='href',
                                    link_head='http://data.eastmoney.com'):

        response = requests.get(url)

        response.encoding = encoding_method

        html = response.text

        soup = BeautifulSoup(html, 'html.parser')
        # 获取所有herf地址
        total_heft = []
        for target in soup.find_all(label):
            try:
                value = target.get(attr)
            except:
                value = ''
            if value:
                total_heft.append(value)
        # 过滤
        filtered_list = list(filter(lambda x: re.match(r'/hsgtcg/(\w+)\.(\w+)\?(\w+)\=(\w+)', x) != None, total_heft))
        # 拼接所有公司HTML网址
        filtered_list = list(map(lambda x: link_head + x, filtered_list))
        # 获取公司名列表
        company_list = list(map(lambda x: re.findall(r'\=(\D+)', x)[-1], filtered_list))

        if len(company_list) > 0 and len(filtered_list) > 0:
            return company_list, filtered_list
        else:
            return None, None

    @staticmethod
    def get_relative_detail_link(filtered_list=None,
                                 head_link='http://data.eastmoney.com/hsgtcg/InstitutionHdDetail.aspx?'):
        if filtered_list is None:
            raise ValueError('filtered_list cannot be empty!')
        final_path = []
        today = datetime.today().strftime('%Y-%m-%d')
        for path in filtered_list:
            head_local = path.find('?')
            date_local = path[head_local:].find('jgCode=')
            name_local = path[head_local:].find('jgName=')
            print(path[head_local:][date_local:name_local])
            path = head_link + path[head_local:][date_local:name_local] + 'date=%s' % today
            final_path.append(path)
        if len(final_path) == 0:
            return None
        return final_path

    @staticmethod
    def get_current_institution_holding_detail(jgcode, start_date=None, end_date=None,
                                               get_data_method='daily', iter=1000):
        # 每次调用此函数一次即可
        cookies, headers = NorthernCF().__get_cookies_headers(jgcode, start_date, end_date, get_data_method)

        print('开始抓取%s历史数据' % jgcode)
        if get_data_method == 'specify':
            part_data = pd.DataFrame()
            for p in range(1, iter):
                print('正在抓取公司jgcode=%s下第%d页历史北向持有数据!' % (jgcode, int(p)))
                try:
                    response = requests.get('http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get',
                                            headers=headers,
                                            params=NorthernCF().__get_params(p, jgcode=jgcode, start_date=start_date,
                                                                             end_date=end_date,
                                                                             get_data_method=get_data_method),
                                            cookies=cookies, verify=False)
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
                                                params=NorthernCF().__get_params(p, jgcode=jgcode,
                                                                                 start_date=start_date,
                                                                                 end_date=end_date,
                                                                                 get_data_method=get_data_method),
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
        elif get_data_method == 'daily':
            part_data = pd.DataFrame()
            for p in range(1, iter):
                print('正在抓取公司jgcode=%s下第%d页历史北向持有数据!' % (jgcode, int(p)))
                try:
                    response = requests.get('http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get',
                                            headers=headers,
                                            params=NorthernCF().__get_params(p, jgcode=jgcode, start_date=None,
                                                                             end_date=end_date,
                                                                             get_data_method=get_data_method),
                                            cookies=cookies, verify=False)
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
                                                params=NorthernCF().__get_params(p, jgcode=jgcode, start_date=None,
                                                                                 end_date=end_date,
                                                                                 get_data_method=get_data_method),
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
        return part_data

    def __get_params(self, p, jgcode, start_date, end_date, get_data_method):
        if get_data_method == 'daily':
            params = (
                ('token', '70f12f2f4f091e459a279469fe49eca5'),
                ('st', 'HDDATE,SHAREHOLDPRICE'),
                ('sr', '-1'),
                ('p', '%d' % int(p)),
                ('ps', '50'),
                ('js', '{"data":(x)/}'),
                ('filter',
                 '(PARTICIPANTCODE=\'%s\')(MARKET in (\'001\',\'003\'))(HDDATE=^%s^)' % (jgcode, end_date)),
                ('type', 'HSGTNHDDET'),
                ('rt', '53277419'),
            )
        elif get_data_method == 'specify':
            params = (
                ('token', '70f12f2f4f091e459a279469fe49eca5'),
                ('st', 'HDDATE,SHAREHOLDPRICE'),
                ('sr', '-1'),
                ('p', '%d' % int(p)),
                ('ps', '50'),
                ('js', '{"data":(x)/}'),
                ('filter',
                 '(PARTICIPANTCODE=\'%s\')(MARKET in (\'001\',\'003\'))(HDDATE>=^%s^ and HDDATE<=^%s^)' % (
                     jgcode, start_date, end_date)),
                ('type', 'HSGTNHDDET'),
                ('rt', '53277419'),
            )

        else:
            raise ValueError('Function only supports with "daily" and "specify"!')

        return params

    def __get_cookies_headers(self, jgcode, star_date=None, end_date=None, get_data_method='specify'):

        if get_data_method == 'specify':
            start_date, end_date = pd.to_datetime(star_date).strftime('%Y-%m-%d'), \
                                   pd.to_datetime(end_date).strftime('%Y-%m-%d')
            cookies = {
                'cowminicookie': 'true',
                'cowCookie': 'true',
                'HAList': 'a-sh-600037-%u6B4C%u534E%u6709%u7EBF',
                'em_hq_fls': 'js',
                'qgqp_b_id': 'eec5e5aee09ab30b4cce02434a4501c5',
                '_qddaz': 'QD.nrlnaz.z3zjgk.ke83twi9',
                'pgv_pvi': '1785103360',
                'st_si': '95272343297610',
                'st_asi': 'delete',
                'intellpositionL': '832.607px',
                'intellpositionT': '1050.81px',
                'st_pvi': '34673485105609',
                'st_sp': '2020-08-20%2011%3A03%3A10',
                'st_inirUrl': 'http%3A%2F%2Fquote.eastmoney.com%2Fcenter%2Fgridlist.html',
            }

            headers = {
                'Connection': 'keep-alive',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36 Edg/84.0.522.63',
                'Accept': '*/*',
                'Referer': 'http://data.eastmoney.com/hsgtcg/InstitutionHdDetail.aspx?jgCode=%s&date=%s' % (
                    jgcode, end_date),  # 网页上以时间结束日开始往前推
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            }

        elif get_data_method == 'daily':
            cookies = {
                'cowminicookie': 'true',
                'cowCookie': 'true',
                'HAList': 'a-sh-600037-%u6B4C%u534E%u6709%u7EBF',
                'em_hq_fls': 'js',
                'qgqp_b_id': 'eec5e5aee09ab30b4cce02434a4501c5',
                '_qddaz': 'QD.nrlnaz.z3zjgk.ke83twi9',
                'pgv_pvi': '1785103360',
                'st_si': '95272343297610',
                'st_asi': 'delete',
                'intellpositionL': '832.607px',
                'intellpositionT': '1050.81px',
                'st_pvi': '34673485105609',
                'st_sp': '2020-08-20%2011%3A03%3A10',
                'st_inirUrl': 'http%3A%2F%2Fquote.eastmoney.com%2Fcenter%2Fgridlist.html',

            }

            headers = {
                'Connection': 'keep-alive',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36 Edg/84.0.522.63',
                'Accept': '*/*',
                'Referer': 'http://data.eastmoney.com/hsgtcg/InstitutionHdDetail.aspx?jgCode=%s&date=%s' % (
                    jgcode, end_date),  # 网页上以时间结束日开始往前推
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            }

        else:
            raise ValueError('Function only supports with "daily" and "specify"!')

        return cookies, headers

    @staticmethod
    def get_company_jscode(filtered_list):
        if filtered_list is None:
            raise ValueError('filtered_list cannot be empty!')
        jgcode_list = []
        for url in filtered_list:
            pattern = re.compile(r'jgCode=(\w+)')
            jgcode = pattern.findall(url)
            jgcode_list.append(jgcode[0])
        return jgcode_list


if __name__ == "__main__":
    # 获取所有公司列表和公司连接
    company_list, filtered_list = NorthernCF.get_html_company_link_names(
        url='http://data.eastmoney.com/hsgtcg/InstitutionQueryMore.aspx')
    jgcode_list = NorthernCF.get_company_jscode(filtered_list)
    # 单个机构单日抓取北向持股
    cookies, headers = set_signal_institution_cookies_and_headers('B01345', '2020-08-21')
    final_data1 = pd.DataFrame()
    for p in range(1, 1000):
        print(p)
        try:
            response = requests.get('http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get', headers=headers,
                                    params=set_signal_institution_params(p, 'B01345', '2020-08-21'), cookies=cookies,
                                    verify=False)
            temp = response.text.replace('/', '').replace("'", '"')
            data = json.loads(temp)['data']
            if len(data) == 0:
                break
            for local_data in data:
                final_data1 = pd.concat([final_data1, pd.DataFrame([local_data])])
        except requests.ConnectionError:
            while True:
                response = requests.get('http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get',
                                        headers=headers,
                                        params=set_signal_institution_params(p, 'B01345', '2020-08-21'),
                                        cookies=cookies, verify=False)
                if response.status_code == 200:
                    break
            temp = response.text.replace('/', '').replace("'", '"')
            data = json.loads(temp)['data']
            if len(data) == 0:
                break
            for local_data in data:
                final_data1 = pd.concat([final_data1, pd.DataFrame([local_data])])

    print(final_data1)
    print('-' * 200)
    final_data.to_csv('C:\\Users\\omaka\\Desktop\\temp\\singal_institution_holding.csv',encoding='gb18030')
    获取单个机构近期北向持股市值变化
    cookies, headers, params = set_cookies_and_headers_and_params('B01345')
    response = requests.get('http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get', headers=headers,
                            params=params, cookies=cookies, verify=False)
    temp = response.text.replace('/', '').replace("'", '"')
    data = json.loads(temp)['data']
    final_data2 = pd.DataFrame()
    for local_data in data:
        final_data2 = pd.concat([final_data2, pd.DataFrame([local_data])])
    print(final_data2)
    print('-' * 200)
    # 获取每个机构的详细持股连接
    secondary_web_link = NorthernCF.get_relative_detail_link(filtered_list=filtered_list)
    current_history_data=NorthernCF.get_current_institution_holding_detail('B01555', start_date='2020-07-01', end_date='2020-08-24',
                                                            get_data_method='specify', iter=1000)
    current_history_data.to_csv('C:\\Users\\omaka\\Desktop\\temp\\current_history_data.csv',encoding='gb18030',index=False)
    daily_history_data=NorthernCF.get_current_institution_holding_detail('B01555', start_date='2020-07-01', end_date='2020-08-24',
                                                            get_data_method='daily', iter=1000)
    daily_history_data.to_csv('C:\\Users\\omaka\\Desktop\\temp\\daily_history_data.csv',encoding='gb18030',index=False)
    # 获取159加机构所有t-1天的历史北向资金持股数据
    start=time.time()
    total_1day_northern_data=pd.DataFrame()
    for jgcode in jgcode_list:
        current_history_data=NorthernCF.get_current_institution_holding_detail(jgcode, start_date='2020-07-01', end_date='2020-08-24',
                                                                               get_data_method='daily', iter=1000)
        total_1day_northern_data=pd.concat([total_1day_northern_data,current_history_data])
    total_1day_northern_data.to_csv('C:\\Users\\omaka\\Desktop\\temp\\total_1day_northern_data.csv',encoding='gb18030',index=False)
    end=time.time()
    t=end-start
    print('用时%0.5f分钟,%0.5f秒' % (t//60,t%60))