import requests
import json
import pandas as pd


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


# NB. Original query string below. It seems impossible to parse and
# reproduce query strings 100% accurately so the one below is given
# in case the reproduced version is not "correct".
# response = requests.get('http://dcfm.eastmoney.com//EM_MutiSvcExpandInterface/api/js/get?type=HSGTCJB&token=70f12f2f4f091e459a279469fe49eca5&sty=hgt&filter=(MarketType=1)(DetailDate=^2020-08-21^)&js=var%20xrRtEZXF=\{%22data%22:(x),%22pages%22:(tp)\}&pagesize=50&page=1&sr=-1&st=HGTCJJE&rt=53274854', headers=headers, cookies=cookies, verify=False)


# 深股通十大
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


if __name__ == '__main__':
    sh_top = get_top_ten_of_sh('2020-08-21')
    sz_top = get_top_ten_of_sz('2020-08-21')
    sh_top.to_csv('C:\\Users\\omaka\\Desktop\\临时保存文件夹\\sh_top.csv', encoding='gb18030',index=False)
    sz_top.to_csv('C:\\Users\\omaka\\Desktop\\临时保存文件夹\\sz_top.csv', encoding='gb18030',index=False)
