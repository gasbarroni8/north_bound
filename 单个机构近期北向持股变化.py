import requests
import pandas as pd
import json

# 单个机构近30日北向持股收集
def set_cookies_and_headers(jgcode):
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
        ('filter', '(PARTICIPANTCODE=\'B01555\')(MARKET=\'N\')'),
        ('rt', '53265996'),
    )
    return cookies,headers,params

if __name__=="__main__":
    cookies, headers, params=set_cookies_and_headers('B01555')
    response = requests.get('http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get', headers=headers, params=params, cookies=cookies, verify=False)
    temp = response.text.replace('/', '').replace("'", '"')
    data=json.loads(temp)['data']
    final_data=pd.DataFrame()
    for local_data in data:
        final_data=pd.concat([final_data,pd.DataFrame([local_data])])
    print(final_data)