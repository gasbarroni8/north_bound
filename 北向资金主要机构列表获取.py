import requests
from bs4 import BeautifulSoup
import re
import urllib
from datetime import datetime


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
            path = head_link + path[head_local:][date_local:name_local] + 'date=%s' % today
            final_path.append(path)
        if len(final_path) == 0:
            return None
        return final_path


if __name__ == '__main__':
    # 获取所有公司列表和公司连接
    company_list, filtered_list = NorthernCF.get_html_company_link_names(
        url='http://data.eastmoney.com/hsgtcg/InstitutionQueryMore.aspx')
    # 获取每个机构的详细持股连接
    secondary_web_link = NorthernCF.get_relative_detail_link(filtered_list=filtered_list)
