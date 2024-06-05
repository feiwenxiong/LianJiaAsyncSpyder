#!/usr/bin/env python
# coding=utf-8
# author: zengyuetian
# 此代码仅供学习与交流，请勿用于商业用途。
# 板块信息相关函数
import random
import requests
from lxml import etree
import json

SPIDER_NAME = "lianjia"

def create_headers():
    headers = {}
    USER_AGENTS = [
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
    "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
    "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
    "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
    "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
    "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
    "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",]
    headers["User-Agent"] = random.choice(USER_AGENTS)

    return headers

# if SPIDER_NAME == LIANJIA_SPIDER:
ERSHOUFANG_QU_XPATH = '//*[@id="filter-options"]/dl[1]/dd/div/a'
ERSHOUFANG_BANKUAI_XPATH = '//*[@id="filter-options"]/dl[1]/dd/div[2]/a'
XIAOQU_QU_XPATH = '//*[@id="filter-options"]/dl[1]/dd/div/a'
XIAOQU_BANKUAI_XPATH = '//*[@id="filter-options"]/dl[1]/dd/div[2]/a'
DISTRICT_AREA_XPATH = '//div[3]/div[1]/dl[2]/dd/div/div[2]/a'
CITY_DISTRICT_XPATH = '///div[3]/div[1]/dl[2]/dd/div/div/a'
# elif SPIDER_NAME == BEIKE_SPIDER:
#     ERSHOUFANG_QU_XPATH = '//*[@id="filter-options"]/dl[1]/dd/div/a'
#     ERSHOUFANG_BANKUAI_XPATH = '//*[@id="filter-options"]/dl[1]/dd/div[2]/a'
#     XIAOQU_QU_XPATH = '//*[@id="filter-options"]/dl[1]/dd/div/a'
#     XIAOQU_BANKUAI_XPATH = '//*[@id="filter-options"]/dl[1]/dd/div[2]/a'
#     DISTRICT_AREA_XPATH = '//div[3]/div[1]/dl[2]/dd/div/div[2]/a'
#     CITY_DISTRICT_XPATH = '///div[3]/div[1]/dl[2]/dd/div/div/a'
    
def get_district_url(city, district):
    """
    拼接指定城市的区县url
    :param city: 城市
    :param district: 区县
    :return:
    """
    return "http://{0}.{1}.com/xiaoqu/{2}".format(city, SPIDER_NAME, district)

chinese_city_district_dict = dict()     # 城市代码和中文名映射
chinese_area_dict = dict()              # 版块代码和中文名映射
area_dict = dict()


def get_chinese_district(en):
    """
    拼音区县名转中文区县名
    :param en: 英文
    :return: 中文
    """
    return chinese_city_district_dict.get(en, None)


def get_districts(city):
    """
    获取各城市的区县中英文对照信息
    :param city: 城市
    :return: 英文区县名列表
    """
    url = 'https://{0}.{1}.com/xiaoqu/'.format(city, SPIDER_NAME)
    headers = create_headers()
    response = requests.get(url, timeout=10, headers=headers)
    html = response.content
    root = etree.HTML(html)
    elements = root.xpath(CITY_DISTRICT_XPATH)
    en_names = list()
    ch_names = list()
    for element in elements:
        link = element.attrib['href']
        en_names.append(link.split('/')[-2])
        ch_names.append(element.text)

        # 打印区县英文和中文名列表
    for index, name in enumerate(en_names):
        chinese_city_district_dict[name] = ch_names[index]
        # print(name + ' -> ' + ch_names[index])
    return en_names
def get_areas(city, district):
    """
    通过城市和区县名获得下级板块名
    :param city: 城市
    :param district: 区县
    :return: 区县列表
    """
    page = get_district_url(city, district)
    areas = list()
    try:
        headers = create_headers()
        response = requests.get(page, timeout=10, headers=headers)
        html = response.content
        root = etree.HTML(html)
        links = root.xpath(DISTRICT_AREA_XPATH)

        # 针对a标签的list进行处理
        for link in links:
            relative_link = link.attrib['href']
            # 去掉最后的"/"
            relative_link = relative_link[:-1]
            # 获取最后一节
            area = relative_link.split("/")[-1]
            # 去掉区县名,防止重复
            if area != district:
                chinese_area = link.text
                chinese_area_dict[area] = chinese_area
                # print(chinese_area)
                areas.append(area)
        return areas
    except Exception as e:
        print(e)


def save_sh_json(city="sh"):
    dists = get_districts(city)
    search_dict = {}
    for d in dists:
        search_dict[d] = get_areas(city, d)
    print(search_dict)
    with open(f"{city}.json", "w") as f:
        json.dump(search_dict, f)
    return
    
def get_total_page():
    import re
    province_code = 'sh'
    ban_kuai = 'yangjing'
    total_page = page = 1
    url = f"https://{province_code}.lianjia.com/ershoufang/{ban_kuai}/pg{page}/"
    html = requests.get(url, headers=create_headers()).text
    root = etree.HTML(html)
    ele = root.xpath("//div[@page-data]")[0]
    # print(ele)
    strings = ele.get("page-data")
    # print(strings)
    try:
        matches = re.search('.*"totalPage":(\d+),.*', strings)
        total_page = int(matches.group(1))
        # print(total_page)
    except Exception as e:
        # print("\tWarning: only find one page for {0}".format(area_name))
        verbose_exception(e)
    return total_page


def verbose_exception(e):
    print(f'error file:{e.__traceback__.tb_frame.f_globals["__file__"]}')
    print(f"error line:{e.__traceback__.tb_lineno}")
    print(e)
    return 

if __name__ == "__main__":
    #save_sh_json() #如果要获取城市的数据，请注释掉
    #save_sh_json("bj") #北京
    pass