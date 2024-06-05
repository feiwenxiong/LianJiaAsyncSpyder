import requests
import parsel
import re
import csv
import urllib3
import logging
logging.captureWarnings(True)
urllib3.disable_warnings()
requests.packages.urllib3.disable_warnings()
# import asyncio

f = open('二手房多页gz111.csv', mode='a', encoding='utf-8', newline='')
csv_writer = csv.DictWriter(f, fieldnames=[
    '标题',
    '卖点',
    '总价',
    '单价',
    '户型',
    '楼层',
    '共有楼层数',
    '装修',
    '朝向',
    '建造时间',
    '面积',
    '小区',
    '区域',
    '所属区',
    '梯户比例',
    '是否有电梯',
    '房屋属性',
    '详情页',
])
csv_writer.writeheader()
"""
发送请求
    1. 确定请求网址是什么
    2. 请求方式
    3. 伪装模拟浏览器
        headers >>> 请求头加什么数据, 怎么找呢?
            User-Agent: 用户代理 表示浏览器基本身份标识... <相当于你进超市, 要看健康码或者戴口罩>
        如果你不加headers对于某些网站, 你可能被识别出来是你爬虫程序, 被反爬 >>> 得不到数据
        headers 字典数据类型
"""


for page in range(1, 20):
    url = f'https://sh.lianjia.com/ershoufang/pg{page}/'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36 Edg/101.0.1210.53'
    }
    response = requests.get(url=url, headers=headers,verify=False)
    # print(response.text)
    selector = parsel.Selector(response.text)
    # 真正的掌握css选择器解析方法 在系统课程都是需要学习2.5个小时左右
    href = selector.css('.sellListContent li.clear .title a::attr(href)').getall()
    for link in href:
        # url = 'https://cs.lianjia.com/ershoufang/104108664407.html'
        # 发送请求
        response = requests.get(url=link, headers=headers)
        # print(response)  # <Response [200]> 响应对象 200 状态码表示请求成功
        # 获取数据
        # print(response.text)
        """
        解析数据
            css选择器 >>> 根据标签属性内容提取数据
        """
        selector_1 = parsel.Selector(response.text)  # 需要把获取html字符串数据转成selector对象
        # print(selector)
        # 复制下来仅仅只是定位到标签, 我获取标签里面title属性
        try:
            # body > div.sellDetailHeader > div > div > div.title > h1
            title = selector_1.css('.main::text').get()  # 标题
            selling_point = selector_1.css('.sub::text').get()  # 卖点
            price = selector_1.css('.price .total::text').get()  # 总价
            unitPrice = selector_1.css('.unitPrice .unitPriceValue::text').get()  # 单价
            house_type = selector_1.css('.room .mainInfo::text').get()  # 户型
            subInfo = selector_1.css('.room .subInfo::text').get().split('/')  # 楼层
            floor = subInfo[0]  # 楼层
            num = re.findall('\d+', subInfo[1])[0]  # 共有楼层数
            furnish = selector_1.css('.type .subInfo::text').get().split('/')[-1]  # 装修
            face = selector_1.css('.type .mainInfo::text').get()  # 朝向
            date = re.findall('\d+', selector_1.css('.area .subInfo::text').get())  # 建造时间
            if len(date) == 0:
                date = '0'
            else:
                date = date[0]
            area = selector_1.css('.area .mainInfo::text').get().replace('平米', '')  # 面积
            community = selector_1.css('.communityName .info::text').get()  # 小区
            areaName_info = selector_1.css('.areaName .info a::text').getall()  # 区域
            areaName = areaName_info[0]  # 所属区
            region = areaName_info[1]  # 区域
            scale = selector_1.css('div.content ul li:nth-child(10)::text').get()  # 梯户比例
            elevator = selector_1.css('div.content ul li:nth-child(11)::text').get()  # 是否有电梯
            houseProperty = selector_1.css('div.content  li:nth-child(2) span:nth-child(2)::text').get()  # 房屋属性
            dit = {
                '标题': title,
                '卖点': selling_point,
                '总价': price,
                '单价': unitPrice,
                '户型': house_type,
                '楼层': floor,
                '共有楼层数': num,
                '装修': furnish,
                '朝向': face,
                '建造时间': date,
                '面积': area,
                '小区': community,
                '区域': region,
                '所属区': areaName,
                '梯户比例': scale,
                '是否有电梯': elevator,
                '房屋属性': houseProperty,
                '详情页': link,
            }
            csv_writer.writerow(dit)
            print(
                title, selling_point, price, unitPrice, house_type, subInfo, furnish, face,
                date, area, community, region, scale, elevator, houseProperty, link
            )
        except:
            pass

