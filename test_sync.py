import requests
from lxml import etree
from utils import create_headers
import numpy as np
url = "https://sh.lianjia.com/ershoufang/yangjing/pg2/"
html = requests.get(url,headers=create_headers()).text
root = etree.HTML(html)
eles = root.xpath('//ul[@class="sellListContent"]/li/div[@class="info clear"]')
title = [next(iter(ele.xpath("div[@class='title']/a/text()")),"None") for ele in eles]
selling_point = [next(iter(ele.xpath("div[@class='tag']/span[@class='taxfree']/text()")),"None") for ele in eles]
price = [next(iter(ele.xpath("div[@class='priceInfo']/div[1]/span/text()")),"None") for ele in eles]  
unitPrice = [next(iter(ele.xpath("div[@class='priceInfo']/div[2]/span/text()")),"None") for ele in eles]  
house_infos_ =  [next(iter(ele.xpath("div[@class='address']/div[@class='houseInfo']/text()")),"None") for ele in eles]
house_infos_aft = [it.split("|")  for it in house_infos_ ]
house_infos_aft_new = []
#修正缺失导致的错误问题
for it in house_infos_aft:
    if len(it) < 7:
        house_infos_aft_new.append(it + ['None' for _ in range(7 - len(it))])
    else:
        house_infos_aft_new.append(it[:7])      
house_infos = np.array(house_infos_aft_new,dtype=object)
# print(house_infos)
house_type = house_infos[:,0]
area = [a.replace("平米","")for a in house_infos[:,1]]
face = house_infos[:,2]
# print(face)
furnish = house_infos[:,3]
floor = house_infos[:,4]
num = house_infos[:,4]
date = house_infos[:,5]
houseProperty = house_infos[:,6]

community = [next(iter(ele.xpath("div[@class='flood']/div[@class='positionInfo']/a[1]/text()")),"None") for ele in eles]
region = [next(iter(ele.xpath("div[@class='flood']/div[@class='positionInfo']/a[2]/text()")),None) for ele in eles]

follow = [next(iter(ele.xpath("div[@class='followInfo']/text()")),"None") for ele in eles]
link = [next(iter(ele.xpath("div[@class='title']/a/@href")),"None") for ele in eles]
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
    "关注": follow,
    '房屋属性': houseProperty,
    '详情页': link,
}
print(dit)