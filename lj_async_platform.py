import asyncio
import aiohttp
import time
from lxml import etree
# from itertools import product
from tqdm import tqdm
import time
import pandas as pd
import numpy as np
import random
import json
# from utils import verbose_exception,create_headers
# import re
import logging 
# from bs4 import BeautifulSoup

logging.basicConfig(filename='app.log',
                    level=logging.INFO,
                    filemode="w", 
                    format='%(asctime)s - %(levelname)s - %(lineno)d - %(module)s - %(funcName)s - %(message)s')

TIMEOUT = 120
colomuns=[
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
    "关注",
    '房屋属性',
    '详情页',
]


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
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
]

async def get_one_page(session, url,semaphore):
    dit = {
                '标题': None,
                '卖点': None,
                '总价': None,
                '单价': None,
                '户型': None,
                '楼层': None,
                '共有楼层数': None,
                '装修': None,
                '朝向': None,
                '建造时间': None,
                '面积': None,
                '小区': None,
                '区域': None,
                "关注": None,
                '房屋属性': None,
                '详情页': None,}
    #异步session
    #随机延迟
    headers = {"User-Agent":random.choice(USER_AGENTS),
               "Referer":"https://sh.lianjia.com/",
            #    "Accept-Encoding":"gzip, deflate, br, zstd",
            #    "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            #    "Connection":"keep-alive",
            #    "Host":"sh.lianjia.com",
            #    "Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            #    "Cookie":"lianjia_uuid=ddbe60c3-74a2-44bb-a86d-98e72b1706c3; _smt_uid=665e8096.1d5e6a01; Hm_lvt_9152f8221cb6243a53c83b956842be8a=1717469334; _jzqc=1; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2218fe1264c3edb1-0124604a9f69e5-4c657b58-2073600-18fe1264c3f3025%22%2C%22%24device_id%22%3A%2218fe1264c3edb1-0124604a9f69e5-4c657b58-2073600-18fe1264c3f3025%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_referrer%22%3A%22%22%2C%22%24latest_referrer_host%22%3A%22%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%7D%7D; _ga=GA1.2.1265665742.1717469336; _gid=GA1.2.2007536876.1717469336; _ga_654P0WDKYN=GS1.2.1717469336.1.0.1717469336.0.0.0; _qzjc=1; select_city=310000; _jzqa=1.2325446619701753300.1717469334.1717475770.1717560778.3; _jzqckmp=1; Hm_lpvt_9152f8221cb6243a53c83b956842be8a=1717564925; _qzja=1.1933495970.1717469365462.1717475770299.1717560778123.1717564316016.1717564925578.0.0.0.23.3; _qzjto=11.1.0; srcid=eyJ0Ijoie1wiZGF0YVwiOlwiNDllZGM3ZTk3NmFiN2MzYzkwMDdiOTczMDUzOTM2ZjkzZWNhOGMyYjRiNzI5ZWI2YjQxOGEyNzc0MjE2YWZhMTViMGZjNDMwMTFjYTU4MWJiNWY4Y2MyMTRiZGQ0MTdlNTVkYjE2N2IzOGY5ZWJjNjM4N2U2OGYzYWU1Y2I5ZWI2ODY4ZDQyZDRiNjFmNzczOTQwYjRjMTkxZDg1MDAzNWQwMWE3ODM4ZmQzNWFkMTg1YTk2NjQ0YzMyYmZhOTQ5NDA1MjNhYmUwZjI5MjA0YzgwZDA4NTg3OTNhZTZiN2VjNmM4ZTU0NzBlNjNhNmNiMjJkYjBjMjY3ZTQyNmRkOVwiLFwia2V5X2lkXCI6XCIxXCIsXCJzaWduXCI6XCI3Y2FiMmZlYVwifSIsInIiOiJodHRwczovL3NoLmxpYW5qaWEuY29tL2Vyc2hvdWZhbmcvZ2FvaGFuZy9wZzM1LyIsIm9zIjoid2ViIiwidiI6IjAuMSJ9; _ga_LRLL77SF11=GS1.2.1717561392.3.1.1717564933.0.0.0; _ga_GVYN2J1PCG=GS1.2.1717561392.3.1.1717564933.0.0.0"
               
}
    async with semaphore:
        async with session.get(url, 
                            headers=headers,
                            timeout=TIMEOUT,) as resp:
            # resp.encoding = 'utf-8'
            await asyncio.sleep(random.randint(1,3) * 3)
            html = await resp.text()
            
            if resp.status != 200:
                logging.error(f"{resp.status}")
                logging.info(f'{url}')
                return dit


            root = etree.HTML(html)
            eles = root.xpath('//ul[@class="sellListContent"]/li/div[@class="info clear"]')
            try:    
                # print(len(eles))
                title = [next(iter(ele.xpath("div[@class='title']/a/text()")),"None") for ele in eles]
                # logging.info(title)
                # print(eles[1].xpath("//div[@class='title']/a/text()"))
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
            except Exception as e:
                # verbose_exception(e)
                logging.error(e)
                # print(house_infos_aft)
    return dit

async def get_pages(urls,max_concurrency=30,ban_kuai="",semaphore=None):
    s = time.time()
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=max_concurrency),) as session:
        tasks = [get_one_page(session, url,semaphore) for url in urls]
        results_ = await asyncio.gather(*tasks)
    # logging.info(results_)
    try:
        df = pd.concat([pd.DataFrame(result) for result in results_])
        df.to_excel(f"outputs/output__{ban_kuai}.xlsx",index=False)
    except Exception as e:
        logging.error(e)
    logging.info(f"get_pages Total time cost: {time.time() -  s}")


async def job(max_concurrency,semaphore):
    s = time.time()
    try:
        with open(urls_json, "r") as f:
            urls_dict = json.load(f)
    except Exception as e:

        logging.error(e)
        raise Exception("no json")
    tasks = []
    for ban_kuai,urls in urls_dict.items():
        # print(urls)
        # if ban_kuai == "yangjing":
        #     urls = urls[:3]
        tasks.append(get_pages(urls,max_concurrency=max_concurrency,ban_kuai=ban_kuai,semaphore=semaphore))
    
    length = len(tasks)
    with tqdm(total=length, desc="Fetching bankuai pages") as pbar:
        for task in asyncio.as_completed(tasks):
            await task
            pbar.update()
    
    logging.info(f"whole job Total time cost: {time.time() -  s}")
    
    
if __name__ == "__main__":
    province_code = "sh" #shanghai
    max_concurrency = 20
    semaphore = asyncio.Semaphore(500)
    urls_json = "fruits.json"
    asyncio.run(job(max_concurrency,semaphore))