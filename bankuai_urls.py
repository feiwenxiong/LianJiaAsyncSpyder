import asyncio
import aiohttp
import time
from lxml import etree
from itertools import product
from tqdm import tqdm
import time
import pandas as pd
import numpy as np
import random
import json
from utils import verbose_exception,create_headers
import re
import logging 
logging.basicConfig(filename='app.log',
                    level=logging.ERROR, 
                    filemode="w",
                    format='%(asctime)s - %(levelname)s - %(lineno)d - %(module)s - %(funcName)s - %(message)s')
TIMEOUT = 10
async def get_total_page(province_code, url, max_concurrency, max_retries=2, retry_delay=1):
    total_page = 50
    headers = create_headers()
    headers["Referer"] = f"https://{province_code}.lianjia.com/"

    for retry in range(max_retries + 1):
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=max_concurrency)) as ss:
                async with ss.get(url,
                                  headers=headers,
                                  timeout=TIMEOUT) as resp:
                    await asyncio.sleep(random.randint(1, 3) * 1)
                    html = await resp.text()

                    if resp.status != 200:
                        resp.raise_for_status()

                    root = etree.HTML(html)
                    ele = root.xpath("//div[@page-data]")[0]
                    strings = ele.get("page-data")
                    matches = re.search('.*"totalPage":(\d+),.*', strings)
                    total_page = int(matches.group(1))
                    break  # 成功获取到数据，跳出循环
        except Exception as e:
            if retry < max_retries:
                # verbose_exception(e)
                logging.error(f"{e}_:{retry}次")
                await asyncio.sleep(retry_delay)  # 延时后再重试
            else:
                # verbose_exception(e)
                logging.error("MAX RETRIES")
                # raise e  # 超过最大重试次数，抛出异常

    return total_page

async def fetch_lianjia_pages_urls(province_code, ban_kuai, start_page, max_concurrency):
    url_base = f'https://{province_code}.lianjia.com/ershoufang/{ban_kuai}/pg1/'
    total_page = await get_total_page(province_code,url_base,max_concurrency)
    urls = [f'https://{province_code}.lianjia.com/ershoufang/{ban_kuai}/pg{page}/' for page in range(start_page,total_page + 1)]
    return {ban_kuai: urls}


async def job(province_code,max_concurrency):
    s = time.time()
    start_page = 1
    tasks = []
    try:
        with open(f"{province_code}.json", "r") as f:
            dists_bankuai = json.load(f)
    except Exception as e:
        # verbose_exception(e)
        logging.error(e)
        raise Exception("no json")
    #
    for dist,ban_kuais in dists_bankuai.items():
        for ban_kuai in ban_kuais:
            tasks.append(fetch_lianjia_pages_urls(province_code, ban_kuai, start_page, max_concurrency))
    results_ = {}
    with tqdm(total=len(tasks), desc="Fetching urls") as pbar:
        for task in asyncio.as_completed(tasks):
            result = await task
            # print(result)
            results_.update(result.copy())
            pbar.update()
    #results = await asyncio.gather(*tasks)
    # 指定要保存的文件名
    filename = 'fruits.json'

    # 使用json.dump()方法将列表写入json文件
    with open(filename, 'w') as json_file:
        json.dump(results_, json_file)
    print("total time cost:", time.time() - s)


if __name__ == "__main__":
    province_code = "sh" #shanghai
    max_concurrency = 30
    asyncio.run(job(province_code,max_concurrency))