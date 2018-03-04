#!/usr/bin/python
# -*- coding:utf-8 -*-
import random
import time
import pymysql
import re
import os
import json
import requests
import datetime

import sys
from pyquery import PyQuery as pq
from fake_useragent import UserAgent
from urllib.parse import urlencode
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec

host = "127.0.0.1"
password = '123456'
db = "chartsite"
table = "农产品价格行情数据库"

Basepath = os.path.dirname(__file__)
index_path = os.path.join(Basepath, "index.json")
if sys.platform == 'win32':
    driver_path = os.path.join(Basepath, "selenium", "phantomjs-2.1.1-windows", "bin", "phantomjs.exe")
else:
    driver_path = os.path.join(Basepath, "selenium", "phantomjs-2.1.1-linux-x86_64", "bin", "phantomjs")

conn = pymysql.connect(host, 'root', password, db, charset="utf8", use_unicode=True)
cursor = conn.cursor()

start_url = 'http://nc.mofcom.gov.cn/channel/jghq2017/price_list.shtml'

category_code = {
    "13079": "畜产品",
    "13080": "水产品",
    "13073": "粮油",
    "13076": "果品",
    "13075": "蔬菜",
}

insert_sql = """
           INSERT INTO `{db}`.`{table}`(`类别`, `日期`, `产品`, `价格`, `市场`)
           VALUES (%s, %s,%s, %s, %s) ON DUPLICATE KEY UPDATE `产品`=VALUES (`产品`)""".format(table=table, db=db)

index = json.loads(open(index_path, 'r', encoding='utf-8').read())

f = open('record.txt', 'a+', encoding='utf-8')


def down(url, parse):
    code = parse["par_craft_index"]
    header = {
        'cache-control': "no-cache",
        'User-Agent': getattr(UserAgent(), 'random')
    }
    tiaoshu = 0
    try:
        res = requests.request("GET", url, headers=header, params=parse)
        url = res.url
        flag = int(res.status_code)
        responses = pq(res.text)
    except:
        driver = webdriver.PhantomJS(executable_path=driver_path)
        driver.get(url)
        time.sleep(0.5)
        flag = 200
        wait = WebDriverWait(driver, 30)
        wait.until(ec.presence_of_element_located((By.CSS_SELECTOR, ".table-01.mt30 tbody tr")))
        responses = driver.page_source
        responses = pq(responses)
    tables = responses.find(".table-01.mt30 tr").items()
    aa = 0
    for items in tables:
        aa += 1
        if aa > 1:
            items_value = [category_code[code], ]
            for value in items.find('td').text().split(" "):
                if value == '�|鱼':
                    value = "鮸鱼"
                if value == '�崭�':
                    value = "椪柑"
                items_value.append(value)
            if len(items_value) >= 6:
                if '/' in items_value[4]:
                    items_value.remove(items_value[4])
            feedback = cursor.execute(insert_sql, items_value)
            print(feedback, items_value)
            conn.commit()
            if feedback or not feedback:
                tiaoshu += 1
    huifu = [flag, tiaoshu]
    return huifu


def main(arg):
    try:
        num = 0
        today = datetime.date.today()
        today = today - datetime.timedelta(days=37)
        start_day = today - datetime.timedelta(days=90)
        driver = webdriver.PhantomJS(executable_path=driver_path)
        for item in arg:
            wait = WebDriverWait(driver, 30)
            category_id = item['id']
            for product in item['sub_value'].keys():
                num += 1
                if num > 0:
                    product_id = item['sub_value'][product]
                    querystring = {"par_craft_index": category_id,
                                   "craft_index": product_id,
                                   "startTime": start_day,
                                   "endTime": today,
                                   "par_p_index": "",
                                   "p_index": "",
                                   "keyword": ""
                                   }
                    parses = urlencode(querystring)
                    url = start_url + "?" + parses
                    try:
                        driver.get(url)
                        wait.until(ec.presence_of_element_located((By.CSS_SELECTOR, ".new_page4")))
                    except:
                        driver = webdriver.PhantomJS(executable_path=driver_path)
                        driver.get(url)
                        wait.until(ec.presence_of_element_located((By.CSS_SELECTOR, ".new_page4")))
                        driver.quit()
                    all_page = re.compile(".*?(\d+).*?").findall(driver.find_element_by_css_selector(".new_page4").text)[-1]
                    cont = driver.find_element_by_css_selector(".table-01.mt30 tbody").text
                    if len(cont) == 0 or cont == '日期 产品 价格 市场 走势':
                        all_page = 0
                    print(num, product, all_page)
                    for i in range(1, int(all_page) + 1):
                        time.sleep(0.51)
                        querystring['page'] = str(i)
                        results = down(start_url, querystring)
                        print(product, item["product"], num, results[0], results[1], i, all_page)
                        f.write(" ".join([product, item["product"], str(num), str(results[0]), str(results[1]), str(i), str(all_page) + '\n']))
        driver.quit()
        return True
    except Exception as e:
        print(e)
        return False


if __name__ == '__main__':
    while True:
        todays = str(datetime.datetime.today()).split(" ")[1].split(":")
        hour = int(todays[0])
        minute = todays[1]
        print(datetime.datetime.today())
        start_time = datetime.datetime.today()
        result = main(index)
        if result:
            break
    haoshi = datetime.datetime.today() - start_time
    print(haoshi)



