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
db = "myprojects"
table = "农产品价格行情数据库"

Basepath = os.path.dirname(__file__)
index_path = os.path.join(Basepath, "index.json")
if sys.platform == 'win32':
    driver_path = os.path.join(Basepath, "selenium", "phantomjs-2.1.1-windows", "bin", "phantomjs.exe")
else:
    driver_path = os.path.join(Basepath, "selenium", "phantomjs-2.1.1-linux-x86_64", "bin", "phantomjs")

conn = pymysql.connect(host, 'root', password, db, charset="utf8", use_unicode=True)
cursor = conn.cursor()

start_url = 'http://nc.mofcom.gov.cn/channel/gxdj/jghq/jg_list.shtml'

category_code = {
    "13079": "畜产品",
    "13080": "水产品",
    "13073": "粮油",
    "13076": "果品",
    "13075": "蔬菜",
}

insert_sql = """
           INSERT INTO `myprojects`.`{table}`(`category`, `product`, `price`, `market`, `datetime`)
           VALUES (%s, %s,%s, %s, %s) ON DUPLICATE KEY UPDATE product=VALUES (`product`)""".format(table=table)

index = json.loads(open(index_path, 'r', encoding='utf-8').read())


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
        flag = 200
        wait = WebDriverWait(driver, 30)
        wait.until(ec.presence_of_element_located((By.CSS_SELECTOR, "div.pmCon > table > tbody > tr")))
        responses = driver.page_source
        responses = pq(responses)
    tables = responses.find("div.pmCon > table > tbody > tr")
    for items in tables.items():
        items_value = [category_code[code], ]
        for value in items.find('td').text().split(" "):
            if value == '�|鱼':
                value = "鮸鱼"
            items_value.append(value)
        items_value[2] = float(items_value[2])
        feedback = cursor.execute(insert_sql, items_value)
        print(feedback, items_value)
        conn.commit()
        if feedback or not feedback:
            tiaoshu += 1
    huifu = [flag, tiaoshu]
    time.sleep(random.randint(0, 5))
    return huifu


def main(arg):
    try:
        num = 0
        today = str(datetime.date.today())
        driver = webdriver.PhantomJS(executable_path=driver_path)
        for item in arg:
            wait = WebDriverWait(driver, 30)
            category_id = item['id']
            for product in item['sub_value'].keys():
                product_id = item['sub_value'][product]
                querystring = {"par_craft_index": category_id,
                               "startTime": today,
                               "endTime": today,
                               "craft_index": product_id,
                               "par_p_index": "",
                               "p_index": "",
                               "keyword": ""
                               }
                parses = urlencode(querystring)
                url = start_url + "?" + parses
                try:
                    driver.get(url)
                    wait.until(ec.presence_of_element_located((By.CSS_SELECTOR, ".new_page2")))
                except:
                    driver = webdriver.PhantomJS(executable_path=driver_path)
                    driver.get(url)
                    wait.until(ec.presence_of_element_located((By.CSS_SELECTOR, ".new_page2")))
                    driver.quit()
                all_page = re.compile(".*?(\d+).*?").findall(driver.find_element_by_css_selector(".new_page2").text)[-1]
                cont = driver.find_element_by_css_selector(".s_table03 tbody").text
                if len(cont) == 0:
                    all_page = 0
                num += 1
                print(num, product, all_page)
                for i in range(1, int(all_page) + 1):
                    querystring['page'] = str(i)
                    results = down(start_url, querystring)
                    print(product, item["product"], num, results[0], results[1], i, all_page)
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
        if hour in range(14, 25):
            start_time = datetime.datetime.today()
            result = main(index)
            if result:
                break
    haoshi = datetime.datetime.today() - start_time
    print(haoshi)

