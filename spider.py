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
from pyquery import PyQuery as pq
from fake_useragent import UserAgent

host = "127.0.0.1"
password = '123456'
db = "myprojects"
table = "农产品价格行情数据库"

Basepath = os.path.dirname(__file__)
index_path = os.path.join(Basepath, "index.json")

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
           VALUES (%s, %s,%s, %s, %s) ON DUPLICATE KEY UPDATE price=VALUES(price)""".format(table=table)

index = json.loads(open(index_path, 'r', encoding='utf-8').read())


def down(url, parse):
    code = parse["par_craft_index"]
    header = {
        'cache-control': "no-cache",
        'User-Agent': getattr(UserAgent(), 'random')
    }
    res = requests.request("GET", url, headers=header, params=parse)
    flag = int(res.status_code)
    tiaoshu = 0
    responses = pq(res.text)
    tables = responses.find("div.pmCon > table > tbody > tr")
    for items in tables.items():
        items_value = [category_code[code], ]
        for value in items.find('td').text().split(" "):
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
        for item in arg:
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

                headers = {
                    'cache-control': "no-cache",
                    'User-Agent': getattr(UserAgent(), 'random')
                }
                response = requests.request("GET", start_url, headers=headers, params=querystring)
                all_page = re.compile(""".*var v_PageCount = (\d*);.*""").findall(response.text)
                cont = [i for i in pq(response.text).find(""".s_table03 > tbody:nth-child(2) > tr""").items()]
                if len(all_page) > 1:
                    all_page = all_page[-2]
                elif len(cont) == 1:
                    all_page = 1
                else:
                    all_page = 0
                for i in range(1, int(all_page) + 1):
                    querystring['page'] = str(i)
                    result = down(start_url, querystring)
                    num += 1
                    print(num, result[0], result[1], item["product"], product, i, all_page)
                print(product, all_page)
        return True
    except Exception as e:
        print(e)
        return False


if __name__ == '__main__':
    while True:
        hour = int(str(datetime.datetime.today()).split(" ")[1].split(":")[0])
        minute = str(datetime.datetime.today()).split(" ")[1].split(":")[1]
        print(datetime.datetime.today())
        if hour in range(12, 25) and minute == "00":
            start_time = datetime.datetime.today()
            result = main(index)
            if result:
                break
    haoshi = datetime.datetime.today() - start_time
    print(haoshi)
