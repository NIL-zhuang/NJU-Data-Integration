# -*- coding: utf-8 -*-
import pandas as pd
from tqdm import tqdm
import csv

def trans2union():
    """
    将数据进行拼接
    :return:
    """
    cols = [1, 2, 3, 4]
    cart = pd.read_csv("../category/itemcart_new.csv", usecols=cols, encoding='utf-8', header=0)
    buy = pd.read_csv("../category/itembuy_new.csv", usecols=cols, encoding='utf-8', header=0)
    favor = pd.read_csv("../category/itemfavor_new.csv", usecols=cols, encoding='utf-8', header=0)
    detail = pd.read_csv("../category/itemgetDetail_new.csv", usecols=cols, encoding='utf-8', header=0)
    res = pd.concat([cart, buy, favor, detail])
    print("Start Sort")
    res.sort_values(["userId", "itemId", "TIME"], inplace=True)
    print("End Sort")
    res['URI'] = res['URI'].map(lambda x: x.split('/')[-1])
    print("End Map")
    res.to_csv("../union/union.csv", index=False, mode="w")

def filter1():
    """
    过滤掉所有的非累计部分
    :return:
    """
    with open("../union/union_after.csv", encoding="utf-8", newline="", mode='w') as f:
        writer = csv.writer(f)
        with open("../union/union.csv", encoding="utf-8", mode='r') as f1:
            reader = csv.reader(f1)
            first = True
            write = False
            pre = ["-", "-", "-", "-"]
            for line in tqdm(reader):
                if first:
                    writer.writerow(line)
                    pre = line
                    first = False
                    continue
                if pre[3] == line[3]:
                    writer.writerow(pre)
                    write = True
                else:
                    if write:
                        writer.writerow(pre)
                    write = False
                pre = line

def filter2():
    """
    过滤掉所有无相同请求的部分
    :return:
    """
    with open("../union/union_filter2.csv", encoding="utf-8", newline="", mode='w') as f:
        writer = csv.writer(f)
        with open("../union/union_after.csv", encoding="utf-8", mode='r') as f1:
            reader = csv.reader(f1)
            first = True
            write = False
            set = []
            uri = []
            for line in tqdm(reader):
                if first:
                    writer.writerow(line)
                    pre = line
                    first = False
                    continue
                if pre[3] == line[3]:
                    set.append(line)
                    if line[1] not in uri:
                        uri.append(line[1])
                else:
                    if len(uri) >= 2:
                        for l in set:
                            writer.writerow(l)
                    uri.clear()
                    set.clear()
                    set.append(line)
                    if line[1] not in uri:
                        uri.append(line[1])
                pre = line

def statistics():
    """
    从全部数据中提取每一个用户的比例
    :return:
    """
    data = pd.read_csv("../union/union.csv", encoding='utf-8', header=0)
    print("装载完成")
    gb = data.groupby(["userId", "URI"])
    result = gb.size().to_frame(name="counts").reset_index()
    print("开始写入")
    result.to_csv("../union/statistics.csv", index=False, mode="w")


if __name__ == '__main__':
    statistics()