# -*- coding: utf-8 -*-
import os
import pandas as pd
from tqdm import tqdm

'''
找到爬虫机器人
'''
class SpiderProcessor:
    def __init__(self, source, target):
        self.source = source
        self.target = target

        if not os.path.exists(source):
            print("Source Not Found!")
        if not os.path.exists(target):
            os.mkdir(target)

    def near_time(self, number):
        """
        调整到最近的一分钟
        :param number:
        :return:
        """
        n = number % 60
        if (n > 30):
            number = number - n + 60
        else:
            number -= n
        return number

    def group(self, query):
        """
        根据 query 进行 group by 后进行统计
        :param filename: 绝对路径
        :param query:
        :return:
        """
        df = pd.read_csv(self.source, encoding='utf-8', header=0)
        df["TIME"] = df["TIME"].apply(self.near_time)
        gb = df.groupby(query)
        result = gb.size().to_frame(name="counts").reset_index()
        filename = "spider_" + "_".join(query).lower() + ".csv"
        result.to_csv(os.path.join(self.target, filename), index=False, mode="w")

    def filter(self, filename):
        """
        没有抢到过的用户被过滤掉
        :param filename:
        :return:
        """
        df = pd.read_csv(filename, encoding='utf-8', header=0)
        result = df[df["counts"] > 1]
        filename = filename.replace(".csv", "_filter.csv")
        result.to_csv(os.path.join(self.target, filename), index=False, mode="w")

    def analysis(self, filename, query=["mean"]):
        df = pd.read_csv(filename, encoding='utf-8', header=0)
        frame = df.describe()
        if(type(query) == int):
            condition = query
        else:
            condition = frame["counts"][query[0]]
        result = df[df["counts"] >= condition]
        filename = filename.replace(".csv", "_result.csv")
        result.to_csv(os.path.join(self.target, filename), index=False, mode="w")

    def run(self, query, analysis_query):
        """
        :return:
        """
        print("Group")
        self.group(query)
        filename = os.path.join(self.target, "spider_" + "_".join(query).lower() + ".csv")
        print("filter")
        self.filter(filename)
        filename = filename.replace(".csv", "_filter.csv")
        print("analysis")
        self.analysis(filename, analysis_query)


if __name__ == '__main__':
    s = SpiderProcessor("../category/itemgetDetail_new.csv", "../spider")
    queries = {
        "[\"userId\", \"TIME\", \"itemId\", \"categoryId\"]": ["mean"],
    }
    for query in tqdm(queries):
        s.run(eval(query), queries[query])