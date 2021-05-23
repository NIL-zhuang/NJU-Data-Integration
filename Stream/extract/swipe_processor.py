# -*- coding: utf-8 -*-
import os
import pandas as pd
from tqdm import tqdm

class SwipeProcessor:
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
        # TODO 过滤掉秒杀的部分？
        df = df[df["isSecondKill"] == 0]
        gb = df.groupby(query)
        result = gb.size().to_frame(name="counts").reset_index()
        # result = result \
        #     .join(gb.agg({"isSecondKill": "sum"}).rename(columns={"isSecondKill": 'secondKillNumber'})) \
        #     .join(gb.agg({"success": "sum"}).rename(columns={"success": "successNumber"})) \
        #     .reset_index()
        filename = "swipe_" + "_".join(query).lower() + ".csv"
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
        备选query
            发现被秒杀的商品Id是5000001-5000010，类别是140410
        :return:
        """
        self.group(query)
        filename = os.path.join(self.target, "swipe_" + "_".join(query).lower() + ".csv")
        self.filter(filename)
        filename = filename.replace(".csv", "_filter.csv")
        self.analysis(filename, analysis_query)

if __name__ == '__main__':
    s = SwipeProcessor("../userId/itembuy_new.csv", "../swipe")
    queries = {
        "[\"categoryId\", \"TIME\"]": ["75%"],
        "[\"itemId\", \"TIME\"]": 10,
        "[\"userId\", \"TIME\"]": 10,
        "[\"userId\", \"TIME\", \"itemId\"]": ["mean"]
    }
    for query in tqdm(queries):
        s.run(eval(query), queries[query])