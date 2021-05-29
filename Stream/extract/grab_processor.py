# -*- coding: utf-8 -*-

import pandas as pd
import os

'''
找到抢单机器人
'''
class GrabProcessor:
    def __init__(self, source, target):
        self.source = source
        self.target = target

        if not os.path.exists(source):
            print("Source Not Found!")
        if not os.path.exists(target):
            os.mkdir(target)

    def group(self, query):
        """
        根据 query 进行 group by 后进行统计
        :param filename: 绝对路径
        :param query:
        :return:
        """
        df = pd.read_csv(self.source, encoding='utf-8', header=0)
        # 添加整点和半点单独检查
        result = df[(df["TIME"] % 1800 < 60) | (df["TIME"] % 1800 > 1740)]
        gb = result.groupby(query)
        result = gb.size().to_frame(name="counts")
        result = result \
            .join(gb.agg({"isSecondKill": "sum"}).rename(columns={"isSecondKill": 'secondKillNumber'})) \
            .join(gb.agg({"success": "sum"}).rename(columns={"success": "successNumber"})) \
            .reset_index()
        filename = "grab_" + "_".join(query).lower() + ".csv"
        result.to_csv(os.path.join(self.target, filename), index=False, mode="w")

    def filter(self, filename):
        """
        没有抢到过的用户被过滤掉
        :param filename:
        :return:
        """
        df = pd.read_csv(os.path.join(self.target, filename), encoding='utf-8', header=0)
        result = df[df["successNumber"] > 0]
        filename = filename.replace(".csv", "_filter.csv")
        result.to_csv(os.path.join(self.target, filename), index=False, mode="w")

    def analysis(self, filename, query=["mean", "75%", "mean"]):
        df = pd.read_csv(os.path.join(self.target, filename), encoding='utf-8', header=0)
        df["success_rate"] = df["successNumber"] / df["secondKillNumber"]
        df["kill_rate"] = df["secondKillNumber"] / df["counts"]
        frame = df.describe()
        print(frame)
        # 秒杀次数较少的先不考虑
        result = df[df["counts"] >= frame["counts"][query[0]]]
        frame = result.describe()
        # 成功率高于平均值
        result = result[result["success_rate"] >= frame["success_rate"][query[1]]]
        # 秒杀率高于平均值
        result = result[result["kill_rate"] >= frame["kill_rate"][query[2]]]
        filename = filename.replace(".csv", "_result.csv")
        result.to_csv(os.path.join(self.target, filename), index=False, mode="w")

    def near_time(self, number):
        """
        调整到最近的整点/半点
        :param number:
        :return:
        """
        n = number % 1800
        if (n > 60):
            number = number - n + 1800
        else:
            number -= n
        return number

    def union_user_time(self, filename, query):
        """
        将整点时间再合并
        :param filename:
        :return:
        """
        df = pd.read_csv(os.path.join(self.target, filename), encoding='utf-8', header=0)
        df["TIME"] = df["TIME"].apply(self.near_time)
        gb = df.groupby(query)
        result = gb.size().to_frame(name="counts")
        result = result.drop(columns=["counts"])
        result = result \
            .join(gb.agg({"counts": "sum"})) \
            .join(gb.agg({"secondKillNumber": "sum"}).rename(columns={"isSecondKill": 'secondKillNumber'})) \
            .join(gb.agg({"successNumber": "sum"}).rename(columns={"success": "successNumber"})) \
            .reset_index()
        filename = filename.replace(".csv", "_union.csv")
        result.to_csv(os.path.join(self.target, filename), index=False, mode="w")

    def analysis_user(self):
        query = ["userId"]
        self.group(query)
        filename = "grab_" + "_".join(query).lower() + ".csv"
        self.filter(filename)
        filename = filename.replace(".csv", "_filter.csv")
        self.analysis(filename, ["mean", "75%", "mean"])

    def analysis_user_time(self, union=True):
        query = ["userId", "TIME"]
        self.group(query)
        filename = "grab_" + "_".join(query).lower() + ".csv"
        if union:
            self.union_user_time(filename, query)
            filename = filename.replace(".csv", "_union.csv")
        self.filter(filename)
        filename = filename.replace(".csv", "_filter.csv")
        self.analysis(filename, ["mean", "mean", "mean"])

    def run(self):
        # 分析整个用户
        self.analysis_user()
        # 分析用户-时间（按照整点或半点合并）
        self.analysis_user_time()
        # 分析用户-时间（未合并原数据）
        self.analysis_user_time(False)

if __name__ == '__main__':
    g = GrabProcessor("../userId/itembuy_new.csv", "../result/grab")
    g.run()