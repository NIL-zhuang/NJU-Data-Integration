# -*- coding: utf-8 -*-
import os
import pandas as pd

class LoginProcessor:
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
        gb = df.groupby(query)
        result = gb.size().to_frame(name="counts")
        result = result \
            .join(gb.agg({"success": "sum"}).rename(columns={"success": "successNumber"})) \
            .reset_index()
        filename = "login_" + "_".join(query).lower() + ".csv"
        result.to_csv(os.path.join(self.target, filename), index=False, mode="w")

    def filter(self, filename):
        """
        过滤掉100%成功的情况
        :param filename:
        :return:
        """
        df = pd.read_csv(filename, encoding='utf-8', header=0)
        result = df[df["successNumber"] != df["counts"]]
        filename = filename.replace(".csv", "_filter.csv")
        result.to_csv(os.path.join(self.target, filename), index=False, mode="w")

    def analysis(self, filename, query=["mean", 0.10]):
        df = pd.read_csv(filename, encoding='utf-8', header=0)
        df["success_rate"] = df["successNumber"] / df["counts"]
        frame = df.describe()
        print(frame)
        # 登录次数次数少的先不考虑
        result = df[df["counts"] >= frame["counts"][query[0]]]
        frame = result.describe()
        # 剔除成功率太高的
        result = result[result["success_rate"] <= query[1]]
        filename = filename.replace(".csv", "_result.csv")
        result.to_csv(os.path.join(self.target, filename), index=False, mode="w")

    def analysis_ip(self):
        query = ["IPADDR"]
        self.group(query)
        filename = os.path.join(self.target, "login_" + "_".join(query).lower() + ".csv")
        self.filter(filename)
        filename = filename.replace(".csv", "_filter.csv")
        self.analysis(filename)

    def analysis_ip_userId(self):
        query = ["IPADDR", "userId"]
        self.group(query)
        filename = os.path.join(self.target, "login_" + "_".join(query).lower() + ".csv")
        self.filter(filename)
        filename = filename.replace(".csv", "_filter.csv")
        self.analysis(filename)

    def run(self):
        self.analysis_ip_userId()

if __name__ == '__main__':
    lp = LoginProcessor("../userId/userlogin_new.csv", "../login")
    lp.run()