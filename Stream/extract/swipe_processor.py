# -*- coding: utf-8 -*-
import os
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm

# 指定默认字体
plt.rcParams['font.sans-serif'] = ['KaiTi']
# 解决保存图像是负号'-'显示为方块的问题
plt.rcParams['axes.unicode_minus'] = False

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
        # df["TIME"] = df["TIME"].apply(self.near_time)
        # 过滤掉秒杀的部分
        df = df[df["isSecondKill"] == 0]
        gb = df.groupby(query)
        result = gb.size().to_frame(name="counts").reset_index()
        filename = "swipe_" + "_".join(query).lower() + ".csv"
        result.to_csv(os.path.join(self.target, filename), index=False, mode="w")

    def filter(self, filename):
        """
        没有抢到过的用户被过滤掉
        :param filename:
        :return:
        """
        df = pd.read_csv(os.path.join(self.target, filename), encoding='utf-8', header=0)
        result = df[df["counts"] > 1]
        filename = filename.replace(".csv", "_filter.csv")
        result.to_csv(os.path.join(self.target, filename), index=False, mode="w")

    def analysis(self, filename, query=["mean"]):
        df = pd.read_csv(os.path.join(self.target, filename), encoding='utf-8', header=0)
        frame = df.describe()
        if(type(query) == int):
            condition = query
        else:
            condition = frame["counts"][query[0]]
        result = df[df["counts"] >= condition]
        filename = filename.replace(".csv", "_result.csv")
        result.to_csv(os.path.join(self.target, filename), index=False, mode="w")

    def run(self, query, analysis_query):
        self.group(query)
        filename = "swipe_" + "_".join(query).lower() + ".csv"
        self.filter(filename)
        filename = filename.replace(".csv", "_filter.csv")
        self.analysis(filename, analysis_query)
        self.draw_filter(filename)


    def draw_filter(self, filename):
        df = pd.read_csv(os.path.join(self.target, filename), encoding='utf-8', header=0)

        fig, axes = plt.subplots(2, 3, figsize=(12, 12))
        fig.suptitle("用户购买行为分析图")

        ax1 = axes[0, 0]
        ax1.boxplot(df["counts"], notch=True, sym='*', labels=["counts(total)"])
        ax1.set_title("(userId, itemId, TIME)购买次数分布情况")

        # ax2 = axes[0, 1]
        # error = df[df["success_rate"] <= 0.2]
        # ax2.boxplot(error["success_rate"], notch=True, sym='*', labels=["success_rate(<= 0.2)"])
        # ax2.set_title("图-2 低登录成功率分布情况")
        #
        # ax3 = axes[0, 2]
        # normal = df[df["success_rate"] > 0.2]
        # ax3.boxplot(normal["success_rate"], notch=True, sym='*', labels=["success_rate(> 0.2)"])
        # ax3.set_title("图-3 高登录成功率分布情况")
        #
        # ax4 = axes[1, 0]
        # ax4.boxplot(df["counts"], labels=["counts(total)"])
        # ax4.set_title("图-4 整体尝试次数分布情况")
        #
        # ax5 = axes[1, 1]
        # ax5.boxplot(error["counts"], notch=True, labels=["counts(success_rate <= 0.2)"])
        # ax5.set_title("图-5 低成功率部分尝试次数分布情况")
        #
        # ax6 = axes[1, 2]
        # ax6.boxplot(normal["counts"], notch=True, labels=["counts(success_rate > 0.2)"])
        # ax6.set_title("图-6 高成功率部分尝试次数分布情况")

        plt.show()

if __name__ == '__main__':
    s = SwipeProcessor("../userId/itembuy_new.csv", "../result/swipe")
    queries = {
        "[\"userId\", \"TIME\"]": 10,
        "[\"userId\", \"TIME\", \"itemId\"]": ["mean"],
        "[\"userId\", \"TIME\", \"categoryId\"]": 15,
    }
    for query in tqdm(queries):
        s.run(eval(query), queries[query])
    # 提取所有大于40的用户
    s.run(["userId", "TIME"], 40)