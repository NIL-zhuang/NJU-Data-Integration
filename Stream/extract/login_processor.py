# -*- coding: utf-8 -*-
import os
import pandas as pd
import matplotlib.pyplot as plt

# 指定默认字体
plt.rcParams['font.sans-serif'] = ['KaiTi']
# 解决保存图像是负号'-'显示为方块的问题
plt.rcParams['axes.unicode_minus'] = False


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

    def draw_filter(self, filename):
        df = pd.read_csv(filename, encoding='utf-8', header=0)
        df["success_rate"] = df["successNumber"] / df["counts"]

        fig, axes = plt.subplots(3, 3, figsize=(12, 12))
        fig.suptitle("用户登录信息分析表")

        ax1 = axes[0, 0]
        ax1.boxplot(df["success_rate"], notch=True, sym='*', labels=["success_rate(total)"])
        ax1.set_title("整体成功率分布")

        ax2 = axes[0, 1]
        error = df[df["success_rate"] <= 0.2]
        ax2.boxplot(error["success_rate"], notch=True, sym='*', labels=["success_rate(<= 0.2)"])
        ax2.set_title("低成功率分布")

        ax3 = axes[0, 2]
        normal = df[df["success_rate"] > 0.2]
        ax3.boxplot(normal["success_rate"], notch=True, sym='*', labels=["success_rate(> 0.2)"])
        ax3.set_title("高成功率分布")

        ax4 = axes[1, 0]
        ax4.boxplot(df["counts"], labels=["counts(total)"])
        ax4.set_title("整体成功率尝试次数分布")

        ax5 = axes[1, 1]
        ax5.boxplot(error["counts"], notch=True, labels=["counts(success_rate <= 0.2)"])
        ax5.set_title("低成功率尝试次数分布")

        ax6 = axes[1, 2]
        ax6.boxplot(normal["counts"], notch=True, labels=["counts(success_rate > 0.2)"])
        ax6.set_title("高成功率尝试次数分布")

        ax7 = axes[2, 0]
        ax7.boxplot(df["successNumber"], labels=["success(total)"])
        ax7.set_title("整体成功率成功次数分布")

        ax8 = axes[2, 1]
        ax8.boxplot(error["successNumber"], notch=True, labels=["success(success_rate <= 0.2)"])
        ax8.set_title("低成功率成功次数分布")

        ax9 = axes[2, 2]
        ax9.boxplot(normal["successNumber"], notch=True, labels=["success(success_rate > 0.2)"])
        ax9.set_title("高成功率成功次数分布")
        plt.show()

    def run(self):
        self.analysis_ip()
        self.analysis_ip_userId()
        self.draw_filter("../login/login_ipaddr_filter.csv")
        self.draw_filter("../login/login_ipaddr_userid_filter.csv")


if __name__ == '__main__':
    lp = LoginProcessor("../userId/userlogin_new.csv", "../login")
    lp.run()
