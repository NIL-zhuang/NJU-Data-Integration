# -*- coding: utf-8 -*-
import os
import csv
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt

# 指定默认字体
plt.rcParams['font.sans-serif'] = ['KaiTi']
# 解决保存图像是负号'-'显示为方块的问题
plt.rcParams['axes.unicode_minus'] = False

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
        if not os.path.exists(os.path.join(target, "user")):
            os.mkdir(os.path.join(target, "user"))

    def check_login(self):
        """
        过滤得到所有成功登录请求并物化，持久化到userId_Ip.csv
        :return:
        """
        df = pd.read_csv("../userId/userlogin_new.csv", encoding='utf-8', header=0)
        df = df[df["success"] == 1]
        df = df.sort_values(by="userId")
        df.to_csv(os.path.join(self.target, "userId_Ip.csv"), columns=["TIME", "IPADDR", "userId"], index=False,
                  mode="w")

    def check_User_Ip(self):
        """
        检查所有的IP地址，得到每一个IP地址对应的用户数量关系的分布情况
        选择出高于10个的IP地址，持久化到user_Ip_check.csv中
        :return:
        """
        df = pd.read_csv("../result/spider/userId_Ip.csv", encoding='utf-8', header=0)
        gb = df.groupby(["IPADDR"]).userId.nunique()
        result = gb.to_frame(name="counts").reset_index()
        fig, axes = plt.subplots(2, 3, figsize=(12, 12))
        fig.suptitle("IP地址对应用户关系分布")

        ax1 = axes[0, 0]
        ax1.boxplot(result["counts"], notch=True, sym='*', labels=["counts(total)"])
        ax1.set_title("整体IP地址对应用户关系分布")

        resultHigh1 = result[result["counts"] < 10]
        ax2 = axes[0, 1]
        ax2.boxplot(resultHigh1["counts"], notch=True, sym='*', labels=["counts(<10)"])
        ax2.set_title("整体IP地址登录次数小于10的用户的分布")

        resultHigh10 = result[result["counts"] > 10]
        print(resultHigh10.describe())
        ax3 = axes[0, 2]
        ax3.boxplot(resultHigh10["counts"], notch=True, sym='*', labels=["counts(>10)"])
        ax3.set_title("整体IP地址登录次数高于10的用户的分布")
        resultHigh10 = resultHigh10.sort_values(["counts"])
        resultHigh10.to_csv("../result/spider/Ip_check.csv", index=False, mode="w")
        plt.show()

    def check_all(self):
        """
        检查所有的可疑Ip地址
        :return:
        """
        source = pd.read_csv(os.path.join(self.target, "userId_Ip.csv"), encoding='utf-8', header=0)
        df = pd.read_csv(os.path.join(self.target, "Ip_check.csv"), encoding='utf-8', header=0)
        for ip in tqdm(df["IPADDR"]):
            print("Checking", ip)
            # 得到所有对应的userId
            ip_result = []
            userIds = source[source["IPADDR"] == ip]["userId"]
            for userId in userIds:
                ips = source[source["userId"] == userId]
                if(not os.path.exists(os.path.join(self.target, "user", str(userId) + ".csv"))):
                    continue
                user_data = pd.read_csv(
                    os.path.join(self.target, "user", str(userId) + ".csv")
                    , encoding='utf-8', header=0)
                ips.sort_values(["TIME"])
                now = []
                times = []
                for row in ips.itertuples():
                    TIME = getattr(row, 'TIME')
                    IPADDR = getattr(row, 'IPADDR')
                    if(IPADDR == ip):
                        if(len(now) == 0):
                            now.append(TIME)
                        else:
                            continue
                    else:
                        if(len(now) == 1):
                            now.append(TIME)
                            times.append(now)
                            now = []
                        continue
                if(len(now) != 0):
                    times.append(now)
                for my_time in times:
                    # 处理每一个时间段
                    if(len(my_time) == 1):
                        temp = user_data[user_data["TIME"] >= my_time[0]]
                    elif(len(my_time) == 2):
                        temp = user_data[user_data["TIME"] >= my_time[0]]
                        temp = temp[temp["TIME"] < my_time[1]]
                    else:
                        print("Error!")
                        print(my_time)
                    ip_result.append(temp)
            ip_pd = pd.concat(ip_result)
            ip_pd.to_csv(os.path.join(self.target, "ip", str(ip) + ".csv"), index=False, mode="w")

    def union(self):
        """
        将/Ip文件夹所有的文件合并成检查的结果
        :return:
        """
        target = os.path.join(self.target, "ip")
        filenames = os.listdir(target)
        source = []
        for filename in filenames:
            df = pd.read_csv(os.path.join(self.target, "ip", filename), encoding='utf-8', header=0)
            # df.insert(df.shape[1], "IPADDR", filename[:-4])
            source.append(df)
        result = pd.concat(source)
        print(type(result))
        result.sort_values(["TIME"], inplace=True)
        result.to_csv(os.path.join(self.target, "union_ip.csv"), index=False, mode="w")

    def final_check(self):
        """
        检查最终的union_Ip文件
        :return:
        """
        df = pd.read_csv(os.path.join(self.target, "union_ip.csv"), encoding='utf-8', header=0)
        gb = df.groupby(["IPADDR", "categoryId"])
        result = gb.size().to_frame(name="counts").reset_index()
        print(result.describe())
        result.to_csv(os.path.join(self.target, "union_ip_group.csv")
                      , index=False, mode="w")

    def draw_filter(self, filename):
        df = pd.read_csv(os.path.join(self.target, filename), encoding='utf-8', header=0)

        fig, axes = plt.subplots(2, 3, figsize=(12, 12))
        fig.suptitle("Ip地址查看同类商品分析图")

        ax1 = axes[0, 0]
        ax1.boxplot(df["counts"], notch=True, sym='*', labels=["counts(total)"])
        ax1.set_title("图-1 整体Ip地址查看同类商品次数分布情况")

        ax2 = axes[0, 1]
        normal = df[df["counts"] <= 150]
        ax2.boxplot(normal["counts"], notch=True, sym='*', labels=["counts(<= 150)"])
        ax2.set_title("图-2 低查询(<=150)时分布情况")

        ax3 = axes[0, 2]
        error = df[df["counts"] > 150]
        ax3.boxplot(error["counts"], notch=True, sym='*', labels=["counts(> 150)"])
        ax3.set_title("图-3 高查询(>150)时分布情况")

        plt.show()
        error.sort_values(["counts"])
        error.to_csv(os.path.join(self.target, "error_ip.csv"), index=False, mode="w")

    def mySplit(self):
        """
        按照用户切分用户文件
        :return:
        """
        now = "1"
        title = ["SESSION_ID", "TIME", "URI", "userId", "itemId", "categoryId"]
        fd = open(os.path.join(self.target, "user", now + ".csv"), encoding="utf-8", mode="w", newline="")
        writer = csv.writer(fd)
        writer.writerow(title)
        with open("../userId/itemgetDetail_new.csv", encoding="utf-8", mode="r") as f:
            reader = csv.reader(f)
            first = True
            for line in tqdm(reader):
                if first:
                    first = False
                    continue
                if line[3] != now:
                    now = line[3]
                    fd.close()
                    fd = open(os.path.join(self.target, "user", now + ".csv"), encoding="utf-8", mode="w", newline="")
                    writer = csv.writer(fd)
                    writer.writerow(title)
                writer.writerow(line)


if __name__ == '__main__':
    s = SpiderProcessor("../category/itemgetDetail_new.csv", "../result/spider")
    # s.union()
    # s.final_check()
    s.draw_filter("union_ip_group.csv")