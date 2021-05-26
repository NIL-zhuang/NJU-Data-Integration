# -*- coding: utf-8 -*-
import os
import csv
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
        if not os.path.exists(os.path.join(target, "user")):
            os.mkdir(os.path.join(target, "user"))

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
        if (type(query) == int):
            condition = query
        else:
            condition = frame["counts"][query[0]]
        result = df[df["counts"] >= condition]
        filename = filename.replace(".csv", "_result.csv")
        result.to_csv(os.path.join(self.target, filename), index=False, mode="w")

    def check_login(self):
        """
        过滤到所有成功登录请求并物化
        :return:
        """
        df = pd.read_csv(self.source, encoding='utf-8', header=0)
        df = df[df["success"] == 1]
        df = df.sort_values(by="userId")
        df.to_csv(os.path.join(self.target, "userId_Ip.csv"), columns=["TIME", "IPADDR", "userId"], index=False,
                  mode="w")

    def find_Ip(self, df, userId, now):
        """
        :param df: userId_Ip 映射
        :param userId: 查询的userId
        :param now: 查询的当前时刻
        :return:
        """
        df = df[df["userId"] == userId]
        df = df[df["TIME"] <= now]
        if len(df) >= 1:
            return df.tail(1)["IPADDR"].values[0]
        else:
            return "0.0.0.0"

    def transform(self):
        """
        将 getDetail 转换为包含IP的部分，并清理列 TODO 优化效率
        :return:
        """
        userIp = {}
        userIp_mapper = {}
        multi_userId = []
        mapper = pd.read_csv(os.path.join(self.target, "userId_Ip.csv"), encoding='utf-8', header=0)
        for row in tqdm(mapper.itertuples()):
            row_time = getattr(row, 'TIME')
            ip_addr = getattr(row, 'IPADDR')
            user_id = getattr(row, "userId")
            if user_id not in userIp:
                userIp[user_id] = {}
            if ip_addr not in userIp[user_id]:
                userIp[user_id][ip_addr] = []
            userIp[user_id][ip_addr].append(row_time)
        for user_id in userIp:
            if (len(userIp[user_id]) > 1):
                multi_userId.append(user_id)
            else:
                userIp_mapper[user_id] = list(userIp[user_id].keys())[0]

        with open(os.path.join(self.target, "itemgetDetail_transform_ip.csv"), mode="w", encoding="utf-8",
                  newline="") as f1:
            writer = csv.writer(f1)
            count = 1000000
            n = 0
            with open(os.path.join(self.target, "itemgetDetail_transform.csv"), mode="r", encoding="utf-8") as f2:
                reader = csv.reader(f2)
                first = True
                for line in tqdm(reader):
                    if first:
                        line.append("IPADDR")
                        writer.writerow(line)
                        first = False
                        continue
                    if (line[2] in multi_userId):
                        ip = self.find_Ip(mapper, eval(line[2]), eval(line[1]))
                    else:
                        try:
                            ip = userIp_mapper[eval(line[2])]
                        except:
                            ip = "0.0.0.0"

                    line.append(ip)
                    writer.writerow(line)
                    n += 1
                    if n > count:
                        break

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

    def sortFolder(self, folder):
        """
        排序所有的用户文件
        :param folder:
        :return:
        """
        filenames = os.listdir(folder)
        for filename in tqdm(filenames):
            user_id = eval(filename.split(".")[0])
            self.sortOne(user_id)

    def sortOne(self, user_id):
        """
        排序一个用户文件
        :param user_id:
        :return:
        """
        df = pd.read_csv(os.path.join(self.target, "user", str(user_id) + ".csv"), encoding='utf-8', header=0)
        df = df.sort_values("TIME")
        df.to_csv(os.path.join(self.target, "user", str(user_id) + ".csv"), index=False, mode="w")

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
    s = SpiderProcessor("../category/itemgetDetail_new.csv", "../result/spider")
    # queries = {
    #     "[\"userId\", \"TIME\", \"itemId\", \"categoryId\"]": ["mean"],
    # }
    # for query in tqdm(queries):
    #     s.run(eval(query), queries[query])

    # s.check_login()
    s.sortFolder("../result/spider/user")
