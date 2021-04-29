# -*- coding: utf-8 -*-
# 将txt文件简单的切成csv文件
import os
import csv
from tqdm import tqdm


class Digest:
    def __init__(self, source, target):
        self.source = source
        self.target = target
        if (not os.path.exists(target)):
            os.mkdir(target)
        self.data = []

    def string2tuple(self, line):
        """
        将一行提取数据后提取出
        :param line:
        :return: [session_id, time, uri, request, ip_addr(if exists)]
        """
        # session_id
        session_id_index = line.find("SESSIONID") + 10
        session_id = line[session_id_index: session_id_index + 32]

        ip_addr = ""
        if (line.find("IPADDR") != -1):
            ip_addr_index = line.find("IPADDR") + 7
            ip_addr = line[ip_addr_index: session_id_index - 13]

        # uri
        uri_index = line.find("uri") + 4
        uri_index_end = line.rfind("|") - 1
        uri = line[uri_index: uri_index_end]

        # t
        t_index = session_id_index + 34
        t = line[t_index: t_index + 19]

        # request_body
        request_body_index = line.rfind("{")
        request_body = line[request_body_index:].strip()
        return [session_id, t, uri, request_body, ip_addr]

    def tuple2string(self, tuple):
        result = "[SESSIONID=" + tuple[0] + "] " \
                 + tuple[1] + " DEBUG [nio-8080-exec-1] com.some.taopao.aop.LogHandler : uri=" \
                 + tuple[2] + " | requestBody=" + tuple[3]
        if (len(tuple[4]) != 0):
            result = "[IPADDR=" + tuple[4] + "] " + result
        return result

    def write2csv(self, filename):
        with open(os.path.join(self.target, filename), "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["SESSION_ID", "TIME", "URI", "REQUEST_BODY", "IPADDR"])
            for line in self.data:
                writer.writerow(line)

    def run(self):
        logs = os.listdir(self.source)
        for log in tqdm(logs):
            with open(os.path.join(self.source, log), "r", encoding="utf-8") as f:
                lines = f.readlines()
                for line in lines:
                    self.data.append(self.string2tuple(line))
            self.write2csv(log.replace("txt", "csv"))
            self.data.clear()


if __name__ == '__main__':
    digest = Digest("../log", "../format")
    digest.run()
