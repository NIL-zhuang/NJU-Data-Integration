# -*- coding: utf-8 -*-
from kafka import KafkaConsumer
import datetime
import os

# 需要在校园网环境或nju vpn环境中使用
consumer = KafkaConsumer(
    'foobar',
    bootstrap_servers='172.29.4.17:9092',
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='PLAIN',
    sasl_plain_username='student',
    sasl_plain_password='nju2021',
)

# 多个 consumer 可以重复消费相同的日志，每个 consumer 只会消费到它启动后产生的日志，不会拉到之前的余量
bufferLen = 1000
timeRange = 60  # 60min换一个文件
buffer = ['' for _ in range(bufferLen)]
cur = 0  # 记录当前buffer指针
disc = ''  # 移动盘盘符


def getFileName(timestamp):
    return timestamp.strftime('%Y%m%d %H%M%S') + '.txt'

if __name__ == '__main__':
    startTime = datetime.datetime.now()  # 获取开始时间戳
    f = open(getFileName(startTime), 'w+')  # zzy端改成a+才能跑
    for msg in consumer:

        curTime = datetime.datetime.now()  # 当前时间戳
        if (curTime - startTime).seconds >= timeRange:
            f.close()
            print((curTime - startTime).seconds)
            f = open(getFileName(curTime), 'w+')  # zzy端改成a+才能跑
            startTime = curTime  # 更新检查点

        line = msg.value.decode("utf-8")
        buffer[cur] = line
        cur += 1

        if cur == bufferLen:
            print('write in')
            f.write('\n'.join(buffer) + '\n')
            cur = 0