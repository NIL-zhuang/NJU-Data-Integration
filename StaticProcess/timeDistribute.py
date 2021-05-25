import pandas as pd
from datetime import datetime
import json
import os
from tqdm import tqdm

streamPath = "data/stream"
dataPath = "data/static/tmp"
# 这是用户浏览时间分布的计算内容
# 将用户的浏览记录按照每6小时一段统计，然后归一化算聚类
# 六小时分别是： 00:00-06:00, 06:00-12:00, 12:00-18:00, 18:00-24:00，分别对应半夜，上午，下午，晚上


class TimeDistribute():
    def __init__(self, fileList):
        self.fileList = fileList
        self.user_time = dict()

    def countTimeDistribute(self):
        for file in self.fileList:
            filePath = os.path.join(streamPath, file)
            print(filePath)
            df = pd.read_csv(filePath)
            print("We are prepared to process")
            # for idx, row in tqdm(df.iterrows(), total=df.shape[0], desc=filePath):
            for idx, row in df.iterrows():
                pre = self.user_time.get(row['userId'], [0]*4)
                dtTime = datetime.fromtimestamp(row['TIME'])
                pre[(dtTime.hour-1)//6] += 1
                self.user_time[row['userId']] = pre
        b = json.dumps(self.user_time)
        with open(os.path.join(dataPath, "time_distribution.json"), 'w+') as f:
            f.write(b)


if __name__ == '__main__':
    # fileList = os.listdir(streamPath)
    fileList = ["itemgetDetail_new.csv"]
    print(fileList)
    td = TimeDistribute(fileList)
    td.countTimeDistribute()
