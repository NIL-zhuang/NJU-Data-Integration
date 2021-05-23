import pandas as pd
from datetime import datetime
import json


class TimeDistribute():
    def __init__(self, filename):
        self.filename = filename

    def countTimeDistribute(self):
        user_time = dict()
        df = pd.read_csv(self.filename)
        for idx, row in df.iterrows():
            pre = user_time.get(row['userId'], [0]*24)
            dtTime = datetime.fromtimestamp(row['TIME'])
            pre[dtTime.hour-1] += 1
            user_time[row['userId']] = pre
        b = json.dumps(user_time)
        with open("user_time_cart.json", 'w+') as f:
            f.write(b)


if __name__ == '__main__':
    td = TimeDistribute("./data/stream/itemcart_new.csv")
    td.countTimeDistribute()
