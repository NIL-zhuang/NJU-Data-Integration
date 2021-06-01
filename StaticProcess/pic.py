import matplotlib.pyplot as plt
import pandas as pd
from numpy import polyfit, poly1d
from tqdm import tqdm

dataSrc = 'data/static/time_distribution.csv'


def buyCount(df):
    count = [0] * 30
    index = list(range(1, 31))
    for idx, row in tqdm(df.iterrows(), total=df.shape[0], desc="count buy"):
        if row['TOTAL'] > 30:
            continue
        count[row['TOTAL']-1] += 1
    # plt.plot(index, count, 'ro')
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
    plt.bar(index, count)
    plt.xlabel("Buy count")
    plt.ylabel("Number of people")
    plt.show()


if __name__ == '__main__':
    df = pd.read_csv(dataSrc)
    print(df.describe())
    buyCount(df)
