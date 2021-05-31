from scipy.sparse import data
from sklearn.cluster import KMeans
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm
from sklearn.manifold import TSNE
import os
import csv
import heapq
from random import random


TOP25 = 5
srcPath = 'data/static/time_distribution.csv'
kmeansPath = 'data/static/kmeans.csv'


def getDataset():
    # 这是设置取样规则的，如果要重新取样记得删掉data/static/kmeans.csv文件
    if not os.path.exists(kmeansPath):
        X = []
        df = pd.read_csv(srcPath)
        for idx, row in tqdm(df.iterrows(), total=df.shape[0], desc="Dataset"):
            if row['TOTAL'] < 5:
                # 取样规则在这里
                continue
            data = [row['DAWN'], row['MORNING'], row['AFTERNOON'], row['NIGHT']]
            X.append(list(map(lambda x: x/row['TOTAL'], data)))
        with open(os.path.join('data/static', 'kmeans.csv'), 'w+') as f:
            writer = csv.writer(f)
            writer.writerow(['DAWN', 'MORNING', 'AFTERNOON', 'NIGHT'])
            writer.writerows(X)
    df = pd.read_csv(kmeansPath)
    print("shape is:", df.shape)
    return df


def decideK(df):
    # 通过找肘点确定分类数量
    dataset = []
    for idx, row in df.iterrows():
        dataset.append([row['DAWN'], row['MORNING'], row['AFTERNOON'], row['NIGHT']])
    SSE = []
    K = []

    for n_cluster in tqdm(range(1, 20), desc='decideK'):
        cls = KMeans(n_clusters=n_cluster, max_iter=10000)
        cls.fit(dataset)
        SSE.append(cls.inertia_)
        K.append(n_cluster)
    plt.scatter(K, SSE)
    plt.plot(K, SSE)
    plt.xlabel("K")
    plt.ylabel("SSE")
    plt.show()


def kmeans(df):
    df = pd.read_csv(kmeansPath)
    dataset = []
    for idx, row in tqdm(df.iterrows(), total=df.shape[0], desc="read dataset"):
        dataset.append([row['DAWN'], row['MORNING'], row['AFTERNOON'], row['NIGHT']])
    estimator = KMeans(n_clusters=4, max_iter=100000)
    estimator.fit(dataset)
    print("finish kmeans calculation")
    r1 = pd.Series(estimator.labels_).value_counts()  # 统计各个类别的数目
    print(r1)
    r2 = pd.DataFrame(estimator.cluster_centers_)  # 找出聚类中心
    print(r2)
    r = pd.concat([df, pd.Series(estimator.labels_, index=df.index)], axis=1)  # 详细
    r.columns = list(df.columns) + ['TYPE']  # 重命名表头
    print(r.head())
    print("finish data concat")
    tsne = TSNE(n_components=2)
    tsne.fit_transform(dataset)
    print("finish dataset transform")
    tsne = pd.DataFrame(tsne.embedding_, index=df.index)
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
    # 不同类别用不同颜色和样式绘图
    d = tsne[r['TYPE'] == 0]
    plt.plot(d[0], d[1], 'ro')
    print("finish category 1")
    d = tsne[r['TYPE'] == 1]
    plt.plot(d[0], d[1], 'go')
    print("finish category 2")
    d = tsne[r['TYPE'] == 2]
    plt.plot(d[0], d[1], 'bo')
    print("finish category 3")
    d = tsne[r['TYPE'] == 3]
    plt.plot(d[0], d[1], 'yo')
    print("finish category 4")
    plt.show()


if __name__ == '__main__':
    df = getDataset()
    kmeans(df)
