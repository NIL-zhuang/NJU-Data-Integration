from tqdm import tqdm
import pandas as pd
from collections import defaultdict
import csv

# TIME,URI,userId,itemId
# 1511700967,favor,2,111223
# 1511701555,getDetail,2,111223
# 1511701683,getDetail,2,111223
# 1511852797,getDetail,2,3730891
# 1511853505,buy,2,3730891
# 1511853300,getDetail,2,4681233
dataPath = 'data/stream/union_filter2.csv'
countPath = 'data/stream/statistics.csv'

user_buy = defaultdict(list)
user_cart = defaultdict(list)
user_favor = defaultdict(list)
user_getDetail = defaultdict(list)
tableMap = {'favor': user_favor,
            'buy': user_buy,
            'cart': user_cart,
            'getDetail': user_getDetail}


def initData():
    df = pd.read_csv(dataPath)
    for idx, row in tqdm(df.iterrows(), total=df.shape[0], desc="init data"):
        tableMap[row['URI']][row['userId']].append(row['itemId'])


def calTransfer(dest, src):
    destTable = tableMap[dest]
    srcTable = tableMap[src]
    totalSrc = 0
    totalDest = 0
    hitDest = 0
    for k, v in tqdm(destTable.items(), desc='-'.join([dest, src, '转化率'])):
        userDest = v
        userSrc = srcTable[k]
        totalDest += len(v)
        totalSrc += len(userSrc)
        hitDest += len([item for item in userDest if item in userSrc])
    print(dest, '共计有{}个行为'.format(totalDest))
    print(src, '共计有{}个行为'.format(totalSrc))
    print('命中的{}为{}'.format(dest, hitDest))
    print('{}到{}的转化率为{}'.format(src, dest, hitDest / totalSrc))


def countAll():
    df = pd.read_csv(countPath)
    print(df.groupby(['URI']).sum())


if __name__ == '__main__':
    countAll()
    # initData()
    # calTransfer('buy', 'favor')
    # calTransfer('buy', 'cart')
    # calTransfer('buy', 'getDetail')
    # calTransfer('favor', 'getDetail')
    # calTransfer('cart', 'getDetail')
