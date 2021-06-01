from tqdm import tqdm
import pandas as pd
from collections import defaultdict

# SESSION_ID, TIME, URI, userId, itemId, categoryId
itemCart = 'data/stream/itemcart_new.csv'
# SESSION_ID, TIME, URI, userId, itemId, categoryId, isSecondKill, success
itemBuy = 'data/stream/itembuy_new.csv'
# SESSION_ID, TIME, URI, userId, itemId, categoryId
itemFavor = 'data/stream/itemfavor_new.csv'
# SESSION_ID, TIME, URI, userId, itemId, categoryId
itemGetDetail = 'data/stream/itemgetDetail_new.csv'
# ID, USER_ID, ITEM_ID, CATEGORY_ID, TYPE, TIMESTAMP
staticData = 'data/static/static.csv'


def getBuyCount(file):
    user_item = defaultdict(list)
    df = pd.read_csv(staticData)
    for idx, row in tqdm(df.iterrows(), total=df.shape[0], desc="getBuyCount"):
        user_item[row['USER_ID']].append(row['ITEM_ID'])
    return user_item


def getSpecificBuy(path):
    user_item = defaultdict(list)
    df = pd.read_csv(path)
    for idx, row in tqdm(df.iterrows(), total=df.shape[0], desc=path):
        user_item[row['userId']].append(row['itemId'])
    return user_item


def getRate(buy: defaultdict, target: defaultdict):
    totalBuy = 0
    totalFavor = 0
    hitBuy = 0
    for k, v in tqdm(buy.items(), desc='iter users'):
        userBuy = v
        userTar = target[k]
        totalBuy += len(v)
        totalFavor += len(userTar)
        hitBuy += len([item for item in userBuy if item in userTar])
    return hitBuy, totalBuy, totalFavor, hitBuy/totalBuy, hitBuy / totalFavor


if __name__ == '__main__':
    # todo 修改这里获取的两个default dict 然后交给getRate就可以获得转化率
    userTar = getSpecificBuy(itemGetDetail)
    userBuy = getBuyCount(staticData)
    print(getRate(userBuy, userTar))
