import pandas as pd
import os
from tqdm import tqdm
from collections import defaultdict
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori

dataPath = "data/static"
itemSetList = []


def loadDataSet():
    with open(os.path.join(dataPath, "aprioriData.csv"), 'r') as f:
        for line in f.readlines():
            line = line.replace('\n', '')
            cates = line.split(' ')
            itemSetList.append(list(map(int, cates)))


def myApriori():
    te = TransactionEncoder()
    te_ary = te.fit(itemSetList).transform(itemSetList)
    df = pd.DataFrame(te_ary, columns=te.columns_)
    return df


def dataInit():
    if os.path.exists(os.path.join(dataPath, "aprioriData.csv")):
        return
    df = pd.read_csv("data/static/static.csv")
    user_category = defaultdict(set)
    for idx, row in tqdm(df.iterrows(), total=df.shape[0], desc="category data generate"):
        user_category[row['USER_ID']].add(row['CATEGORY_ID'])
    with open(os.path.join(dataPath, "aprioriData.csv"), 'w+') as f:
        for k, v in tqdm(user_category.items()):
            f.write(' '.join(sorted(list(map(str, v))))+'\n')


if __name__ == '__main__':
    dataInit()
    loadDataSet()
    df = myApriori()
    frequent_itemsets = apriori(df, min_support=0.0035, use_colnames=True)
    frequent_itemsets['length'] = frequent_itemsets['itemsets'].apply(lambda x: len(x))
    print(frequent_itemsets[(frequent_itemsets['length'] >= 2)])
