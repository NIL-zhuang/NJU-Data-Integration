import pandas as pd
import os
import tqdm
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


if __name__ == '__main__':
    loadDataSet()
    df = myApriori()
    frequent_itemsets = apriori(df, min_support=0.004, use_colnames=True)
    frequent_itemsets['length'] = frequent_itemsets['itemsets'].apply(lambda x: len(x))
    print(frequent_itemsets[(frequent_itemsets['length'] >= 2)])
