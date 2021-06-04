# grab  删掉userId对应的所有购买
# swipe 删掉userId在那个timestamp在category的购买
import pandas as pd
from tqdm import tqdm

grabPath = 'data/static/grab_userid_filter_result.csv'
swipePath = 'data/static/swipe_userid_time_categoryid_filter_result.csv'
staticPath = 'data/static/static.csv'
staticTarPath = 'data/static/staticTar.csv'

static = pd.read_csv(staticPath)


def filterGrab():
    grab = pd.read_csv(grabPath)
    grabId = grab['userId']
    for userId in tqdm(grabId):
        static.drop(static[static['USER_ID'] == userId].index, inplace=True)


def filterSwipe():
    swipe = pd.read_csv(swipePath)
    for idx, row in tqdm(swipe.iterrows(), total=swipe.shape[0], desc='filter swipe'):
        static.drop(static[(static['USER_ID'] == row['userId']) &
                           (static['TIMESTAMP'] == row['TIME']) &
                           (static['CATEGORY_ID'] == row['categoryId'])].index, inplace=True)


if __name__ == '__main__':
    filterSwipe()
    filterGrab()
    static.to_csv(staticTarPath, index=None)
    print(static.shape)
    print(static.head())
