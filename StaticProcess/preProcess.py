import pandas as pd


class Prep():
    def __init__(self):
        self.static = "data/static/static.csv"
        self.itembuy = "data/stream/itembuy_new.csv"
        self.itemcart = "data/stream/itemcart_new.csv"
        self.itemfavor = "data/stream/itemfavor_new.csv"
        self.itemgetDetail = "data/stream.itemgetDetail_new.csv"

    def mergeStatic(self, stream_path):
        df = pd.read_csv(stream_path)

    def countBehavior(self, stream_path):
        df = pd.read_csv(stream_path)
        user_behave = dict()
        for idx, row in df.iterrows():
            break


if __name__ == '__main__':
    prep = Prep()
    df = pd.read_csv(prep.itemcart)
    print(df.head())
