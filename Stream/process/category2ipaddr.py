# -*- coding: utf-8 -*-
import pandas as pd
import os


class Category2Specific:
    def __init__(self, source, target):
        self.source = source
        self.target = target

        if not os.path.exists(source):
            print("Source Not Found!")
        if not os.path.exists(target):
            os.mkdir(target)

    def run(self, filename):
        print("Start", filename)
        df = pd.read_csv(os.path.join(self.source, filename), encoding='utf-8', header=0)
        df = df.sort_values(by=["IPADDR", "TIME"])
        df.to_csv(os.path.join(self.target, filename), index=False, mode="w")


if __name__ == '__main__':
    t = Category2Specific("../category", "../login")
    t.run("userlogin.csv")
