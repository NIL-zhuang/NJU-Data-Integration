# -*- coding: utf-8 -*-

import pandas as pd
from tqdm import tqdm
import os


class Category2Specific:
    def __init__(self, source, target):
        self.source = source
        self.target = target

        if not os.path.exists(source):
            print("Source Not Found!")
        if not os.path.exists(target):
            os.mkdir(target)

    def run(self, value):
        for filename in tqdm(os.listdir(self.source)):
            print("Start", filename)
            df = pd.read_csv(os.path.join(self.source, filename), encoding='utf-8', header=0)
            df = df.sort_values(by=value)
            df.to_csv(os.path.join(self.target, filename),
                      index=False, mode="w")


if __name__ == '__main__':
    t = Category2Specific("../category", "../sessionId")
    t.run("SESSION_ID")
