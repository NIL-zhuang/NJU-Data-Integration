# -*- coding: utf-8 -*-
import os
import pandas as pd
import csv
from tqdm import tqdm
import json

class Format2Category:
    def __init__(self, source, target):
        self.source = source
        self.target = target

        self.uri = {
            "/item/getDetail": ["userId", "itemId", "categoryId"],
            "/item/favor": ["userId", "itemId", "categoryId"],
            "/item/cart": ["userId", "itemId", "categoryId"],
            "/item/buy": ["userId", "itemId", "categoryId", "isSecondKill", "success"],
            "/user/login": ["userId", "password", "authCode", "success"]
        }

        if not os.path.exists(source):
            print("Source Not Found!")
        if not os.path.exists(target):
            os.mkdir(target)

    def clear(self):
        for filename in self.uri:
            with open(os.path.join(self.target, self.get_output_file_name(filename)), "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)

    def get_output_file_name(self, category):
        return os.path.join(self.target, category.replace("/", "") + ".csv")

    def run(self):
        self.clear()
        first = True
        for filename in tqdm(os.listdir(self.source)):
            if not filename.endswith("csv"):
                continue
            df = pd.read_csv(os.path.join(self.source, filename), encoding='utf-8', header=0)
            for category in tqdm(self.uri):
                t = df[df["URI"] == category].copy()
                columns = self.uri[category]
                for column in columns:
                    t[column] = t["REQUEST_BODY"].map(lambda s: json.loads(s)[column] if column in s else 0)
                target_column = t.columns.values.tolist()
                target_column.remove("REQUEST_BODY")
                if category != "/user/login":
                    target_column.remove("IPADDR")
                if first:
                    t.to_csv(self.get_output_file_name(category),
                             index=False, columns=target_column, mode="a+")
                else:
                    t.to_csv(self.get_output_file_name(category),
                             index=False, columns=target_column, mode="a+", header=None)
            first = False

if __name__ == '__main__':
    m = Format2Category("../format", "../category")
    m.run()
