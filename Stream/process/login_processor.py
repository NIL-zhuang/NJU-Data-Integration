# -*- coding: utf-8 -*-
import os
import csv
import pandas as pd
from tqdm import tqdm

class LoginProcessor:
    def __init__(self, source, target):
        self.source = source
        self.target = target

        if not os.path.exists(source):
            print("Source Not Found!")
        if not os.path.exists(target):
            os.mkdir(target)

    def run(self):
        data = {}
        with open(self.source, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            first = True
            for line in tqdm(reader):
                if first:
                    first = False
                    continue
                if(line[3] not in data):
                    data[line[3]] = [line[3], 0, 0]
                if eval(line[-1]) == 0:
                    data[line[3]][1] += 1
                else:
                    data[line[3]][2] += 1

        for key in data:
            total = data[key][1] + data[key][2]
            rate = data[key][1] / total * 1.0
            data[key].append(rate)

        with open(os.path.join(self.target, "ip_login.csv"), 'w', newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for key in data:
                if data[key][1] + data[key][2] < 5 or data[key][3] != 0.0:
                    continue
                writer.writerow(data[key])

if __name__ == '__main__':
    lp = LoginProcessor("../login/userlogin.csv", "../login")
    lp.run()