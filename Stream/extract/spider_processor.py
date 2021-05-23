# -*- coding: utf-8 -*-

import pandas as pd
import os
from tqdm import tqdm

'''
找到爬虫机器人
'''
class SpiderProcessor:
    def __init__(self, source, target):
        self.source = source
        self.target = target

        if not os.path.exists(source):
            print("Source Not Found!")
        if not os.path.exists(target):
            os.mkdir(target)

    def run(self):
        print("Do Analyze")