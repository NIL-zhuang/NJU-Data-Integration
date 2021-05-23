# -*- coding: utf-8 -*-
import os
import pandas as pd


class SwipeProcessor:
    def __init__(self, source, target):
        self.source = source
        self.target = target

        if not os.path.exists(source):
            print("Source Not Found!")
        if not os.path.exists(target):
            os.mkdir(target)

    def run(self):
        print("Do Analyze")
