#!/usr/bin/env python
#!-*- coding: utf-8 -*-

import sys

from plugin import Plugin
from mapper import Mapper

def get_instance(conf):
    return Loader(conf)


class Loader(Plugin):

    def __init__(self, conf=None, delimiter=","):
        self.delimiter = delimiter
        self.set_conf(conf)
                
    def execute(self, data):
        target = self.target
        out = '%s' %(target)
        
        data['in'] = out
        return data
