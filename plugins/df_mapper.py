#!/usr/bin/env python
#!-*- coding: utf-8 -*-

import sys
import re

from mapper import Mapper

def get_instance(conf):
    return Dumper(conf)

class Dumper(Mapper):

    
    def __init__(self, conf, delimiter=','):
        self.delimiter = delimiter
        self.suffix = 'dump'
        self.skip = False
        
        self.set_conf(conf)

        self.reject_pat = re.compile(r'\d+')



    def execute(self, data):
        target = data.get('in')
        output = '%s.%s' %(target, self.suffix)
        
        data['in'] = output
        if self.skip:
            return data


        io = open(target, 'r')
        oo = open(output, 'w')
        
        buf = set()

        zc = 0
        
        for i, l in enumerate(io):
            print i, l

        oo.close()
        io.close()


        return data

        
    
