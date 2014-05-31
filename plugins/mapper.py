#!/usr/bin/env python
#!-*- coding: utf-8 -*-

import sys

from plugin import Plugin

def get_instance(conf):
    return Mapper(conf)

class Mapper(Plugin):
    def __init__(self, conf):
        self.suffix = 'mapped'
        self.set_conf(conf)

    def execute(self, data):
        Plugin.init(self, data)

        self.target = data.get('in')

        if not self.target:
            return data

        out = '%s.%s' %(self.target, self.suffix)
        data['in'] = out
        
        oio = open(out, 'w')
        io = open(self.target, 'r')
        for i, l in enumerate(io):
            ss = l.strip().split(self.mr_delimiter)
            
            k = ss[0]
            vs = ss[1]
            
            oio.write('%s%s1\n' %(vs, self.mr_delimiter))
            
        io.close()
        
        Plugin.terminate(self, data)
        
        return data
    
