#!/usr/bin/env python
#!-*- coding: utf-8 -*-

import sys

from plugin import Plugin
from mapper import Mapper

def get_instance(conf):
    return InverseMapper(conf)

class InverseMapper(Mapper):

    def __init__(self, conf):
        self.suffix = 'mapped'
        self.set_conf(conf)

    
    def execute(self, data):
        Plugin.init(self, data)
        
        self.target = data.get('in')
        output = '%s.%s' %(self.target, self.suffix)
        data['in'] = output
        
        if self.skip:
            return data
        
        io = open(self.target, 'r')
        oo = open(output, 'w')
        for i, l in enumerate(io):
            ss = l.strip().split(self.mr_delimiter)
            try:
                k = ss[0]
                v = ss[1]
            except:
                continue
            
            oo.write('%s%s%s\n' %(v, self.mr_delimiter, k))

            
        oo.close()
        io.close()



        Plugin.terminate(self, data)
        
        return data
        
if __name__ == '__main__':
    print 'mapper.py'
    
