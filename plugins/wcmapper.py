#!/usr/bin/env python
#!-*- coding: utf-8 -*-

import sys

from mapper import Mapper
try:
    import mecabparser
except:
    pass

def get_instance(conf):
    return WCMapper(conf)

class WCMapper(Mapper):

    def __init__(self, conf, delimiter=' '):
        self.delimiter = delimiter
        
        self.set_conf(conf)

    
    def execute(self, data):
        
        print 'execute', data
        print 'self.target', self.target

        
        io = open(self.target, 'r')
        oo = open(self.output, 'w')
        for i, l in enumerate(io):
            ss = l.strip().split(self.delimiter)
            if self.debug:
                print ss
            
            for s in ss:
                if s and len(s) > 0:
                    oo.write('%s%s1\n' %(s, self.mr_delimiter))

            
        oo.close()
        io.close()

        data['in'] = self.output

        return data
        
if __name__ == '__main__':
    print 'mapper.py'
    
