#!/usr/bin/env python
#!-*- coding: utf-8 -*-

from plugin import Plugin

def get_instance(conf):
    return Reducer(conf)



class Reducer(Plugin):


    def __init__(self, conf=None, delimiter="\t"):
        self.set_conf(conf)

        

    def execute(self, data):
        Plugin.init(self, data)
        
        self.target = data.get('in')
        out = '%s.reduced' %(self.target)

        io = open(self.target, 'r')
        oio = open(out, 'w')

        for i, l in enumerate(io):
            ss = l.strip().split(self.mr_delimiter)
            try:
                k = ss[0]
                v = ss[1]
            except:
                continue

            oio.write('%s%s%s\n' %(v, self.mr_delimiter, k))


        io.close()
        oio.close()
        
        data['in'] = out
        
        Plugin.terminate(self, data)
        return data

    
    

if __name__ == '__name__':
    print 'reducer.py'
