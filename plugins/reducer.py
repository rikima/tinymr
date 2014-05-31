#!/usr/bin/env python
#!-*- coding: utf-8 -*-
import time

from plugin import Plugin

def get_instance(conf):
    return Reducer(conf)



class Reducer(Plugin):
    
    def __init__(self, conf):
        self.suffix = 'reduced'
        self.set_conf(conf)
    
    def execute(self, data):
        t = time.time()

        print 'reducing...',

        Plugin.init(self, data)

        self.target = data.get('in')
        out = '%s.reduced' %(self.target)
        data['in'] = out
        
        if self.skip:
            return data

        io = open(self.target, 'r')
        oio = open(out, 'w')

        pk = None
        pv = 0
        for i, l in enumerate(io):
            ss = l.strip().split(self.mr_delimiter)
            try:
                k = ss[0]
                v = int(ss[1])
            except:
                continue
            
            if pk and pk != k:
                oio.write('%s%s%d\n' %(pk, self.mr_delimiter, pv))
                pv = v
            else:
                pv += v

            pk = k
        

        # last
        oio.write('%s%s%d\n' %(pk, self.mr_delimiter, pv))

        io.close()
        oio.close()
        
        data['in'] = out
        
        Plugin.terminate(self, data)
        
        t = time.time() - t
        
        print ' .done ', t, '[ms]'
        
        return data

    
    

if __name__ == '__name__':
    print 'reducer.py'
