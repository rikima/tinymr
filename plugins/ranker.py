#!/usr/bin/env python
#!-*- coding: utf-8 -*-

from plugin import Plugin

def get_instance(conf):
    return Ranker(conf)



class Ranker(Plugin):
    def __init__(self, conf=None, delimiter="\t"):
        self.set_conf(conf)

        

    def execute(self, data):
        Plugin.init(self, data)
        
        self.target = data.get('in')
        out = '%s.formatted' %(self.target)

        io = open(self.target, 'r')
        oio = open(out, 'w')

        for i, l in enumerate(io):
            ss = l.strip().split(self.mr_delimiter)
            try:
                k = ss[0]
                v = float(ss[1])
                v = int(v)
                v = str(v)
            except Exception, e:
                print e
                print ss
            
            oio.write('%d%s%s%s%s\n' %(i+1, self.mr_delimiter, v, self.mr_delimiter, k))


        io.close()
        oio.close()
        
        data['in'] = out
        
        Plugin.terminate(self, data)
        return data

    
    

if __name__ == '__main__':
    print 'formatter.py'
    
    p = get_instance({})
    print p
