#!/usr/bin/env python
#! -*- encoding: utf-8 -*-


# name:r.py
# date:2010-02-16 15:51:23.388621
#
# 
# Time-stamp: " "
#
import sys

debug = True

stderr = sys.stderr
stdout = sys.stdout

if __name__ == '__main__':
    """
    main
    
    """
    if len(sys.argv) < 2:
        print 'please input size'
        sys.exit(1)


    size = int(sys.argv[1])
    
    import random
    i = 0
    while i < size:
        i += 1
        r = random.random()

        print '%f,%d' %(r, i)
