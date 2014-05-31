#!/usr/bin/env python
#!-*- coding: utf-8 -*-
import sys
import time
import os
import multiprocessing

from plugin import Plugin

debug = False
stdout = sys.stdout
stderr = sys.stderr

package_name = 'mrsystem'

def get_instance(conf):
    return Sorter(conf)



class OnMemorySortWorker(multiprocessing.Process):
    def __init__(self, sorter, index, target, num_threads):
        multiprocessing.Process.__init__(self)

        self.index = index
        self.io = open(target, 'r')
        self.num_threads = num_threads
        self.sorter = sorter
        self.buf = []


    def _output_buffer(self, buf, index):
        buf.sort(lambda x, y: cmp(y[0], x[0]))
        fname = '%s/%s.%d' %(self.sorter.temp_dir, self.sorter.temp_name, index)

        t = time.time()
        io = open(fname, 'w')
        for k, v in self.buf:
            io.write('%s%s%s\n' %(str(k), self.sorter.mr_delimiter, str(v)))

        io.close()
        t = time.time() - t
        
        #print '_output_buffer', t, '[t] thread', self.index, '#buf', len(buf)
        
        self.buf = []


    def run(self):
        j = 0
        c = 0	
        t = time.time()
        for i, l in enumerate(self.io):
            if i % self.num_threads != self.index:
                continue

            l = l.strip()
            
            ss = l.strip().split(self.sorter.mr_delimiter)
            if len(ss) < 2:
                continue

            if self.sorter.numeric:
                ss[0] = float(ss[0])

            self.buf.append((ss[0], ss[1]))
            j = c / self.sorter.unit
            k = c % self.sorter.unit
            
            c += 1
            if j > 0 and k == 0:
                #print '#buf', len(self.buf), j, 'thread', self.index
                tmp_index = ((j-1) * self.num_threads)+self.index 
                self._output_buffer(self.buf, tmp_index+1)
                self.buf = []
        
        if j > 0:
            tmp_index = ((j-1)*self.num_threads)+self.index
        else:
            tmp_index = (j*self.num_threads)+self.index
            
        self._output_buffer(self.buf, tmp_index+1)
        
        t = time.time() - t
        #print '#processed', c , "in thread", self.index, t, "[s]"





class MergeSortWorker(multiprocessing.Process):
    def __init__(self, sorter, index1, index2, num_threads):
        multiprocessing.Process.__init__(self)
        
        self.index1 = index1
        self.index2 = index2

        self.temp = '%s/%s.%d.%d' %(sorter.temp_dir, sorter.temp_name, index1, index2)
        self.output_io = open(self.temp, 'w')
        self.num_threads = num_threads
        self.sorter = sorter
        self.buf = []


    def run(self):
        """
        2つの配列のマージソート
        
        """
        target1 = '%s/%s.%d' %(self.sorter.temp_dir, self.sorter.temp_name, self.index1)
        io1 = open(target1, 'r')
        
        target2 = '%s/%s.%d' %(self.sorter.temp_dir, self.sorter.temp_name, self.index2)
        io2 = open(target2, 'r')
        
        l1 = io1.readline().strip()
        ss1 = l1.split(self.sorter.mr_delimiter)
            
        l2 = io2.readline().strip()
        ss2 = l2.split(self.sorter.mr_delimiter)
        
        while 1:
            if self.sorter.numeric:
                k_1 = float(ss1[0])
                k_2 = float(ss2[0])
            else:
                k_1 = ss1[0]
                k_2 = ss2[0]
            
            try:
                v_1 = ss1[1]
                v_2 = ss2[1]
            except IndexError, e:
                print e
                print ss1[0], ss2[0]
                sys.exit(1)
                
            if k_1 > k_2:
                k_o = k_1
                v_o = v_1
                
            else:
                k_o = k_2
                v_o = v_2
                
            # output
            self.output_io.write('%s%s%s\n' %(str(k_o), self.sorter.mr_delimiter, str(v_o)))
            
            if k_1 > k_2:
                l1 = io1.readline().strip()
                if not l1:
                    break
                ss1 = l1.split(self.sorter.mr_delimiter)
            else:
                l2 = io2.readline().strip()
                if not l2:
                    break
                ss2 = l2.split(self.sorter.mr_delimiter)

        self.output_io.close()

        # i_1にrename
        #print 'rename', self.temp
        os.rename(self.temp, '%s/%s.%d' %(self.sorter.temp_dir, self.sorter.temp_name, self.index1))
        
        f2 = '%s.%d' %(self.sorter.temp_name, self.index2)
        os.remove(os.path.join(self.sorter.temp_dir, f2))



    
class Sorter(Plugin):
    def __init__(self, conf=None, unit=10000):
        self.unit = unit

        self.temp_name = 'tmp'
        self.temp_dir = '%s/tmp' %(os.getcwd())
            
        self.buf = []
        self.numeric = 0
        
        self.num_threads = 10

        self.suffix = 'sorted'
        self.temp_clean = True
        self.skip = False
        self.debug = False
        
        self.set_conf(conf)
        
        if not os.path.exists(self.temp_dir):
            os.mkdir(self.temp_dir)



    def execute(self, data):
        Plugin.init(self, data)
        self.target = data.get('in')

        sorted = '%s.%s' %(self.target, self.suffix)
        # add to data
        data['sorted'] = sorted
        data['in'] = sorted
        
        if self.skip:
            return data

        t = time.time()

        stdout.write('# process split sort ...')

        j = 0
        io = open(self.target, 'r')
        
        # tmp dirをクリア
        stderr.write('# clear temp files ... ')
        if self.temp_clean and os.path.exists(self.temp_dir):
            for f in os.listdir(self.temp_dir):
                os.remove(os.path.join(self.temp_dir, f))
        stderr.write(' .done\n')

        #
        # ここをワーカースレッドで並列化
        #
        workers = []
        for i in range(self.num_threads):
            w = OnMemorySortWorker(self, i, self.target, self.num_threads)
            workers.append(w)


        for w in workers:
            w.start()

        for w in workers:
            w.join()

        t = time.time() - t
        stdout.write(' done. %f [s]\n' %(t))

        # merge
        t = time.time()
        sorted = '%s.%s' %(self.target, self.suffix)
        stdout.write('# process merge sort and output to %s ...' %(sorted))

        # new merge sort
        fs = os.listdir(self.temp_dir)
        num_tmp_files = len(fs)
        while True:
            self._merge_sort(sorted, num_tmp_files)
            
            fs = os.listdir(self.temp_dir)
            if len(fs) == 1:
                break

        # sortedを
        if os.path.exists(sorted):
            os.remove(sorted)
        
        #print fs[0]
        os.rename('%s/%s' %(self.temp_dir, fs[0]), sorted)

        t = time.time() - t
        stdout.write(' done. %f [s]\n' %(t))


        # add to data
        data['sorted'] = sorted
        data['in'] = sorted
        
        Plugin.terminate(self, data)
        return data



    def terminate(self):
        stderr.write('# clear temp files ... ')
        if self.temp_clean and os.path.exists(self.temp_dir):
            for f in os.listdir(self.temp_dir):
                os.remove(os.path.join(self.temp_dir, f))
        stderr.write(' .done\n')


        
    def _merge_sort(self, sorted, split_index):
        workers = []
        
        
        fs = os.listdir(self.temp_dir)
        
        while True:
            
            fs = os.listdir(self.temp_dir)
            size = len(fs)
            
            f1 = None
            f2 = None

            for f in fs:
                ss = f.split('.')
                if len(ss) >= 3:
                    continue

                if not f1:
                    f1 = f
                    continue

                if not f2:
                    f2 = f
                    if not f1 or not f2:
                        continue
                

                i_1 = int(f1.split('.')[1])
                i_2 = int(f2.split('.')[1])
                                
                w = MergeSortWorker(self, i_1, i_2, self.num_threads)
                f1 = None
                f2 = None

                #print '#workers', len(workers)

                if len(workers) < self.num_threads:
                    workers.append(w)
                    #print 'append ', w, i_1, i_2, len(workers)
                    continue
                else:
                    for w in workers:
                        w.start()

                    for w in workers:
                        w.join()
                    
                    workers = []

                f1 = None
                f2 = None

            #print 'out of loop', workers
            for w in workers:
                w.start()

            for w in workers:
                w.join()
        

            if len(workers) == 0:
                #break
                return 
            else:
                workers = []


    
    def _merge_sort_for_2instance(self, output_io, index1, index2):
        """
        2つの配列のマージソート
        
        """
        target1 = '%s/%s.%d' %(self.temp_dir, self.temp_name, index1)
        io1 = open(target1, 'r')
        
        target2 = '%s/%s.%d' %(self.temp_dir, self.temp_name, index2)
        io2 = open(target2, 'r')
        
        l1 = io1.readline().strip()
        ss1 = l1.split(self.mr_delimiter)
            
        l2 = io2.readline().strip()
        ss2 = l2.split(self.mr_delimiter)
        
        while 1:
            if self.numeric:
                k_1 = float(ss1[0])
                k_2 = float(ss2[0])
            else:
                k_1 = ss1[0]
                k_2 = ss2[0]
            
            try:
                v_1 = ss1[1]
                v_2 = ss2[1]
            except IndexError, e:
                print e
                print ss1[0], ss2[0]
                sys.exit(1)
                
            if k_1 > k_2:
                k_o = k_1
                v_o = v_1
                
            else:
                k_o = k_2
                v_o = v_2
                
            # output
            output_io.write('%s%s%s\n' %(str(k_o), self.mr_delimiter, str(v_o)))
            
            if k_1 > k_2:
                l1 = io1.readline().strip()
                if not l1:
                    break
                ss1 = l1.split(self.mr_delimiter)
            else:
                l2 = io2.readline().strip()
                if not l2:
                    break
                ss2 = l2.split(self.mr_delimiter)
            
        #残り
        l1 = io1.readline().strip()
        while l1:
            output_io.write('%s\n' %(l1))
            l1 = io1.readline().strip()
        io1.close()

        l2 = io2.readline().strip()
        while l2:
            output_io.write('%s\n' %(l2))
            l2 = io2.readline().strip()
            
        io2.close()
               
        # 削除する
        os.remove(target1)
        os.remove(target2)
                
                
                
    def _merge_output(self, output_io, split_index):

        ios = []
        # open temp files
        indices = range(split_index+1)
        for j in indices:
            target = '%s/%s.%d' %(self.temp_dir, self.temp_name, j)
            io = open(target, 'r')

            l = io.readline().strip()
            ss = l.split(self.mr_delimiter)
            if len(ss) == 2:
                if self.numeric:
                    ios.append((float(ss[0]), ss[1], io))
                else:
                    ios.append((ss[0], ss[1], io))

                
        ios.sort(lambda x, y:cmp(y[0], x[0]))
        
        # merge output
        while 1:
            if len(ios) == 0:
                break

            min_k = ios[0][0]
            min_v = ios[0][1]

            # output
            output_io.write('%s%s%s\n' %(str(min_k), self.mr_delimiter, str(min_v)))
            io = ios[0][-1]
            l_i = io.readline().strip()
            if l_i:
                ss = l_i.split(self.mr_delimiter)
                k_i = ss[0]
                if self.numeric:
                    k_i = float(k_i)
                    
                v_i = ss[1]
                j = 1
                l_j = None
                io_j = None
                for k_j, v_j, io_j in ios[1:]:
                    #if k_i <=  k_j:
                    if k_i >=  k_j:
                        break

                    j += 1
                    
                ios.insert(j, (k_i, v_i, io))  


            del ios[0]



        

if __name__ == '__main__':

    # conf
    conf = dict(numeric=1, unit=100000)

    # init data
    data = dict(target_file="./test/test.txt")

    ex = get_instance(conf)
    data = ex.execute(data)

    target_file = data.get('target_file')
    io = open(target_file, 'r')
    td = io.readlines()
    td.sort()

    sorted_file = data.get('sorted')
    io = open(sorted_file, 'r')
    sd = io.readlines()

    for i, l in enumerate(td):
        
        sl = sd[i]

        try:
            k_l = l.split(',')[0]
            k_sl = sl.split(',')[0]

            assert k_l == k_sl
            
        except:
            print "#", i, l, sl
            sys.exit(1)



    print 'test ok'
    
    
