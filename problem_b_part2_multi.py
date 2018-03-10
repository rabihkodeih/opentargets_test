'''
Created on Mar 8, 2018

@author: rabihkodeih
'''

import os
import sys
import csv
import operator as op
import multiprocessing as mp

from functools import reduce
from functools import wraps
from datetime import datetime


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, 'data', 'output', 'problem_b_first_part.csv')

global DATA
DATA = []


#===============================================================================
# Decorators
#===============================================================================

def measure_time(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        t_start = datetime.now()
        result = func(*args, **kwargs)
        t_end = datetime.now()
        sys.stdout.write('Operation completed in %s seconds\n' % (t_end - t_start))
        return result
    return wrapped


#===============================================================================
# Utility Functions
#===============================================================================

def nCr(n, r):
    if n == 0: return 0
    r = min(r, n-r)
    numer = reduce(op.mul, range(n, n-r, -1), 1)
    denom = reduce(op.mul, range(1, r+1), 1)
    return numer//denom


def binarySearch(data_func, item, lower, upper):
    first, last = lower, upper
    result = 0
    while first <= last:
        midpoint = (first + last) // 2
        v = data_func(midpoint)
        v_next = data_func(midpoint + 1)
        if v <= item and v_next > item:
            result = midpoint
            break
        else:
            if item < data_func(midpoint):
                last = midpoint - 1
            else:
                first = midpoint + 1
    return result
 
 
def binomialDecomposition(v):
    j, result = v, []
    for i in [2, 1]:
        lower, upper = 0, 1
        while nCr(upper, i) < j:
            lower, upper = upper, 2*upper
        r = binarySearch(lambda x: nCr(x, i), j, lower, upper)
        result.append(r)
        j -= nCr(r, i)
    return result


def find2CombinationFromIndex(index, data_len):
    #ref: http://vlkan.com/blog/post/2013/12/04/combinations/
    n = data_len
    total = nCr(n, 2)
    if index == total - 1: return (n - 2, n - 1)
    if index == total - 2: return (n - 3, n - 1)
    j = total - index - 1
    a, b = binomialDecomposition(j)
    a, b = n - a - 1, n - b - 1
    return a, b


def partition2Combinations(n, num_chunks):
    total = nCr(n, 2)
    chunk_size = total // num_chunks
    remaining = total % chunk_size
    result = [(a*chunk_size, (a + 1)*chunk_size - 1) for a in range(num_chunks)]
    if remaining:
        result.append((num_chunks*chunk_size, total - 1))
    return result

    
def generatePairs(data, from_i=0, from_j=1, num_combs=None):    
    i, j = from_i, from_j
    n = len(data)
    count = 0
    while i <= n - 1:
        if j is None: j = i + 1
        while j <= n - 1:
            yield (data[i], data[j])
            j += 1
            count += 1
            if count == num_combs: return
        i += 1
        j = None


#===============================================================================
# Main Functions
#===============================================================================

@measure_time
def getDataMap():
    data_map = {}
    disease_map = {}
    disease_count = 0
    with open(DATA_PATH) as csvfile:
        reader = csv.reader(csvfile)
        for ith, row in enumerate(reader):
            if ith % 100000 == 0:
                sys.stdout.write('data_map: processed %s rows\n' % ith)
            target_id, disease_id = row[0], row[1]
            if target_id not in data_map: data_map[target_id] = set()
            if disease_id not in disease_map:
                disease_map[disease_id] = disease_count
                disease_count += 1
            data_map[target_id].add(disease_map[disease_id])
    return data_map


def countWorker(job_id, chunk):
    sys.stdout.write('job %s starting with chunk %s:\n' % (job_id, chunk))
    pair_count = 0
    a, b = chunk
    chunk_size = b - a + 1
    i_from, j_from = find2CombinationFromIndex(a, len(DATA))
    for ith, (v1, v2) in enumerate(generatePairs(DATA, i_from, j_from, num_combs=chunk_size)):
        if len(v1.intersection(v2)) > 1:
            pair_count += 1
        if (ith + 1) % int(1e6) == 0:
            sys.stdout.write('job %s: processed %s M rows out of %s Ms, (%s)\n' % 
                             (job_id, (ith + 1)/1e6, chunk_size/1e6, pair_count))
    return pair_count


@measure_time
def countPairs(data_map, num_jobs):        
    for v in data_map.values():
        DATA.append(v)
    n = len(DATA)
    chunks = partition2Combinations(n, num_jobs)
    jobs = [(i + 1, chunk) for i, chunk in enumerate(chunks)]
    sys.stdout.write('-'* 10 + '\n')
    sys.stdout.write('Generated %s jobs with the following chunks:\n' % len(jobs))
    for jid, chunk in jobs:
        sys.stdout.write('%s: %s\n' % (jid, chunk))
    sys.stdout.write('-'* 10 + '\n')
    pool = mp.Pool(processes=mp.cpu_count())
    results = pool.starmap(countWorker, jobs, chunksize=1)
    pool.close()
    pool.join()
    final_result = sum(results)
    return final_result


@measure_time
def testCombinationEnumeration():
    sys.stdout.write('testCombinationEnumeration started...\n')
    for data in (range(1), range(2), range(3), range(10), range(50), range(100)):
        n = len(data)
        index = 0
        for a, b in generatePairs(data):
            x, y = find2CombinationFromIndex(index, n)
            assert a == x
            assert b == y
            index += 1
    sys.stdout.write('testCombinationEnumeration completed successfully\n')


#===============================================================================
# Main Script
#===============================================================================
if __name__ == '__main__':
    sys.stdout.write('starting...\n\n')
    testCombinationEnumeration()
    num_jobs = 10 * mp.cpu_count() 
    data_map = getDataMap()
    result = countPairs(data_map, num_jobs)
    sys.stdout.write('number of pairs: %s\n' % result)
    sys.stdout.write('\n\ndone')

#===========================================================================
# Output
#===========================================================================
# testCombinationEnumeration started...
# testCombinationEnumeration completed successfully
# Operation completed in 0:00:00.526849 seconds
# Operation completed in 0:00:02.632012 seconds
# ----------
# Generated 6 jobs with the following chunks:
# 1: (0, 91347730)
# 2: (91347731, 182695461)
# 3: (182695462, 274043192)
# 4: (274043193, 365390923)
# 5: (365390924, 456738654)
# 6: (456738655, 548086385)
# ----------
# Operation completed in 0:06:37.995477 seconds
# number of pairs: 121114622

    
    
    
