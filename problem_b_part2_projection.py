'''
Created on Mar 8, 2018

@author: rabihkodeih
'''

import os
import sys
import csv
import multiprocessing as mp

from functools import wraps
from datetime import datetime


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, 'data', 'output', 'problem_b_first_part.csv')

global TARGET_NODES
TARGET_NODES = {}

global DISEASE_NODES
DISEASE_NODES = {}


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
# Main Functions
#===============================================================================
@measure_time
def updateNodeLists():    
    with open(DATA_PATH) as csvfile:
        reader = csv.reader(csvfile)
        disease_map = {}
        target_map = {}
        target_count, disease_count = 0, 0
        for ith, row in enumerate(reader):
            if ith % 100000 == 0:
                sys.stdout.write('updateNodeLists: processed %s rows\n' % ith)
            target_id, disease_id = row[0], row[1]
            if target_id not in target_map:
                target_map[target_id] = target_count
                target_count += 1
            if disease_id not in disease_map:
                disease_map[disease_id] = disease_count
                disease_count += 1
            tind, dind = target_map[target_id], disease_map[disease_id]
            if tind not in TARGET_NODES: TARGET_NODES[tind] = []
            if dind not in DISEASE_NODES: DISEASE_NODES[dind] = []
            TARGET_NODES[tind].append(dind)
            DISEASE_NODES[dind].append(tind)


def countWorker(job_id, chunk):
    sys.stdout.write('job %s starting with chunk %s:\n' % (job_id, chunk))
    pair_count = 0
    a, b = chunk
    chunk_size = b - a + 1 
    for ith, (target, diseases) in enumerate(list(TARGET_NODES.items())[a:b + 1]):
        if (ith + 1) % 500 == 0:
            sys.stdout.write('job %s: processed %s rows out of %s, (%s)\n' % 
                             (job_id, ith + 1, chunk_size, pair_count))
        pairs = {}
        for d in diseases:
            for related_target in DISEASE_NODES[d]:
                if related_target > target:
                    if related_target not in pairs: pairs[related_target] = 0
                    pairs[related_target] += 1
        pair_count += sum(1 for v in pairs.values() if v > 1)
    return pair_count


def getJobs(num_jobs):
    total = len(TARGET_NODES)
    chunk_size = total // num_jobs
    remaining = total % chunk_size
    jobs = [[a*chunk_size, (a + 1)*chunk_size - 1] for a in range(num_jobs)]
    if remaining:
        jobs.append([num_jobs*chunk_size, total - 1])
    if len(jobs[-1]) < 100:
        jobs[-2][1] = jobs[-1][1]
        jobs = jobs[:-1]
    jobs = [(ith + 1, tuple(j)) for ith, j in enumerate(jobs)]
    return jobs 
    
 
@measure_time
def countPairs(num_jobs):
    jobs = getJobs(num_jobs)
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

    
#===============================================================================
# Main Script
#===============================================================================
if __name__ == '__main__':
    sys.stdout.write('starting...\n\n')
    num_jobs = 10 * mp.cpu_count() 
    updateNodeLists()
    result = countPairs(num_jobs)
    sys.stdout.write('number of pairs: %s\n' % result)
    sys.stdout.write('\n\ndone')

    
#===========================================================================
# Output
#===========================================================================
# Operation completed in 0:00:03.158954 seconds
# ----------
# Generated 40 jobs with the following chunks:
# 1: (0, 826)
# 2: (827, 1653)
# 3: (1654, 2480)
# ...
# 40: (32253, 33108)
# ----------
# Operation completed in 0:02:52.945630 seconds
# number of pairs: 121114622


    
    
    
