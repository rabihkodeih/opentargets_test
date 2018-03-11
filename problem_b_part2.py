'''
Created on Mar 6, 2018

@author: rabihkodeih
'''

import os
import sys
import csv
import multiprocessing as mp
from datetime import datetime
from functools import wraps


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, 'data')
LIMIT = -1


def measure_time(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        t_start = datetime.now()
        result = func(*args, **kwargs)
        t_end = datetime.now()
        sys.stdout.write('Operation completed in %s seconds\n' % (t_end - t_start))
        return result
    return wrapped


@measure_time
def getDataMap(data_path):
    data_map = {}
    with open(data_path) as csvfile:
        reader = csv.reader(csvfile)
        for ith, row in enumerate(reader):
            if ith % 100000 == 0:
                sys.stdout.write('data_map: processed %s rows\n' % ith)
            target_id, disease_id = row[0], row[1]
            if target_id not in data_map: data_map[target_id] = []
            data_map[target_id].append(disease_id)
            if ith == LIMIT: break
    return data_map


@measure_time
def countSingle(data_map):
    pair_count = 0
    total = (len(data_map) * (len(data_map) - 1)) / 2000000
    items = list(data_map.values())
    row_count = 0
    for ith, v1 in enumerate(items[:-1]):
        for v2 in items[ith + 1:]:
            if  len(set(v1).intersection(set(v2))) > 1:
                pair_count += 1
            if row_count % 1000000 == 0:
                sys.stdout.write('count_single: processed %s M rows out of %s Ms, (%s)\n' % (row_count/1000000, total, pair_count))
            row_count += 1
    return pair_count
    
    
def worker(pairs):
    count = 0
    for v1, v2 in pairs:
        v = len(set(v1).intersection(set(v2)))
        if v > 1:
            count += 1
    return count
    

@measure_time
def countMulti(data_map):
    num_chunks = mp.cpu_count() - 1
    chunk_size = 1000
    pair_count = 0
    total = (len(data_map) * (len(data_map) - 1)) / 2000000
    data = []
    chunk = []
    items = list(data_map.values())
    pool = mp.Pool()
    row_count = 0
    for ith, target1 in enumerate(items[:-1]):
        for target2 in items[ith + 1:]:
            chunk.append((target1, target2))
            if ith % (chunk_size) == chunk_size - 1:
                data.append(chunk)
                chunk = []
                if len(data) == num_chunks:
                    pair_count += sum(pool.map(worker, data))
                    data = []
            if row_count % 500000 == 0:
                sys.stdout.write('count_single: processed %s M rows out of %s Ms, (%s)\n' % (row_count/1000000, total, pair_count))
            row_count += 1
    if chunk: data.append(chunk)
    pair_count += sum(pool.map(worker, data))
    pool.close()
    pool.join()
    return pair_count



def worker2(pair):
    v1, v2 = pair
    v = len(set(v1).intersection(set(v2)))
    if v > 1:
        return 1
    return 0


@measure_time
def countMulti2(data_map):
    pool = mp.Pool()
    pair_count = 0
    total = (len(data_map) * (len(data_map) - 1)) / 2000000
    items = list(data_map.values())
    chunk = []
    row_count = 0
    for ith, v1 in enumerate(items[:-1]):
        for v2 in items[ith + 1:]:
            chunk.append((v1, v2))
            if len(chunk) == 10000:
                pair_count += sum(pool.map(worker2, chunk))
                chunk = []
            if row_count % 500000 == 0:
                sys.stdout.write('count_single: processed %s M rows out of %s Ms, (%s)\n' % (row_count/1000000, total, pair_count))
            row_count += 1
    pair_count += sum(pool.map(worker2, chunk))
    pool.close()
    pool.join()
    return pair_count


@measure_time
def createJobs(data_map, jobs_path, num_jobs):
    import shutil
    if os.path.exists(jobs_path):
        shutil.rmtree(jobs_path)
    os.makedirs(jobs_path)
    items = list(data_map.values())
    total = (len(data_map) * (len(data_map) - 1)) // 2
    chunk_size = total // num_jobs
    job_count = 0
    row_count = 0
    write_path = os.path.join(jobs_path, 'jobs_%s.csv' % job_count)
    job = open(write_path, 'w') 
    writer = csv.writer(job)
    sys.stdout.write('inintialized job %s\n' % job_count)
    for ith, v1 in enumerate(items[:-1]):
        for v2 in items[ith + 1:]:
            if row_count == chunk_size:
                row_count = 0
                job.close()
                sys.stdout.write('created job %s\n' % job_count)
                job_count += 1
                write_path = os.path.join(jobs_path, 'jobs_%s.csv' % job_count)
                job = open(write_path, 'w') 
                writer = csv.writer(job)
                sys.stdout.write('initialized job %s\n' % job_count)
            row_count += 1
            writer.writerow(v1)
            writer.writerow(v2)
    job.close()
    sys.stdout.write('created job %s\n' % job_count)
                
    

if __name__ == '__main__':
    sys.stdout.write('starting...\n\n')
    
    data_path = os.path.join(DATA_PATH, 'output', 'problem_b_first_part.csv')
    jobs_path = os.path.join(DATA_PATH, 'output', 'jobs')
    assert os.path.exists(data_path)
    
    data_map = getDataMap(data_path)
    result = countSingle(data_map)
    #result = countMulti(data_map)
    #result = countMulti2(data_map)
    sys.stdout.write('%s\n' % result)
    
    #Operation completed in 0:00:02.551176 seconds
    #Operation completed in 0:43:12.404540 seconds
    #121114622
    
    sys.stdout.write('\n\ndone')










