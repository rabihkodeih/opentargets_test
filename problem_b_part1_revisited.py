'''
Created on Mar 6, 2018

@author: rabihkodeih
'''

import os
import sys
import csv
from datetime import datetime
import multiprocessing as mp
import ujson  # @UnresolvedImport
from functools import wraps


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, 'data')

data_map = {}


def getMedian(data):
    n = len(data)
    if n == 0:
        return 0
    if n % 2 == 1:
        return data[n//2]
    else:
        i = n // 2
        return (data[i - 1] + data[i])/2


def measure_time(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        t_start = datetime.now()
        result = func(*args, **kwargs)
        t_end = datetime.now()
        sys.stdout.write('Operation completed in %s seconds\n' % (t_end - t_start))
        return result
    return wrapped


def worker(jsonline):
    data = ujson.loads(jsonline)
    target_id = data['target']['id']
    disease_id = data['disease']['id']
    association_score = data['scores']['association_score']
    return (target_id, disease_id, association_score) 
    
    
@measure_time
def handleData(data_path):
    pool = mp.Pool(processes=mp.cpu_count())
    jsonsrc = open(data_path, 'rb')
    for ith, result in enumerate(pool.imap_unordered(worker, jsonsrc, chunksize=1000)):
        if ith % 100000 == 0:
            sys.stdout.write('read %s data lines\n' % ith)
        target_id, disease_id, association_score = result
        if target_id not in data_map:
            data_map[target_id] = {}
        if disease_id not in data_map[target_id]:
            data_map[target_id][disease_id] = []
        data_map[target_id][disease_id].append(association_score)
    pool.close()
    pool.join()
    jsonsrc.close()
    

@measure_time
def outputStageProcessing(data_map, output_path):
    big_list = []
    for target_id, v in data_map.items():
        for disease_id, scores in v.items():
            scores.sort()
            ln = len(scores)
            if ln <= 3:
                top3 = scores
            else:
                top3 = scores[ln - 3:ln]
            median = getMedian(scores)
            big_list.append((median, target_id, disease_id, top3))
    big_list.sort()
    write_path = os.path.join(output_path, 'problem_b_first_part_rev.csv')
    with open(write_path, 'w') as csvfile:
        writer = csv.writer(csvfile)
        for median, target_id, disease_id, top3 in big_list:
            row = [target_id, disease_id, median]
            row.extend(top3)
            writer.writerow(row)


if __name__ == '__main__':
    sys.stdout.write('starting...\n\n')
    
    data_path = os.path.join(DATA_PATH, 'input', '17.12_evidence_data.json')
    output_path = os.path.join(DATA_PATH, 'output')
    assert os.path.exists(output_path)
    handleData(data_path)
    outputStageProcessing(data_map, output_path)
    
    sys.stdout.write('\n\ndone')

    # Operation completed in 0:02:29.665445 seconds
    # Operation completed in 0:00:12.608518 seconds








