'''
Created on Mar 4, 2018

@author: rabihkodeih
'''

import sys
import requests
from datetime import datetime
import asyncio
import aiohttp
import numpy as np

HEADERS = {
    'user-agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/45.0.2454.101 Safari/537.36'),
}

ENDPOINT_URL = 'https://api.opentargets.io/v3/platform/public/association/filter'

    
def parseCommandLineArgs():
    args = sys.argv
    search_type = None
    if '-t' in args: search_type = 't'
    if '-d' in args: search_type = 'd'
    search_id = None
    try:
        if search_type == 't':
            search_id = args[args.index('-t') + 1]
        else:
            search_id = args[args.index('-d') + 1]
    except:
        pass
    try:
        size = int(args[args.index('-s') + 1])
    except:
        size = 10
    verbosity = '-q' not in args
    async = '--sync' not in args
    population_std_dev = '-p' in args
    is_test = '--test' in args 
    is_help = len(args) == (2 and ('--help' in args)) or (len(args) == 1)
    return search_type, search_id, size, verbosity, async, population_std_dev, is_test, is_help


def fetchScores(search_type, search_id, size, pfrom, verbose, totals_report=None):
    search_key = 'target' if search_type == 't' else 'disease'
    params = {search_key: search_id, 'size': size, 'from':pfrom}
    if verbose:
        if totals_report:
            sys.stdout.write('fetching scores (total: %s) with params %s\n' % (totals_report, params))
        else:
            sys.stdout.write('fetching scores with params %s\n' % params)
    response = requests.get(ENDPOINT_URL, headers=HEADERS, params=params)
    assert(response.status_code == 200)
    assert(response.headers['content-type'] == 'application/json') 
    json_result = response.json()
    total = json_result['total']
    scores = [row['association_score']['overall'] for row in json_result['data']]
    return total, scores


def fetchAllScores(search_type, search_id, size, async, verbose):
    if async == False:
        pfrom = 0
        total, scores = fetchScores(search_type, search_id, size, pfrom, verbose)
        for pfrom in range(size, total, size):
            _, partial_scores = fetchScores(search_type, search_id, size, pfrom, verbose, total)
            scores.extend(partial_scores)
        return scores
    else:
        pfrom = 0
        total, scores = fetchScores(search_type, search_id, size, pfrom, verbose)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncfetchAllScores(loop, scores, search_type, search_id, size, total, verbose))
        return scores


async def asyncfetchAllScores(loop, scores, search_type, search_id, size, total, verbose):
    async with aiohttp.ClientSession(loop=loop) as session:
        for pfrom in range(size, total, size):
            search_key = 'target' if search_type == 't' else 'disease'
            params = {search_key: search_id, 'size': size, 'from':pfrom}
            if verbose:
                sys.stdout.write('fetching scores (total: %s) with params %s\n' % (total, params))
            async with session.get(ENDPOINT_URL, headers=HEADERS, params=params) as response:
                json_result = await response.json()
                partial_scores = [row['association_score']['overall'] for row in json_result['data']]
                scores.extend(partial_scores)
    return scores


def computeResults(scores, use_sample_std_dev):
    max_v = max(scores) if scores else 0
    min_v = min(scores) if scores else 0
    mean = np.mean(scores) if scores else 0
    if use_sample_std_dev:
        std_dev = np.std(scores, ddof=1) if scores else 0
    else:
        std_dev = np.std(scores) if scores else 0
    return max_v, min_v, mean, std_dev


if __name__ == '__main__':
    
    search_type, search_id, size, verbosity, async, population_std_dev, is_test, is_help = parseCommandLineArgs()
    
    if is_help:
        help_message = """Target/Disease overall score comutation command, supported arguments are:
-t <target_id>     : to specify a target followed by a arget id
-d <disease_id>    : to specify a disease followed by a disease id
-s <size>          : to specify a size (default of 10)
--sync             : use synchronous fetching of data (default is asynchronous data fetching)
-p                 : to use population standard deviation (default is using sample standard deviation)
--test             : to run test cases
--help             : displays this help message
"""
        sys.stdout.write(help_message)
        exit()

    if is_test:
        test_cases = (
            ('t', 'ENSG00000157764'),
            ('d', 'EFO_0002422'), 
            ('d', 'EFO_0000616'),
        )
        results = (
            (1.0, 7.4006e-06, 0.321235479534, 0.3604306793),
            (1.0, 2.1e-07, 0.0547549997623, 0.120994621099),
            (1.0, 1.946e-07, 0.317462726813, 0.334038477862),
        )
        for (search_type, search_id), (tmax_v, tmin_v, tmean, tstd_dev) in zip(test_cases, results):
            pt = 'target' if search_type == 't' else 'disease'
            sys.stdout.write('Running test case for %s %s' % (pt, search_id))
            t_start = datetime.now()
            scores = fetchAllScores(search_type=search_type, 
                                    search_id=search_id, 
                                    size=size, 
                                    async=async, 
                                    verbose=verbosity)
            t_end = datetime.now()
            sys.stdout.write('All scores fetched in {0} seconds.\n'.format(t_end - t_start))
            max_v, min_v, mean, std_dev = computeResults(scores, use_sample_std_dev=not population_std_dev)
            np.testing.assert_almost_equal(max_v, tmax_v)
            np.testing.assert_almost_equal(min_v, tmin_v)
            np.testing.assert_almost_equal(mean, tmean)
            np.testing.assert_almost_equal(std_dev, tstd_dev)
            sys.stdout.write('test case passed\n')
        sys.stdout.write('All test cases passed.\n')
        exit()
        
    if None in [search_type, search_id]:    
        error_message = 'Error: missing search type or target id, use --help for help.\n'
        sys.stdout.write(error_message)
        exit()
    
    if verbosity:
        sys.stdout.write('starting...\n')

    t_start = datetime.now()
    scores = fetchAllScores(search_type=search_type, 
                            search_id=search_id, 
                            size=size, 
                            async=async, 
                            verbose=verbosity)
    t_end = datetime.now()
    sys.stdout.write('All scores fetched in {0} seconds.\n'.format(t_end - t_start))
    max_v, min_v, mean, std_dev = computeResults(scores, use_sample_std_dev=not population_std_dev)
    sys.stdout.write('Results are:\n')
    sys.stdout.write('           maximum: {0}\n'.format(max_v))
    sys.stdout.write('           minimum: {0}\n'.format(min_v))
    sys.stdout.write('           average: {0}\n'.format(mean))
    sys.stdout.write('standard deviation: {0}\n'.format(std_dev))
        
    

    







    