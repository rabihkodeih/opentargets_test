# Opentargets Coding Test

## Problem A
Requirements:

1. [Python 3.6](https://www.python.org/downloads/release/python-364/)
2. [requests](http://docs.python-requests.org/en/master/)
3. [numpy](http://www.numpy.org)
4. [aiohttp](https://aiohttp.readthedocs.io/en/stable/)


## Usage

From the source folder, use:

`python problem_a.py [-t|-d] <search_id> -s <size>`

Possible switches and options are:
```
Target/Disease overall score comutation command, supported arguments are:
-t <target_id>     : to specify a target followed by a arget id
-d <disease_id>    : to specify a disease followed by a disease id
-s <size>          : to specify a size (default of 10)
--sync             : use synchronous fetching of data (default is asynchronous data fetching)
-p                 : to use population standard deviation (default is using sample standard deviation)
--test             : to run test cases
--help             : displays this help message
```

Using a size of 20 gave good performance for most queries.

## Note on Testcase 3

When runnign the test cases, the third test case fails with the following error message:

> {"message": "(size + from) cannot be bigger than 10000 use search_after...}

Unfortunately it was not specified nor was it clear how to invoke pagination > 10000 resuls using 
the "search_after" option. A thorough search in opentargets documentation and blog yielded no results.


## Problem B Part 1
Requirements:

1. [Python 3.6](https://www.python.org/downloads/release/python-364/)
2. [ujson](https://pypi.python.org/pypi/ujson)

## Elaboration

This part of the problem was fairly straightforward. The code ran in under 4 minutes:

```
Operation completed in 0:03:34.487030 seconds
Operation completed in 0:00:12.608518 seconds
```

and produces the required csv file in tabular form as specified. It was assumed prior to running the program that the input data file was unzipped.

The main data structure used in the solution is a dict of dict of lists. The datastructure maps a **(target_id, disease_id)** pair to a list of association scores. This part ran in the function `handleDataMulti` which used the multiprocessing module for a saving of approx 40% in running time. A single process function `handleDataSingle` which does the same job is provided for convenience.

After that the generation of the csv file took place in the function `outputStageProcessing` which was fairly quick so there was no need for multiprocessing.


## Problem B Part 2
Requirements:

1. [Python 3.6](https://www.python.org/downloads/release/python-364/)


## Elaboration

This part of the problem was a bit more challenging as we had to sift through approx 550 million potential pairs of target_ids. The main data structures held the values of the target_ids in dict mapping each target_id to a list of related disseases. This was performed in function `getDataMap`. Then a list of potential pairs was generated and the relevant pairs were counted in function `countSingle`. This was the produced result:

```
Operation completed in 0:00:02.551176 seconds
Operation completed in 0:43:12.404540 seconds
121114622
```

The whole operation took slightly less than 45 mins with 121114622 pairs sharing two or more diseases.

## Discussion About Execution Speed

Unfortunately, we couldn't make things faster using multiprocessing. Two attempts were made in functions `countMulti` and `countMulti2` but the overhead of passing data to the processess proved to be too much for any potential gain in speed. We even tried to generated separated input files from the main data structure in order to avoid the interprocess communication bottleneck, but that in itself was extremely slow (~550 million pairs) and needs multliprocessing for any hoped speedup. The best solution for these types of problems is to create a multitide of job input files on seprate machines (ex: using a cluster) or a single machine with multiple processes.


## Problem B Part 2 Parallel
Requirements:

1. [Python 3.6](https://www.python.org/downloads/release/python-364/)


## Elaboration

The main idea here is to segregate the parralellized brute force double loop in the worker processes instead of running it in the master process. This necessitates the partitionning of two-combination list over the number of jobs. For example, if we had a list of 5 targers we would have the following pairs to check:

```
t1 t2
t1 t3
t1 t4
t1 t5
t2 t3
t2 t4
t2 t5
t3 t4
t3 t5
t4 t5
```

Instead of generating these pairs in the master process, we partition the list of pairs into two parts as follows:

```
0: t1 t2
1: t1 t3
2: t1 t4
3: t1 t5
4: t2 t3
--
5: t2 t4
6: t2 t5
7: t3 t4
8: t3 t5
9: t4 t5
```

and have a number of processes (2 in this case) handle each part in parallel (by the multiprocessing.pool.map mechanism). In this example, the actual job list to be mapped in the pool would look like this:

```
jobs =[(0, 4), (5, 9)]
```

which is a list of the indecies of the chunked list of pairs. It would then be the responsibility of the workers to generate the list of pairs in each pair (and do the actual computation). In addition to a number of optimizations, this resulted in a great reduction in execution time from 44 minutes to approx. 6 minutes and 40 seconds:

```
testCombinationEnumeration started...
testCombinationEnumeration completed successfully
Operation completed in 0:00:00.526849 seconds
Operation completed in 0:00:02.632012 seconds
----------
Generated 6 jobs with the following chunks:
1: (0, 91347730)
2: (91347731, 182695461)
3: (182695462, 274043192)
4: (274043193, 365390923)
5: (365390924, 456738654)
6: (456738655, 548086385)
----------
Operation completed in 0:06:37.995477 seconds
number of pairs: 121114622
```

The logic involved in generating the combination starting from a certain index is quite involved and was implemented in the functions: `nCr`, `binarySearch`, `binomialDecomposition`, `find2CombinationFromIndex`, `partition2Combinations`, and `generatePairs`. Further details can be found [here](http://vlkan.com/blog/post/2013/12/04/combinations/").

 
## Problem B Part 2 Projection
Requirements:

1. [Python 3.6](https://www.python.org/downloads/release/python-364/)

## Elaboration

Here we improved upon the results of the last program. The main idea is to itertate through a subset of the pairs instead of all potential pairs. This ideas is borrowed from the field of bipartite graph projection algorithms. Thus for each target, we iterate through its directly related diseases and then through their related targets as well. By that we construct the set of all meaningful target-target pairs that have two or more diseases in common. 

We use two main data structures `TARGET_NODES` and `DISEASE_NODES`. These are simply dicsts mapping targets to their related diseases (or diseases to their related targets in the second dict). The two dicsts are constructed in `updateNodeLists`.

From these dicsts, we can then perform the projection logic as implemented in function `countWorker` which takes a chunk of the total computation to be performed. The chunks are simply generated by partitioning the targets list (function `getJobs`). Finally function `countPairs` generates the job list, creates a pool object and maps the workers to the jobs.

The total exucution time for this implementation was just under three minutes:
```
Operation completed in 0:00:03.158954 seconds
----------
Generated 40 jobs with the following chunks:
1: (0, 826)
2: (827, 1653)
3: (1654, 2480)
...
40: (32253, 33108)
----------
Operation completed in 0:02:52.945630 seconds
number of pairs: 121114622
```  

 