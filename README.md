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

The main data structure used in the solution is a dict of dict of lists. The datastructure maps a **(target_id, disease_id)** pair to a list of association scores. This part ran in the function `handle_main_multi` which used the multiprocessing module for a saving of approx 40% in running time. A single process function `handle_main_single` which does the same job is provided for convenience.

After that the generation of the csv file took place in the function `output_stage_processing` which was fairly quick so there was no need for multiprocessing.


## Problem B Part 2
Requirements:

1. [Python 3.6](https://www.python.org/downloads/release/python-364/)


## Elaboration

This part of the problem was a bit more challenging as we had to sift through approx 550 million potential pairs of target_ids. The main data structures held the values of the target_ids in dict mapping each target_id to a list of related disseases. This was performed in function `get_data_map`. Then a list of potential pairs was generated and the relevant pairs were counted in function `count_single`. This was the produced result:

```
Operation completed in 0:00:02.551176 seconds
Operation completed in 0:43:12.404540 seconds
121114622
```

The whole operation took slightly less than 45 mins with 121114622 pairs sharing two or more diseases.

## Discussion About Execution Speed

Unfortunately, we couldn't make things faster using multiprocessing. Two attempts were made in functions `count_multi` and `count_multi2` but the overhead of passing data to the processess proved to be too much for any potential gain in speed. We even tried to generated separated input files from the main data structure in order to avoid the interprocess communication bottleneck, but that in itself was extremely slow (~550 million pairs) and needs multliprocessing for any hoped speedup. The best solution for these types of problems is to create a multitide of job input files on seprate machines (ex: using a cluster) or a single machine with multiple processes.
