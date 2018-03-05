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

