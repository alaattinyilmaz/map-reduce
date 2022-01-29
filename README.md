# map-reduce

MapReduce is a programming framework that allows us to perform distributed and parallel processing on large data sets in a distributed environment. 

# run

The first parameter is either COUNT or CYCLE which switches between
FindCitations and FindCyclicReferences.

The second parameter is the number of workers. You should support as
many as 10 of them.

The third parameter is the name of the file that you will process.

In the following, we have two example invocations from the command line:

    python main.py COUNT 4 test01.txt
    python main.py CYCLE 3 test02.txt
