from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import re
def validity(x):
	if x is "" or x is " ":
		return "","NULL","OTHER","NULL"
	else :
        location = ["INSIDE", "OUTSIDE", "FRONT OF", "OPPOSITE OF", "REAR OF"]
        if x not in location:
            return x,"STRING","LOCATION_OF_OCCURENCE","INVALID"
        else:
            return x,"STRING","LOCATION_OF_OCCURENCE","VALID"

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.take(1) #extract header
    lines = lines.filter(lambda x : x!= header)
    lines = lines.mapPartitions(lambda x: reader(x))
    catg = lines.map(lambda x: '%s\t%s\t%s\t%s'%(validity(x[15])))
    count = lines.map(lambda x: ('%s '%validity(x[15])[3],1)).reduceByKey(lambda x,y: x+y)
    catg.saveAsTextFile('LOC_OF_OCCUR_DESC.out')
    count.saveAsTextFile('LOC_OF_OCCUR_DESC_COUNT.out')

    sc.stop()