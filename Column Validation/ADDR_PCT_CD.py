from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import re

def validity(x):
	if x is "" or x is " ":
		return "","NULL","OTHER","NULL"
	elif re.match('[0-9]+',x):
		return x,"INT","PRECINCT","VALID"
	else :
		return x,"INT","PRECINCT","INVALID"

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.take(1) #extract header
    lines = lines.filter(lambda x : x!= header)
    lines = lines.mapPartitions(lambda x: reader(x))
    catg = lines.map(lambda x: '%s\t%s\t%s\t%s'%(validity(x[14])))
    count = lines.map(lambda x: ('%s '%validity(x[14])[3],1)).reduceByKey(lambda x,y: x+y)
    catg.saveAsTextFile('ADDR_PCT_CD.out')
    count.saveAsTextFile('ADDR_PCT_CD_COUNT.out')

    sc.stop()