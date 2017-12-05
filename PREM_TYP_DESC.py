from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import re
def validity(x):
	if x is "" or x is " ":
		return "","NULL","OTHER","NULL"
	else :
		return x,"STRING","PREMISES_DESCRIPTION","VALID"

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.take(1) #extract header
    lines = lines.filter(lambda x : x!= header)
    lines = lines.mapPartitions(lambda x: reader(x))
    catg = lines.map(lambda x: '%s\t%s\t%s\t%s'%(validity(x[16])))
    count = lines.map(lambda x: ('%s '%validity(x[16])[3],1)).reduceByKey(lambda x,y: x+y)
    catg.saveAsTextFile('PREM_TYP_DESC.out')
    count.saveAsTextFile('PREM_TYP_DESC_COUNT.out')

    sc.stop()