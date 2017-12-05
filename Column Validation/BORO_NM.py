from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import re
def validity(x):
	if x is "" or x is " ":
		return "","NULL","OTHER","NULL"
	else :
		boroughs = ["BRONX","BROOKLYN","MANHATTAN","QUEENS","STATEN ISLAND"]
        if x not in boroughs:
            return x,"STRING","BOROUGH","INVALID"
        else :
            return x,"STRING","BOROUGH","VALID" 

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.take(1) #extract header
    lines = lines.filter(lambda x : x!= header)
    lines = lines.mapPartitions(lambda x: reader(x))
    catg = lines.map(lambda x: '%s\t%s\t%s\t%s'%(validity(x[13])))
    count = lines.map(lambda x: ('%s '%validity(x[13])[3],1)).reduceByKey(lambda x,y: x+y)
    catg.saveAsTextFile('BORO_NM.out')
    count.saveAsTextFile('BORO_NM_COUNT.out')

    sc.stop()