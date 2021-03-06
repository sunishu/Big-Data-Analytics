from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import re
def validity(x):
	if x is "" or x is " ":
		return "","NULL","OTHER","NULL"
	else :
		indicator = ["SUCCESSFULLY COMPLETED", "ATTEMPTED", "FAILED", "INTERRUPTED PREMATURELY"]
        if x not in indicator:
            return x,"STRING","INDICATOR_OF_CRIME","INVALID"
        else:
            return x,"STRING","INDICATOR_OF_CRIME","VALID" 

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.take(1) #extract header
    lines = lines.filter(lambda x : x!= header)
    lines = lines.mapPartitions(lambda x: reader(x))
    catg = lines.map(lambda x: '%s\t%s\t%s\t%s'%(validity(x[10])))
    count = lines.map(lambda x: ('%s '%validity(x[10])[3],1)).reduceByKey(lambda x,y: x+y)
    catg.saveAsTextFile('CRM_ATPT_CPTD_CD.out')
    count.saveAsTextFile('CRM_ATPT_CPTD_CD_COUNT.out')

    sc.stop()