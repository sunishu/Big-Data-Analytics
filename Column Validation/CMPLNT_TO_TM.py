from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import time
import re
import string

def validity(x):
    if x is "" or x is " ":
        return "","NULL","OTHER","NULL"
    try:
        y=x
        if time.strptime(x,"%H:%M:%S"):
            mat=re.match('([01][0-9]|2[0-3]|0?[1-9]):([0-5][0-9]|0?[1-9]):([0-5][0-9]|0?[1-9])$', x)
            if mat:
                return y,"TIME","END_OF_EVENT","VALID"
            else:
                raise ValueError
    except ValueError:
        return y,"TIME","END_OF_EVENT","INVALID"


if __name__ == "__main__":

    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.take(1) #extract header
    lines = lines.filter(lambda x : x!= header)
    lines = lines.mapPartitions(lambda x: reader(x))
    catg = lines.map(lambda x: '%s\t%s\t%s\t%s'%(validity(x[4])))
    count = lines.map(lambda x: ('%s\t'%validity(x[4])[3],1)).reduceByKey(lambda x,y: x+y)
    catg.saveAsTextFile('CMPLNT_TO_TM.out')
    count.saveAsTextFile('CMPLNT_TO_TM_COUNT.out')

    sc.stop()