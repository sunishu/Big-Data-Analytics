from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import math
import sys

def  validity(x):
        if x=="" or x==" " or x=="\t":
                return "","NULL","OTHER","NULL"
        try:
                x=float(x)
                if  x>=117500 and x<=275000:
                        return x,"INT","Y-COORDINATE","VALID"
                else:
                    return x,"INT","Y-COORDINATE","INVALID"
        except:
                return x,"INT","Y-COORDINATE","INVALID"

if __name__ == "__main__":

    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.take(1) #extract header
    lines = lines.filter(lambda x : x!= header)
    lines = lines.mapPartitions(lambda x: reader(x))
    catg = lines.map(lambda x: '%s\t%s\t%s\t%s'%(validity(x[20])))
    count = lines.map(lambda x: ('%s\t'%validity(x[20])[3],1)).reduceByKey(lambda x,y: x+y)
    catg.saveAsTextFile('Y.out')
    count.saveAsTextFile('Y_COUNT.out')

    sc.stop()