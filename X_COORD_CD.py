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
                if  x>=909900 and x<=1067600:
                        return x,"INT","X-COORDINATE","VALID"
                else:
                    return x,"INT","X-COORDINATE","INVALID"
        except:
                return x,"INT","X-COORDINATE","INVALID"

if __name__ == "__main__":

    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.take(1) #extract header
    lines = lines.filter(lambda x : x!= header)
    lines = lines.mapPartitions(lambda x: reader(x))
    catg = lines.map(lambda x: '%s\t%s\t%s\t%s'%(validity(x[19])))
    count = lines.map(lambda x: ('%s\t'%validity(x[19])[3],1)).reduceByKey(lambda x,y: x+y)
    catg.saveAsTextFile('X3.out')
    count.saveAsTextFile('X_COUNT3.out')

    sc.stop()