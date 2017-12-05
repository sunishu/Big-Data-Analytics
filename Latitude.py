from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import math
import sys

def range(latitude):

        if(latitude>=40.477399 and latitude<=40.917577):
                return True
        else:
                return False
def  validity(x):

        if x=="" or x==" " or x=="\t":
                return "","NULL","OTHER","NULL"
        try:
                x=float(x)
                if range(x):
                        return x,"INT","LATITUDE","VALID"
                else:
                        return x,"INT","LATITUDE","INVALID"
        except:
                return x,"INT","LATITUDE","INVALID"

if __name__ == "__main__":

    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.take(1) #extract header
    lines = lines.filter(lambda x : x!= header)
    lines = lines.mapPartitions(lambda x: reader(x))
    catg = lines.map(lambda x: '%s\t%s\t%s\t%s\t'%(validity(x[21])))
    count = lines.map(lambda x: ('%s\t'%validity(x[21])[3],1)).reduceByKey(lambda x,y: x+y)
    catg.saveAsTextFile('Latitude.out')
    count.saveAsTextFile('Latitude_COUNT.out')

    sc.stop()