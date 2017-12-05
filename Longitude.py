from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import math
import sys

def range(longitude):

        if(longitude>=-74.25909 and longitude<=-73.700009):
                return True
        else:
                return False
def  validity(x):

        if x=="" or x==" " or x=="\t":
                return "","NULL","OTHER","NULL"
        try:
                x=float(x)
                if range(x):
                        return x,"INT","LONGITUDE","VALID"
                else:
                        return x,"INT","LONGITUDE","INVALID"
        except:
                return x,"INT","LONGITUDE","INVALID"

if __name__ == "__main__":

    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.take(1) #extract header
    lines = lines.filter(lambda x : x!= header)
    lines = lines.mapPartitions(lambda x: reader(x))
    catg = lines.map(lambda x: '%s\t%s\t%s\t%s\t'%(validity(x[22])))
    count = lines.map(lambda x: ('%s\t'%validity(x[22])[3],1)).reduceByKey(lambda x,y: x+y)
    catg.saveAsTextFile('LONGITUDE.out')
    count.saveAsTextFile('LONGITUDE_COUNT.out')

    sc.stop()