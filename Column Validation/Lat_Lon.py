from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import math
import sys

def range(latitude,longitude):
        if(latitude>=40.477399 and latitude<=40.917577):
                return True
        else:
                return False
        if(longitude>=-74.25909 and longitude<=-73.700009):
                return True
        else:
                return False


def  validity(x):
        if x=="" or x==" " or x=="\t":
                return "","NULL","OTHER","NULL"
        try:
                x=x.strip()
                x=x.replace("(","")
                x=x.replace(")","")
                latitude,longitude=x.split(",")
                latitude=latitude.strip()
                longitude=longitude.strip()
                latitude=float(latitude)
                longitude=float(longitude)
                if range(latitude,longitude):
                        return x,"Text/Int","Location","VALID"
                else:
                        return x,"Text/Int","Location","INVALID"
        except:
                return x,"Text/Int","Location","INVALID"

if __name__ == "__main__":

    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.take(1) #extract header
    lines = lines.filter(lambda x : x!= header)
    lines = lines.mapPartitions(lambda x: reader(x))
    catg = lines.map(lambda x: '%s\t%s\t%s\t%s\t'%(validity(x[23])))
    count = lines.map(lambda x: ('%s\t'%validity(x[23])[3],1)).reduceByKey(lambda x,y: x+y)

    catg.saveAsTextFile('Lat_Lon.out')
    count.saveAsTextFile('Lat_Lon_COUNT.out')

    sc.stop()