from __future__ import print_function
from pyspark import SparkContext
import sys
from csv import reader
import datetime

def validity(x):

	if x is "" or x is " ":
		return "","NULL","OTHER","NULL"
	else:
        y = x
        x = x.split("/")
        try:
            month = int(x[0])
            day = int(x[1])
            year = int(x[2])
            if year>= 2006 and year<= 2017:
                try:
                    date = datetime.datetime(year, month, day)
                    return y, "DATE", "DATE_OF_COMPLAINT", "VALID"
                except:
                    return y, "DATE", "DATE_OF_COMPLAINT", "INVALID"
            else:
                return y, "DATE", "DATE_OF_COMPLAINT", "INVALID"
        except:
            return y, "DATE", "DATE_OF_COMPLAINT", "INVALID"
            


if __name__ == "__main__":

    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    header = lines.take(1) #extract header
    lines = lines.filter(lambda x : x!= header)
    lines = lines.mapPartitions(lambda x: reader(x))
    catg = lines.map(lambda x: '%s\t%s\t%s\t%s'%(validity(x[1])))
    count = lines.map(lambda x: ('%s\t'%validity(x[1])[3],1)).reduceByKey(lambda x,y: x+y)
    catg.saveAsTextFile('CMPLNT_FR_DT.out')
    count.saveAsTextFile('CMPLNT_FR_DT_COUNT.out')

    sc.stop()
