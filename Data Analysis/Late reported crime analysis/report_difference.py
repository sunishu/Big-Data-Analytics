from __future__ import print_function
from pyspark import SparkContext
import sys
from operator import add
from csv import reader

def toCSVLine(data):
    return ','.join(str(d) for d in data)


if __name__ == '__main__':
	
	sc = SparkContext()
	lines = sc.textFile(sys.argv[1], 1)

	lines = lines.mapPartitions(lambda x: reader(x))
	header = lines.first() #extract header
    lines = lines.filter(lambda x : x!= header)

    # difference in reporting date and the date on which the crime was reported
    diff_a = lines.map(lambda x: (x[1], x[5], x[7], x[11], x[13]))
    diff = diff_a.map(lambda x: (x[0], x[1], x[2], x[3], x[4]))
    count_diff = diff.map(lambda x: (x,1)).reduceByKey(add).replace(',', '').sortByKey().collect()
    count_diff = sc.parallelize(count_diff)
    count_diff = count_diff.map(lambda x: (str(x).replace('(',' ').replace('"',"'").replace(')',' ')))
    final_diff = count_diff.map(toCSVLine)
    final_diff = final_diff.map(lambda x: (str(x).replace('(','').replace(')','').replace(',','').replace("' ",',').replace("'", '')))
    final_diff.saveAsTextFile('difference.csv')

    sc.stop()
