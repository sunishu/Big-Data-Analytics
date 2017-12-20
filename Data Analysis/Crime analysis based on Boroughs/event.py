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

    # Independence Day
    event_a = lines.map(lambda x: (x[1], x[2], x[5], x[7], x[9], x[10], x[11], x[12], x[13], x[16], x[21], x[22]))
    event = event_a.map(lambda x: (x[0].split('/')[0], x[0].split('/')[1], x[0].split('/')[2], x[3], x[4], x[8], x[10], x[11]))
    count_event = event.map(lambda x: (x,1)).reduceByKey(add).sortByKey().collect()
    count_event = sc.parallelize(count_event)
    count_event = count_event.map(lambda x: (str(x).replace('(',' ').replace('"',"'").replace(')',' ')))
    final_event = count_event.map(toCSVLine)
    final_event = final_event.map(lambda x: (str(x).replace('(','').replace(')','').replace(',','').replace("' ",',').replace("'", '')))
    final_event.saveAsTextFile('event.csv')


    sc.stop()