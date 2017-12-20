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

    # for all boroughs
    overall_a = lines.map(lambda x: (x[1], x[2], x[5], x[7], x[9], x[10], x[11], x[12], x[13]))
    overall = overall_a.map(lambda x: (x[7]))
    count_overall = overall.map(lambda x: (x,1)).reduceByKey(add).sortByKey().collect()
    count_overall = sc.parallelize(count_overall)
    count_overall = count_overall.map(lambda x: (str(x).replace('(',' ').replace('"',' ').replace(')',' ')))
    final_overall = count_overall.map(toCSVLine)
    final_overall = final_overall.map(lambda x: (str(x).replace('(','').replace(')','').replace(',','').replace("' ",','),replace("'", '')))
    final_overall.saveAsTextFile('overall_Category.csv')

    # for Bronx
    bronx_a = lines.filter(lambda x: x[13] == 'BRONX')
    bronx_b = bronx_a.map(lambda x: (x[1], x[2], x[5], x[7], x[9], x[10], x[11], x[12], x[13]))
    bronx = bronx_b.map(lambda x: (x[7]))
    count_bronx = bronx.map(lambda x: (x,1)).reduceByKey(add).sortByKey().collect()
    count_bronx = sc.parallelize(count_bronx)
    count_bronx = count_bronx.map(lambda x: (str(x).replace('(',' ').replace('"',' ').replace(')',' ')))
    final_bronx = count_bronx.map(toCSVLine)
    final_bronx = final_bronx.map(lambda x: (str(x).replace('(','').replace(')','').replace(',','').replace("' ",','),replace("'", '')))
    final_bronx.saveAsTextFile('bronx_Category.csv')


    # for brooklyn
    brooklyn_a = lines.filter(lambda x: x[13] == 'BROOKLYN')
    brooklyn_b = brooklyn_a.map(lambda x: (x[1], x[2], x[5], x[7], x[9], x[10], x[11], x[12], x[13]))
    brooklyn = brooklyn_b.map(lambda x: (x[7]))
    count_brooklyn = brooklyn.map(lambda x: (x,1)).reduceByKey(add).sortByKey().collect()
    count_brooklyn = sc.parallelize(count_brooklyn)
    count_brooklyn = count_brooklyn.map(lambda x: (str(x).replace('(',' ').replace('"',' ').replace(')',' ')))
    final_brooklyn = count_brooklyn.map(toCSVLine)
    final_brooklyn = final_brooklyn.map(lambda x: (str(x).replace('(','').replace(')','').replace(',','').replace("' ",','),replace("'", '')))
    final_brooklyn.saveAsTextFile('brooklyn_Category.csv')


    # for manhattan
    manhattan_a = lines.filter(lambda x: x[13] == 'MANHATTAN')
    manhattan_b = manhattan_a.map(lambda x: (x[1], x[2], x[5], x[7], x[9], x[10], x[11], x[12], x[13]))
    manhattan = manhattan_b.map(lambda x: (x[7]))
    count_manhattan = manhattan.map(lambda x: (x,1)).reduceByKey(add).sortByKey().collect()
    count_manhattan = sc.parallelize(count_manhattan)
    count_manhattan = count_manhattan.map(lambda x: (str(x).replace('(',' ').replace('"',' ').replace(')',' ')))
    final_manhattan = count_manhattan.map(toCSVLine)
    final_manhattan = final_manhattan.map(lambda x: (str(x).replace('(','').replace(')','').replace(',','').replace("' ",','),replace("'", '')))
    final_manhattan.saveAsTextFile('manhattan_Category.csv')


    # for queens
    queens_a = lines.filter(lambda x: x[13] == 'QUEENS')
    queens_b = queens_a.map(lambda x: (x[1], x[2], x[5], x[7], x[9], x[10], x[11], x[12], x[13]))
    queens = queens_b.map(lambda x: (x[7])) 
    count_queens = queens.map(lambda x: (x,1)).reduceByKey(add).sortByKey().collect()
    count_queens = sc.parallelize(count_queens)
    count_queens = count_queens.map(lambda x: (str(x).replace('(',' ').replace('"',' ').replace(')',' ')))
    final_queens = count_queens.map(toCSVLine)
    final_queens = final_queens.map(lambda x: (str(x).replace('(','').replace(')','').replace(',','').replace("' ",','),replace("'", '')))
    final_queens.saveAsTextFile('queens_Category.csv')


    # for staten island
    staten_a = lines.filter(lambda x: x[13] == 'STATEN ISLAND')
    staten_b = staten_a.map(lambda x: (x[1], x[2], x[5], x[7], x[9], x[10], x[11], x[12], x[13]))
    staten = staten_b.map(lambda x: (x[7])) 
    count_staten = staten.map(lambda x: (x,1)).reduceByKey(add).sortByKey().collect()
    count_staten = sc.parallelize(count_staten)
    count_staten = count_staten.map(lambda x: (str(x).replace('(',' ').replace('"',' ').replace(')',' ')))
    final_staten = count_staten.map(toCSVLine)
    final_staten = final_staten.map(lambda x: (str(x).replace('(','').replace(')','').replace(',','').replace("' ",','),replace("'", '')))
    final_staten.saveAsTextFile('staten_Category.csv')


    sc.stop()
