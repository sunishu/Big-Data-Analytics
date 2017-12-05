from __future__ import print_function
from pyspark import SparkContext
from csv import reader
import sys
import re

def validate_complaint_number(x):
	if x is "" or x is " ":
		return False
	elif re.match('[1-9][0-9]+$',x):
		return True
	else :
		return False

def validate_date(x):
	if x is "" or x is " ":
		return False
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
	                    return True
	                except:
	                    return False
	            else:
	                return False
	        except:
	            return False

def validate_time(x):
	if x is "" or x is " ":
        	return False
    else:

		    try:
			        y=x
			        if time.strptime(x,"%H:%M:%S"):
			            mat=re.match('([01][0-9]|2[0-3]|0?[1-9]):([0-5][0-9]|0?[1-9]):([0-5][0-9]|0?[1-9])$', x)
			            if mat:
			                return True
			            else:
			                raise ValueError
			except ValueError:
		        return False



def validate_key_codes(x):
	if x is "" or x is " ":
		return False
	elif re.match('[1-9][0-9][0-9]$',x):
		return True
	else :
		return False

def validate_desc(x):
	if x is "" or x is " ":
		return False
	else :
		return True

def validate_crime_indicator(x):
	if x is "" or x is " ":
		return False
	else :
		indicator = ["SUCCESSFULLY COMPLETED", "ATTEMPTED", "FAILED", "INTERRUPTED PREMATURELY"]
        if x not in indicator:
            return False
        else:
            return True

def validate_offense(x):
	if x is "" or x is " ":
		return False
	else :
        offense = ["FELONY", "MISDEMEANOR", "VIOLATION"]
        if x not in offense:
            return False
        else:
            return True

def validate_borough(x):
	if x is "" or x is " ":
		return False
	else :
		boroughs = ["BRONX","BROOKLYN","MANHATTAN","QUEENS","STATEN ISLAND"]
        if x not in boroughs:
            return False
        else :
            return True


def validate_precinct(x):
	if x is "" or x is " ":
		return False
	elif re.match('[0-9]+',x):
		return True
	else :
		return False

def validate_location(x):
	if x is "" or x is " ":
		return False
	else :
        location = ["INSIDE", "OUTSIDE", "FRONT OF", "OPPOSITE OF", "REAR OF"]
        if x not in location:
            return False
        else:
            return True


def validate_x_coor(x):
	if x=="" or x==" " or x=="\t":
                return False
        try:
                x=float(x)
                if  x>=909900 and x<=1067600:
                        return True
                else:
                	return False
        except:
                return False

def validate_y_coor(x):
	if x=="" or x==" " or x=="\t":
                return False
        try:
                x=float(x)
                if x>=117500 and x<=275000:
                        return True
                else:
                	return False
        except:
                return False


def range_lat(latitude):

        if(latitude>=40.477399 and latitude<=40.917577):
                return True
        else:
                return False

def  validate_lat(x):

        if x=="" or x==" " or x=="\t":
                return False
        try:
                x=float(x)
                if range_lat(x):
                        return True
                else:
                        return False
        except:
                return False

def range_lon(longitude):

        if(longitude>=-74.25909 and longitude<=-73.700009):
                return True
        else:
                return False

def  validate_lon(x):

        if x=="" or x==" " or x=="\t":
                return False
        try:
                x=float(x)
                if range_lon(x):
                        return True
                else:
                        return False
        except:
                return False               

def range(latitude,longitude):
        if(latitude>=40.477399 and latitude<=40.917577):
                return True
        else:
                return False
        if(longitude>=-74.25909 and longitude<=-73.700009):
                return True
        else:
                return False

def validate_latlon(latitude,longitude):
	if x=="" or x==" " or x=="\t":
                return False
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
                        return True
                else:
                        return False
        except:
                return False




def check(x):
	if validate_complaint_number(x[0]) and validate_date(x[1]) and validate_time(x[2]) and validate_date(x[3]) and validate_time(x[4]) and validate_date(x[5]) and validate_key_codes(x[6]) and validate_desc(x[7]) and validate_key_codes(x[8]) and validate_desc(x[9]) and validate_crime_indicator(x[10]) and validate_offense(x[11]) and validate_desc(x[12]) and validate_borough(x[13]) and validate_precinct(x[14]) and validate_location(x[15]) and validate_desc(x[16]) and validate_desc(x[17]) and validate_desc(x[18]) and validate_x_coor(x[19]) and validate_y_coor(x[20]) and validate_lat(x[21]) and validate_lon(x[22]) and validate_latlon(x[23]):
		return True
	else:
		return False

def toCSVFile(line):
	for i in range(len(line)):
        if "," in str(line[i]) :
            line[i]='"'+line[i]+'"'
    return ','.join(x for x in line)


if __name__ == '__main__':
	sc = SparkContext()
	lines = sc.textFile(sys.argv[1], 1)
    header = lines.take(1) #extract header
    lines = lines.filter(lambda x : x!= header)
    lines = lines.mapPartitions(lambda x: reader(x)).map(lambda x: (x,check(x))).filter(lambda x: x[1]==True).map(lambda x: toCSVFile(x[0]))

    lines.saveAsTextFile('clean.csv')

    sc.stop()