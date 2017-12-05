# **Big-Data-Analytics** 

>## **Team Members:-**
  *Sunidhi Shukla(ss10448)
   Swati Bhatt(sb5984)*

>## **Dataset:**
The dataset can be found at this link: https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i

**About the dataset:** 
-This dataset includes all valid felony, misdemeanor, and violation crimes reported to the New York City Police Department (NYPD) from 2006 to the end of last year (2016). 

-It contains 5.58M rows and 24 columns. (Futher information about each column can be found in the project report.)

-The data is categorised under 24 heads which has information such as the date and the time of the crime, the location where the crime was committed and the type of the crime, the latitude and longitude of the crime location.

-The type of the data in each column is from amongst these data types- date and time, text, number and location.

>This project is divided into two sections:
Part 1: Data cleaning
Part 2: Data analysis and visualisation.

## **Data Cleaning:**
Instructions:
1- Login to dumbo using instructions given at http://wikis.nyu.edu/display/NYUHPC.

2- Setup the aliases:
  alias hfs='/usr/bin/hadoop fs ' export HAS=/opt/cloudera/parcels/CDH-5.9.0-1.cdh5.9.0.p0.23/lib export HSJ=hadoop-mapreduce/hadoop-   streaming.jar  alias hjs='/usr/bin/hadoop jar $HAS/$HSJ'
  
3- Upload the dataset to Dumbo:
   On MacOS- open terminal and run : scp data.csv your_netid@dumbo.es.its.nyu.edu:/home/your_netid/
   On Windows, run cmd.exe and run : pscp data.csv your_netid@dumbo.es.its.nyu.edu:/home/your_netid/
   
4- Upload the the file to Hadoop cluster using: hadoop fs -copyFromLocal data.csv

The dataset is already on the cluster under /home/ss10448/FP and the column validation files are in /home/ss10448/FP/ColumnValidation .

To run a job on Hadoop use following command-
spark-submit <python-code>.py <dataset>.csv

To get the ouput file, use:
hadoop fs -getmerged <file-name>.out <file-name>.csv

For each column validation file, we get two output files- the first file returns the data along with its type, mini description and tells if the data is valid/invalid/null.
The second file returns the total count of valid/invalid/null rows in each column.

The link to the project report is- https://docs.google.com/document/d/18-51SWcI-9JDbDGjkIF6H3jYnUyA2YTcvHv4K7EW0WI/edit?ts=5a25f4f3 

