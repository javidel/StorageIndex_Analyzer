from pyspark import SparkContext

from pyspark.streaming import StreamingContext
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import DataFrameWriter
from pyspark import StorageLevel
import sys

from pyspark.sql.functions import monotonically_increasing_id

def minmaxBigInt(iterator):
    
    firsttime = 0
    min = 0;
    max = 0;
    for x in iterator:
      if(x!= '' and x!='NULL' and x is not None):
	y=long(x)
	print y
        if (firsttime == 0):
            min = y;
            max = y;
            firsttime = 1
        else:
            if y > max:
                max = y
            if y < min:
                min = y

    return (min, max)

def minmaxDouble(iterator):

    firsttime = 0
    min = 0;
    max = 0;
    for x in iterator:
     if(x!= '' and x!='NULL' and x is not None):
	y=float(x)
	print y
        if (firsttime == 0):
            min = y;
            max = y;
            firsttime = 1
        else:
            if y > max:
                max = y
            if y < min:
                min = y

    return (min, max)

def minmaxStr(iterator):


    return (0, 0)

def minmaxInt(iterator):
    firsttime = 0
    min = 0
    max = 0
    for x in iterator:
	if(x!= '' and x!='NULL' and x is not None):	
		y=int(x)	
        	if (firsttime == 0):
            		min = y;
            		max = y;
            		firsttime = 1
        	else:
            		if y > max:
                		max = y
            		if y < min:
                		min = y
    return (min, max)

def createTable(result,n_cols,sqlContext):

	result=result.persist(StorageLevel.MEMORY_AND_DISK)
	if(n_cols==2):
		rows= result.map(lambda (a,b): Row(column1=a,column2=b))
		dataFrame = sqlContext.createDataFrame(rows)

	if(n_cols==3):
		rows= result.map(lambda a: Row(column1=a[0][0],column2=a[0][1],column3=a[1]))
		dataFrame = sqlContext.createDataFrame(rows)

	if(n_cols==4):
		rows=result.map(lambda a: Row(column1=a[0][0][0],column2=a[0][0][1],column3=a[0][1],column4=a[1]))
		dataFrame = sqlContext.createDataFrame(rows)

	if(n_cols==5):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0],column2=a[0][0][0][1],column3=a[0][0][1],column4=a[0][1],column5=a[1]))
		dataFrame = sqlContext.createDataFrame(rows)

	if(n_cols==6):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0],column2=a[0][0][0][0][1],column3=a[0][0][0][1],column4=a[0][0][1],column5=a[0][1],column6=a[1]))
		dataFrame = sqlContext.createDataFrame(rows)

	if(n_cols==7):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0],column2=a[0][0][0][0][0][1],column3=a[0][0][0][0][1],column4=a[0][0][0][1],column5=a[0][0][1],column6=a[0][1],column7=a[1]))
		dataFrame = sqlContext.createDataFrame(rows)

	if(n_cols==8):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][1],column3=a[0][0][0][0][0][1],column4=a[0][0][0][0][1],column5=a[0][0][0][1],column6=a[0][0][1],column7=a[0][1],column8=a[1]))
		dataFrame = sqlContext.createDataFrame(rows)
	if(n_cols==9):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][1],column4=a[0][0][0][0][0][1],column5=a[0][0][0][0][1],column6=a[0][0][0][1],column7=a[0][0][1],column8=a[0][1],column9=a[1]))
		dataFrame = sqlContext.createDataFrame(rows)

	if(n_cols==10):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][1],column5=a[0][0][0][0][0][1],column6=a[0][0][0][0][1],column7=a[0][0][0][1],column8=a[0][0][1],column9=a[0][1],column10=a[1]))
		dataFrame = sqlContext.createDataFrame(rows)

	if(n_cols==11):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][1],column6=a[0][0][0][0][0][1],column7=a[0][0][0][0][1],column8=a[0][0][0][1],column9=a[0][0][1],column10=a[0][1],column11=a[1]))
		dataFrame = sqlContext.createDataFrame(rows)

	if(n_cols==12):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][1],column7=a[0][0][0][0][0][1],column8=a[0][0][0][0][1],column9=a[0][0][0][1],column10=a[0][0][1],column11=a[0][1],column12=a[1]))
		dataFrame = sqlContext.createDataFrame(rows)

	if(n_cols==13):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][1],column8=a[0][0][0][0][0][1],column9=a[0][0][0][0][1],column10=a[0][0][0][1],column11=a[0][0][1],column12=a[0][1],column13=a[1]))
		dataFrame = sqlContext.createDataFrame(rows)

	if(n_cols==14):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][0][1],column8=a[0][0][0][0][0][0][1],column9=a[0][0][0][0][0][1],column10=a[0][0][0][0][1],column11=a[0][0][0][1],column12=a[0][0][1],column13=a[0][1],column14=a[1]))
		dataFrame = sqlContext.createDataFrame(rows)

	if(n_cols==15):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][0][0][1],column8=a[0][0][0][0][0][0][0][1],column9=a[0][0][0][0][0][0][1],column10=a[0][0][0][0][0][1],column11=a[0][0][0][0][1],column12=a[0][0][0][1],column13=a[0][0][1],column14=a[0][1],column15=a[1]))
		dataFrame = sqlContext.createDataFrame(rows)

	if(n_cols==16):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][0][0][0][1],column8=a[0][0][0][0][0][0][0][0][1],column9=a[0][0][0][0][0][0][0][1],column10=a[0][0][0][0][0][0][1],column11=a[0][0][0][0][0][1],column12=a[0][0][0][0][1],column13=a[0][0][0][1],column14=a[0][0][1],column15=a[0][1],column16=a[1]))

	if(n_cols==17):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][0][0][0][0][1],column8=a[0][0][0][0][0][0][0][0][0][1],column9=a[0][0][0][0][0][0][0][0][1],column10=a[0][0][0][0][0][0][0][1],column11=a[0][0][0][0][0][0][1],column12=a[0][0][0][0][0][1],column13=a[0][0][0][0][1],column14=a[0][0][0][1],column15=a[0][0][1],column16=a[0][1],column17=a[1]))

	if(n_cols==18):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][0][0][0][0][0][1],column8=a[0][0][0][0][0][0][0][0][0][0][1],column9=a[0][0][0][0][0][0][0][0][0][1],column10=a[0][0][0][0][0][0][0][0][1],column11=a[0][0][0][0][0][0][0][1],column12=a[0][0][0][0][0][0][1],column13=a[0][0][0][0][0][1],column14=a[0][0][0][0][1],column15=a[0][0][0][1],column16=a[0][0][1],column17=a[0][1],column18=a[1]))

	if(n_cols==19):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][0][0][0][0][0][0][1],column8=a[0][0][0][0][0][0][0][0][0][0][0][1],column9=a[0][0][0][0][0][0][0][0][0][0][1],column10=a[0][0][0][0][0][0][0][0][0][1],column11=a[0][0][0][0][0][0][0][0][1],column12=a[0][0][0][0][0][0][0][1],column13=a[0][0][0][0][0][0][1],column14=a[0][0][0][0][0][1],column15=a[0][0][0][0][1],column16=a[0][0][0][1],column17=a[0][0][1],column18=a[0][1],column19=a[1]))

	if(n_cols==20):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][0][0][0][0][0][0][0][1],column8=a[0][0][0][0][0][0][0][0][0][0][0][0][1],column9=a[0][0][0][0][0][0][0][0][0][0][0][1],column10=a[0][0][0][0][0][0][0][0][0][0][1],column11=a[0][0][0][0][0][0][0][0][0][1],column12=a[0][0][0][0][0][0][0][0][1],column13=a[0][0][0][0][0][0][0][1],column14=a[0][0][0][0][0][0][1],column15=a[0][0][0][0][0][1],column16=a[0][0][0][0][1],column17=a[0][0][0][1],column18=a[0][0][1],column19=a[0][1],column20=a[1]))

	if(n_cols==21):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column8=a[0][0][0][0][0][0][0][0][0][0][0][0][0][1],column9=a[0][0][0][0][0][0][0][0][0][0][0][0][1],column10=a[0][0][0][0][0][0][0][0][0][0][0][1],column11=a[0][0][0][0][0][0][0][0][0][0][1],column12=a[0][0][0][0][0][0][0][0][0][1],column13=a[0][0][0][0][0][0][0][0][1],column14=a[0][0][0][0][0][0][0][1],column15=a[0][0][0][0][0][0][1],column16=a[0][0][0][0][0][1],column17=a[0][0][0][0][1],column18=a[0][0][0][1],column19=a[0][0][1],column20=a[0][1],column21=a[1]))

	if(n_cols==22):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column8=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column9=a[0][0][0][0][0][0][0][0][0][0][0][0][0][1],column10=a[0][0][0][0][0][0][0][0][0][0][0][0][1],column11=a[0][0][0][0][0][0][0][0][0][0][0][1],column12=a[0][0][0][0][0][0][0][0][0][0][1],column13=a[0][0][0][0][0][0][0][0][0][1],column14=a[0][0][0][0][0][0][0][0][1],column15=a[0][0][0][0][0][0][0][1],column16=a[0][0][0][0][0][0][1],column17=a[0][0][0][0][0][1],column18=a[0][0][0][0][1],column19=a[0][0][0][1],column20=a[0][0][1],column21=a[0][1],column22=a[1]))

	if(n_cols==23):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column8=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column9=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column10=a[0][0][0][0][0][0][0][0][0][0][0][0][0][1],column11=a[0][0][0][0][0][0][0][0][0][0][0][0][1],column12=a[0][0][0][0][0][0][0][0][0][0][0][1],column13=a[0][0][0][0][0][0][0][0][0][0][1],column14=a[0][0][0][0][0][0][0][0][0][1],column15=a[0][0][0][0][0][0][0][0][1],column16=a[0][0][0][0][0][0][0][1],column17=a[0][0][0][0][0][0][1],column18=a[0][0][0][0][0][1],column19=a[0][0][0][0][1],column20=a[0][0][0][1],column21=a[0][0][1],column22=a[0][1],column23=a[1]))

	if(n_cols==24):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column8=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column9=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column10=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column11=a[0][0][0][0][0][0][0][0][0][0][0][0][0][1],column12=a[0][0][0][0][0][0][0][0][0][0][0][0][1],column13=a[0][0][0][0][0][0][0][0][0][0][0][1],column14=a[0][0][0][0][0][0][0][0][0][0][1],column15=a[0][0][0][0][0][0][0][0][0][1],column16=a[0][0][0][0][0][0][0][0][1],column17=a[0][0][0][0][0][0][0][1],column18=a[0][0][0][0][0][0][1],column19=a[0][0][0][0][0][1],column20=a[0][0][0][0][1],column21=a[0][0][0][1],column22=a[0][0][1],column23=a[0][1],column24=a[1]))

	if(n_cols==25):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column8=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column9=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column10=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column11=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column12=a[0][0][0][0][0][0][0][0][0][0][0][0][0][1],column13=a[0][0][0][0][0][0][0][0][0][0][0][0][1],column14=a[0][0][0][0][0][0][0][0][0][0][0][1],column15=a[0][0][0][0][0][0][0][0][0][0][1],column16=a[0][0][0][0][0][0][0][0][0][1],column17=a[0][0][0][0][0][0][0][0][1],column18=a[0][0][0][0][0][0][0][1],column19=a[0][0][0][0][0][0][1],column20=a[0][0][0][0][0][1],column21=a[0][0][0][0][1],column22=a[0][0][0][1],column23=a[0][0][1],column24=a[0][1],column25=a[1]))

	if(n_cols==26):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column8=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column9=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column10=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column11=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column12=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column13=a[0][0][0][0][0][0][0][0][0][0][0][0][0][1],column14=a[0][0][0][0][0][0][0][0][0][0][0][0][1],column15=a[0][0][0][0][0][0][0][0][0][0][0][1],column16=a[0][0][0][0][0][0][0][0][0][0][1],column17=a[0][0][0][0][0][0][0][0][0][1],column18=a[0][0][0][0][0][0][0][0][1],column19=a[0][0][0][0][0][0][0][1],column20=a[0][0][0][0][0][0][1],column21=a[0][0][0][0][0][1],column22=a[0][0][0][0][1],column23=a[0][0][0][1],column24=a[0][0][1],column25=a[0][1],column26=a[1]))

	if(n_cols==27):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column8=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column9=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column10=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column11=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column12=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column13=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column14=a[0][0][0][0][0][0][0][0][0][0][0][0][0][1],column15=a[0][0][0][0][0][0][0][0][0][0][0][0][1],column16=a[0][0][0][0][0][0][0][0][0][0][0][1],column17=a[0][0][0][0][0][0][0][0][0][0][1],column18=a[0][0][0][0][0][0][0][0][0][1],column19=a[0][0][0][0][0][0][0][0][1],column20=a[0][0][0][0][0][0][0][1],column21=a[0][0][0][0][0][0][1],column22=a[0][0][0][0][0][1],column23=a[0][0][0][0][1],column24=a[0][0][0][1],column25=a[0][0][1],column26=a[0][1],column27=a[1]))

	if(n_cols==28):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column8=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column9=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column10=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column11=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column12=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column13=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column14=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column15=a[0][0][0][0][0][0][0][0][0][0][0][0][0][1],column16=a[0][0][0][0][0][0][0][0][0][0][0][0][1],column17=a[0][0][0][0][0][0][0][0][0][0][0][1],column18=a[0][0][0][0][0][0][0][0][0][0][1],column19=a[0][0][0][0][0][0][0][0][0][1],column20=a[0][0][0][0][0][0][0][0][1],column21=a[0][0][0][0][0][0][0][1],column22=a[0][0][0][0][0][0][1],column23=a[0][0][0][0][0][1],column24=a[0][0][0][0][1],column25=a[0][0][0][1],column26=a[0][0][1],column27=a[0][1],column28=a[1]))

	if(n_cols==29):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column8=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column9=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column10=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column11=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column12=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column13=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column14=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column15=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column16=a[0][0][0][0][0][0][0][0][0][0][0][0][0][1],column17=a[0][0][0][0][0][0][0][0][0][0][0][0][1],column18=a[0][0][0][0][0][0][0][0][0][0][0][1],column19=a[0][0][0][0][0][0][0][0][0][0][1],column20=a[0][0][0][0][0][0][0][0][0][1],column21=a[0][0][0][0][0][0][0][0][1],column22=a[0][0][0][0][0][0][0][1],column23=a[0][0][0][0][0][0][1],column24=a[0][0][0][0][0][1],column25=a[0][0][0][0][1],column26=a[0][0][0][1],column27=a[0][0][1],column28=a[0][1],column29=a[1]))

	if(n_cols==30):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column8=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column9=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column10=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column11=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column12=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column13=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column14=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column15=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column16=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column17=a[0][0][0][0][0][0][0][0][0][0][0][0][0][1],column18=a[0][0][0][0][0][0][0][0][0][0][0][0][1],column19=a[0][0][0][0][0][0][0][0][0][0][0][1],column20=a[0][0][0][0][0][0][0][0][0][0][1],column21=a[0][0][0][0][0][0][0][0][0][1],column22=a[0][0][0][0][0][0][0][0][1],column23=a[0][0][0][0][0][0][0][1],column24=a[0][0][0][0][0][0][1],column25=a[0][0][0][0][0][1],column26=a[0][0][0][0][1],column27=a[0][0][0][1],column28=a[0][0][1],column29=a[0][1],column30=a[1]))

	if(n_cols==31):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column8=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column9=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column10=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column11=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column12=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column13=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column14=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column15=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column16=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column17=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column18=a[0][0][0][0][0][0][0][0][0][0][0][0][0][1],column19=a[0][0][0][0][0][0][0][0][0][0][0][0][1],column20=a[0][0][0][0][0][0][0][0][0][0][0][1],column21=a[0][0][0][0][0][0][0][0][0][0][1],column22=a[0][0][0][0][0][0][0][0][0][1],column23=a[0][0][0][0][0][0][0][0][1],column24=a[0][0][0][0][0][0][0][1],column25=a[0][0][0][0][0][0][1],column26=a[0][0][0][0][0][1],column27=a[0][0][0][0][1],column28=a[0][0][0][1],column29=a[0][0][1],column30=a[0][1],column31=a[1]))

	if(n_cols==32):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column8=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column9=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column10=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column11=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column12=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column13=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column14=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column15=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column16=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column17=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column18=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column19=a[0][0][0][0][0][0][0][0][0][0][0][0][0][1],column20=a[0][0][0][0][0][0][0][0][0][0][0][0][1],column21=a[0][0][0][0][0][0][0][0][0][0][0][1],column22=a[0][0][0][0][0][0][0][0][0][0][1],column23=a[0][0][0][0][0][0][0][0][0][1],column24=a[0][0][0][0][0][0][0][0][1],column25=a[0][0][0][0][0][0][0][1],column26=a[0][0][0][0][0][0][1],column27=a[0][0][0][0][0][1],column28=a[0][0][0][0][1],column29=a[0][0][0][1],column30=a[0][0][1],column31=a[0][1],column32=a[1]))

	if(n_cols==33):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column8=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column9=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column10=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column11=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column12=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column13=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column14=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column15=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column16=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column17=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column18=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column19=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column20=a[0][0][0][0][0][0][0][0][0][0][0][0][0][1],column21=a[0][0][0][0][0][0][0][0][0][0][0][0][1],column22=a[0][0][0][0][0][0][0][0][0][0][0][1],column23=a[0][0][0][0][0][0][0][0][0][0][1],column24=a[0][0][0][0][0][0][0][0][0][1],column25=a[0][0][0][0][0][0][0][0][1],column26=a[0][0][0][0][0][0][0][1],column27=a[0][0][0][0][0][0][1],column28=a[0][0][0][0][0][1],column29=a[0][0][0][0][1],column30=a[0][0][0][1],column31=a[0][0][1],column32=a[0][1],column33=a[1]))

	if(n_cols==34):
		rows=result.map(lambda a: Row(column1=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0],column2=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column3=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column4=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column5=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column6=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column7=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column8=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column9=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column10=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column11=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column12=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column13=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column14=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column15=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column16=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column17=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column18=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column19=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column20=a[0][0][0][0][0][0][0][0][0][0][0][0][0][0][1],column21=a[0][0][0][0][0][0][0][0][0][0][0][0][0][1],column22=a[0][0][0][0][0][0][0][0][0][0][0][0][1],column23=a[0][0][0][0][0][0][0][0][0][0][0][1],column24=a[0][0][0][0][0][0][0][0][0][0][1],column25=a[0][0][0][0][0][0][0][0][0][1],column26=a[0][0][0][0][0][0][0][0][1],column27=a[0][0][0][0][0][0][0][1],column28=a[0][0][0][0][0][0][1],column29=a[0][0][0][0][0][1],column30=a[0][0][0][0][1],column31=a[0][0][0][1],column32=a[0][0][1],column33=a[0][1],column34=a[1]))

		dataFrame = sqlContext.createDataFrame(rows)


	return dataFrame

def show(index, iterator):
	for x in iterator: 
		yield index,x



if __name__ == "__main__":


#Printing metadata
	sc=SparkContext()
	file=sc.textFile(sys.argv[1])
	print sys.argv[1]
	print sys.argv[2]
	print sys.argv[3]
	print sys.argv[4]

#we got the data types and column names
	data_types=sys.argv[3].split("\n")
	column_names=sys.argv[5].split("\n")
	
#Select SubPartitions %

	#print "COUNT"
	#print result.count()
	n_part=file.getNumPartitions()
	per=sys.argv[6]

	final=(n_part*int(per))/100
	test=file.mapPartitionsWithIndex(show)
	sample=test.filter(lambda (x,y): x<final )
	#sample=test
	file=sample.map(lambda x: x[1])

	#print "AFTER COUNT"
	#print result.count()
#End Select Subpartitions %	
	

#we split per column using the delimitator from metadata
	delim=sys.argv[4]
	split=file.map(lambda x:x.split(delim))
	
#we have to analyze each partition individually, we use mapPartitions to analyze it individuallyW
	col=split.map(lambda x: x[0])
	if (data_types[0]=='int'):
		result=col.mapPartitions(minmaxInt)
	if (data_types[0]=='string'):
		result=col.mapPartitions(minmaxStr)
	if (data_types[0]=='bigint'):
		result=col.mapPartitions(minmaxBigInt)
	if (data_types[0]=='double'):
		result=col.mapPartitions(minmaxDouble)


	n_cols=int(sys.argv[2])
	for i in range(1,n_cols):
		col=split.map(lambda x: x[i])
		if (data_types[i]=='int'):
			res=col.mapPartitions(minmaxInt)
			result=result.zip(res)
		if (data_types[i]=='string'):
			res=col.mapPartitions(minmaxStr)
			result=result.zip(res)
		if (data_types[i]=='bigint'):
			res=col.mapPartitions(minmaxBigInt)
			result=result.zip(res)
		if (data_types[i]=='double'):
			res=col.mapPartitions(minmaxDouble)
			result=result.zip(res)



	sqlContext = HiveContext(sc)
	dataFrame=createTable(result,n_cols,sqlContext)

	#We create an index for the query
	dataFrame=dataFrame.withColumn("BLOCK_NUMBER", monotonically_increasing_id())
	dataFrame.registerTempTable("COLUMN_REPORT")


	
	result=sqlContext.sql("""SELECT ((1 - SUM(CASE
                    WHEN (next_min - current_max) >= 0 THEN
                     (next_min - current_max)
                    ELSE
                     (current_max - current_min)
                 END) / SUM(current_max - current_min )) * 100) AS elig
  FROM (SELECT * FROM (SELECT column1 AS current_min,
                       lead(column1,
                            1) over(ORDER BY BLOCK_NUMBER) AS current_max,
                       lead(column1,
                            2) over(ORDER BY BLOCK_NUMBER) AS next_min,
                       Row_Number() OVER(ORDER BY BLOCK_NUMBER) RowNumber FROM COLUMN_REPORT) as t2
                       where pmod(RowNumber,2)!=0 AND next_min IS NOT NULL) as t3 """)

	r= result.map(lambda (a): Row(column=column_names[0],el_si=a))
	r2 = sqlContext.createDataFrame(r)
	r2.write.saveAsTable("result")

	


	for i in range(1,n_cols):
		result=sqlContext.sql("""SELECT ((1 - SUM(CASE
                    WHEN (next_min - current_max) >= 0 THEN
                     (next_min - current_max)
                    ELSE
                     (current_max - current_min)
                 END) / SUM(current_max - current_min)) * 100) AS elig
  FROM (SELECT * FROM (SELECT """+ "column"+str(i+1)+""" AS current_min,
                       lead("""+ "column"+str(i+1)+""",
                            1) over(ORDER BY BLOCK_NUMBER) AS current_max,
                       lead("""+ "column"+str(i+1)+""",
                            2) over(ORDER BY BLOCK_NUMBER) AS next_min,
                       Row_Number() OVER(ORDER BY BLOCK_NUMBER) RowNumber FROM COLUMN_REPORT) as t2
                       where pmod(RowNumber,2)!=0 AND next_min IS NOT NULL) as t3 """)
		
		result=result.na.fill(-1)
		r= result.map(lambda (a): Row(column=column_names[i],el_si=a))

		r2 = sqlContext.createDataFrame(r)
		r2.write.mode("append").saveAsTable("result")
	
	
	sc.stop()
	



