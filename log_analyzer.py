
# STUDENT ID: 013774228, NAME:  SARATHCHANDRA MAKKENA
import re
import sys
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.functions import split, regexp_extract


if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Usage: log_analyzer.py <file>")
        exit(-1)

    spark = SparkSession.builder.master("local[*]").appName("log_analyzer Parser").getOrCreate() #creating new instance/ if instance does exists uses same

    sc = spark.sparkContext #represents the connection to a spark cluster, facilitates active JVM for all activities
    sc.setLogLevel("ERROR") #setting up logger to display any ERRORS



""" Read File into data frame, File is passed as part of arguments from command line"""

base_df = spark.read.text(sys.argv[1])

lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0]) # returns new RDD(Resilient Distributed Datasets) by applying the functions appended
gif = lines.flatMap(lambda l: re.findall(r'(?i:gif)', l)) #Retruns RDD by applying the function and flattens the result, Used regular expression to filter .gif/.GIF in each URL line .
pairs = gif.map(lambda w: (len(w), 1)) # from extracted gif URL requests, map function is performed to find length of each element and stores into pairs RDD
counts = pairs.reduceByKey(lambda n1, n2: n1 + n2).sortByKey().values() #Merge the values for each key using an associative and commutative reduce function, and sort elements
out2 = counts.collect() # collect gathers all the elements of reduced dataset as an array, and counter returns complete count of gif's present in array (this case one set of array is returned as our regex querry fine tuned to capture gif/GIF)
#print('Number of ‘.gif’ : ')
#print(out2)# <--- total count of gif's




jpg = lines.flatMap(lambda l: re.findall(r'(?i:jpg|jpeg)', l)) #Retruns RDD by applying the function and flattens the result, Used regular expression to filter .gif/.GIF in each URL line .
pairs = jpg.map(lambda w: (len(w), 1))# from extracted gif URL requests, map function is performed to find length of each element and stores into pairs RDD
counts = pairs.reduceByKey(lambda n1, n2: n1 + n2).sortByKey().values() #Merge the values for each key using an associative and commutative reduce function, and sort elements
out1 = counts.collect()# collect gathers all the elements of reduced dataset as an array, and counter returns complete count of gif's present in array (this case two sets of array is returned as our regex querry  returns  jpg/JPG and jpeg/JPEG as two sets )
#print('Number of ‘.jpg’ :')
#print(out1[0]+out1[1]) # <--- total count of jpeg/JPEG/jpg/JPG
jpg_count = out1[0]+out1[1]
print('Fetching jpegs')
lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
df1 = lines.map(lambda r: Row(r)).toDF(["line"]) # creating dataframe with column line
log1 = df1.filter(col("line").like("%.JPG%" or "%.jpg%" or "%JPEG%" or "%jpeg%")) #framing query onto dataframe to filter for jpg's in URL
log1 = (log1.groupby('line').count().sort('count',ascending= False))# here we're sorting in descending ordger post grouping together
log1.show(20,False) #displaying only jpgs and restricted number of rows displayed to 20
print('Fetching gifs')
lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
df2 = lines.map(lambda r: Row(r)).toDF(["line"]) # creating dataframe with column line
log2 = df2.filter(col("line").like("%.gif%" or "%.GIF%")) #framing query onto dataframe to filter for gif's in URL
log2 = (log2.groupby('line').count().sort('count',ascending= False))# here we're sorting in descending ordger post grouping together
log2.show(20,False)#displaying only gifs and restricted number of rows displayed to 20
print('Fetching rest of the files like .php  etc')
lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
df3 = lines.map(lambda r: Row(r)).toDF(["line"])# creating dataframe with column line
log3 = df3.filter(~col("line").like("%.JPG%")) \
            .filter(~col("line").like("%.jpg%")) \
            .filter(~col("line").like("%.jpeg%"))\
            .filter(~col("line").like("%.GIF%"))\
            .filter(~col("line").like("%.gif%")) \
            .filter(col("line").like("%.php%" or "%.css%"))
#framing query onto dataframe to filter other than jpgs and gifs's in URL
log3 = (log3.groupby('line').count().sort('count',ascending= False))# here we're sorting in descending ordger post grouping together
log3.show(20,False)#displaying other than gifs and jpgs, restricted number of rows displayed to 20



ttpy= jpg_count + out2[0]
split_df = base_df.select(
regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*', 1).alias('path') #regex expression to capture endpoints from URL
)#returns entire URL's
""" re printing final stats """

print ('>>>>> Output <<<<<<<<')
print ('>>>>> 013774228, sarathchandra makkena <<<<<<<<')


print('Number of ‘.gif’ : ')
print(out2)# <--- total count of gif's
print('Number of ‘.jpg’ :')
print(out1[0]+out1[1]) # <--- total count of jpeg/JPEG/jpg/JPG
print('Number of other requests like ‘.php’')
#print(split_df.count())
print(split_df.count() - ttpy) #Here subtracting gif/GIF/jpg/JPG/jped/JPEG from total endpoints, result is "Number of other requests like '.php'"




sc.stop() # stops session