from kafka import KafkaConsumer
import mysql.connector
import json
import time
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import length
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

cnx = mysql.connector.connect(user='varun', password='872002',
                              host='localhost',
                              database='dbt')

# create a cursor object
cursor = cnx.cursor()

consumer1 = KafkaConsumer(bootstrap_servers=['localhost:9092'], value_deserializer=lambda x: json.loads(x.decode('utf-8')))

consumer1.subscribe(['topic1'])

# create a SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

schema = StructType([
    StructField("title", StringType()),
    StructField("is_self", StringType()),
    StructField("over_18", StringType())
])

# Continuously read messages from the Kafka topic
for message in consumer1:
    fields = message.value.split("$$")
    # fields[0] = StringType(fields[0])
    # fields[1] = StringType(fields[1])
    # fields[2] = StringType(fields[2])
    
    wordsDataFrame = spark.createDataFrame([fields[:3]],schema=schema)
    wordsDataFrame.createOrReplaceTempView("words")

    wordCountsDataFrame = spark.sql("select * from words where is_self='False'")

    wordCountsDataFrame.show()

    insert_stmt = ("INSERT INTO consumertable1 "
               "(title, is_self, over_18) "
               "VALUES (%s, %s, %s)")
    end_time = time.time()
    with open("time.txt", "a") as f:
        f.write("\nConsumer 1 took:\t") 
        f.write(str(end_time))
        f.write("\n")
    f.close()

    # get the data from Spark
    wordCounts = wordCountsDataFrame.collect()

    # loop through the data and insert each row into the table
    for row in wordCounts:
        data = (row['title'], row['is_self'], row['over_18'])
        cursor.execute(insert_stmt, data)
        cnx.commit()

    # commit the changes to the database
    # close the cursor and database connection
    # cnx.close()


"""
create table consumer1 (
    title varchar(10000) primary key,
    isSelf varchar(10),
    over18 varchar(10)
)
"""