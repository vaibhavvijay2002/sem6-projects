import praw
import time
import mysql.connector
from kafka import KafkaProducer
import json

personalusesript = 'JrEYxi9YWpOuf_TzuarJig'
secret = 'F7dJ9YH7CAAGaoBrSoGPTNJ_a4YnHw'

reddit = praw.Reddit(client_id=personalusesript,
                     client_secret=secret,
                     username='ResponsibleAd5774',
                     password='c08bt84535',
                     user_agent='dbtproject for u/ResponsibleAd5774')

subreddit = reddit.subreddit('all')

cnx = mysql.connector.connect(user='varun', password='872002',
                              host='localhost',
                              database='dbt')

# create a cursor object
cursor = cnx.cursor()

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
value_serializer=lambda x: json.dumps(x).encode('utf-8'))

topic = ["topic1","topic2","topic3"]

start_time = time.time()
with open("times.txt", "a") as f:
    f.write(str(start_time))
f.close()


while True:
    for submission in subreddit.stream.submissions():
        info = str(submission.title.encode('utf-8')) + "$$" + str(submission.is_self) + "$$" + str(submission.over_18) + "$$\n"

        print(submission.title)

        message = info

        producer.send('topic1', value=message)
        producer.send('topic2', value=message)
        producer.send('topic3', value=message)

        fields = message.split('$$')
        # extract the fields
        title = fields[0]
        is_self = fields[1]
        over_18 = fields[2]

        add_COMPLETE = ("INSERT INTO completeinfo"
                        "(title, is_self, over_18)"
                        "VALUES (%s, %s, %s)")
        data_complete = (title, is_self, over_18)

        cursor.execute(add_COMPLETE, data_complete)
        cnx.commit()

        print("-----waiting 3 secs-------")
        time.sleep(3)


# from pyspark import SparkContext
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, to_json
# from pyspark.sql.types import StructType, StructField, StringType
# from pyspark.streaming import StreamingContext
# from pyspark.sql.functions import explode, count
# from pyspark.sql.functions import split
# import pandas as pd
# from kafka import KafkaProducer

# client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# client_socket.connect(('localhost', 8001))


# sc = SparkContext(appName="spark2kafka")
# spark = SparkSession(sc)
# ssc = StreamingContext(sc,10)
# socket_stream = ssc.socketTextStream('localhost', 8000)
# print("------Starting--------")
# df = pd.DataFrame()

# sproducer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# def write_to_kafka(rdd):
#     if not rdd.isEmpty():
#         rdd.map(lambda x: (None, x)).toDF(["key", "value"]) \
#             .writeStream.format("console") \
#             .outputMode("complete")\
#             .option("host", "localhost")\
#             .option("port", 8000)\
#             .save()

        # message = client_socket.recv(1024)
        # don't print but use spark sql to create a table that can be used to insert into a db
        


    # producer.send("reddit-posts",message)
    # titles = spark.readStream.format('socket').option('host','localhost').option('port',8000).load()
    # titles.writeStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("topic", "reddit-posts") \
    #     .start()

    # coun = titles.agg(count("*").alias("title_count"))
    # query = titles.writeStream.outputMode("update").format("console").start()
    
   
"""query = titles.writeStream\
    .format("kafka")\
    .option("host", "localhost")\
    .option("topic", "reddit-posts")\
    .start()

    
titles.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "reddit-posts") \
    .start()
"""


# import time
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StructField, StringType

# from kafka import KafkaProducer
# import json

# # Create Spark session
# spark = SparkSession.builder.appName("RedditStreaming").getOrCreate()

# # Define the schema for the incoming data
# schema = StructType([StructField("title", StringType(), True)])

# # Create Kafka producer
# producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
#                          value_serializer=lambda x:
#                          json.dumps(x).encode('utf-8'))

# # Read the stream from Kafka
# df = spark.readStream.format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "reddit-posts") \
#     .option("startingOffsets", "earliest") \
#     .load()

# # Parse the JSON data from Kafka into a DataFrame with the defined schema
# parsed_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

# # Filter posts by text length using Spark
# filtered_posts = parsed_df.filter(col("title").isNotNull()).filter(col("title").rlike(".{20,}"))

# # Add filtered posts to the stream
# filtered_posts.writeStream \
#     .foreach(lambda post: producer.send('reddit-posts', post.title)) \
#     .start()

# # Start the streaming query
# query = filtered_posts.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# # Wait for the query to terminate
# query.awaitTermination()
