import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime,date_format,col,from_json,expr,lit,when,array,to_date,udf
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,ArrayType,DateType,FloatType
from elasticsearch import Elasticsearch
from  info_sensetive import cloud_id,api_key,endpoint_elastic,password
import threading


def create_maping_cloud(cloud_id,api_key,mapping,index_name) :
    try :
        es = Elasticsearch(cloud_id=cloud_id, api_key=api_key)

    except Exception as e:
        print(f"Error Elastecsearch Connection: {e}")

    try :
        # Create the index with the specified mapping :
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name, body=mapping)
            print(f"Index '{index_name}' created with mapping.")

        else:
            es.indices.delete(index=index_name)
            es.indices.create(index=index_name, body=mapping)
            print(f"Index '{index_name} is already exists")

    except Exception as e :
            print(f"Error Creation index: {e}")

mapping_user = {
    "mappings": {
            "properties": {
                "userId": {"type": "integer"},
                "age": {"type": "integer"},
                "gender": {"type": "keyword"},
                "timestamp": {"type": "date"},
                "rating": {"type": "integer"},
                "movieId": {"type": "integer"},
                }
            }
    }

mapping_movie = {
    "mappings": {
            "properties": {
                "movieId": {"type": "integer"},
                "title": {"type": "keyword"},
                "release_date": {"type": "date"},
                "genres": {"type": "keyword"}
                }
            }
    }


create_maping_cloud(cloud_id,api_key,mapping_user,"movies_user_rating")
create_maping_cloud(cloud_id,api_key,mapping_movie,"movies_info")

# Create spark session :
spark = SparkSession.builder \
    .appName("MovieRecommender_consumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"\
    "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,") \
    .getOrCreate()

# Read data from Kafka :
kafka_data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "MovieRecommender") \
    .load()

# Specify the value deserializer :
kafka_data = kafka_data.selectExpr("CAST(value AS STRING)")

# Shema Structure for the data :
schema = StructType([
    StructField("Action", IntegerType()),
    StructField("Adventure", IntegerType()),
    StructField("Animation", IntegerType()),
    StructField("Children's", IntegerType()),
    StructField("Comedy", IntegerType()),
    StructField("Crime", IntegerType()),
    StructField("Documentary", IntegerType()),
    StructField("Drama", IntegerType()),
    StructField("Fantasy", IntegerType()),
    StructField("Film-Noir", IntegerType()),
    StructField("Horror", IntegerType()),
    StructField("Musical", IntegerType()),
    StructField("Mystery", IntegerType()),
    StructField("Romance", IntegerType()),
    StructField("Sci-Fi", IntegerType()),
    StructField("Thriller", IntegerType()),
    StructField("War", IntegerType()),
    StructField("Western", IntegerType()),
    StructField("unknown", IntegerType()),
    StructField("IMDb_URL", StringType()),
    StructField("age", IntegerType()),
    StructField("gender", StringType()),
    StructField("movieId", IntegerType()),
    StructField("movie_title", StringType()),
    StructField("rating", IntegerType()),
    StructField("release_date", StringType()),
    StructField("timestamp", IntegerType()),
    StructField("userId", IntegerType()),
])

# parce data to dataframe : 
kafka_data = kafka_data.select(from_json(col("value"),schema=schema).alias("data")).select("data.*")\
     
# Filter out rows with at least one null value in any column :
columns_to_check = ["Action", "Adventure", "Animation", "Children's", "Comedy", "Crime", "Documentary",
                     "Drama", "Fantasy", "Film-Noir", "Horror", "Musical", "Mystery", "Romance", "Sci-Fi",
                     "Thriller", "War", "Western", "unknown", "IMDb_URL", "age", "gender", "movieId",
                     "movie_title", "rating", "release_date", "timestamp", "userId"]

filtered_kafka_data = kafka_data
for column in columns_to_check:
    filtered_kafka_data = filtered_kafka_data.filter(col(column).isNotNull())

# Create genre list :
genre_columns = ["Action", "Adventure", "Animation", "Children's", "Comedy", "Crime", "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western", "unknown"]

# Replace values with column name if 1, else with space :
kafka_data = kafka_data.withColumn("genre", array(*[when(col(column) == 1, column).otherwise(lit('')) for column in genre_columns]))

# remove empty strings from the "genre" list :
kafka_data = kafka_data.withColumn("genre", expr("filter(genre, element -> element != '')"))

# drop all genre columns :
kafka_data = kafka_data.drop(*genre_columns)

# Change the type of "release_date" from string to date :
kafka_data = kafka_data.withColumn("release_date", to_date(col("release_date"), "dd-MMM-yyyy"))

# Change the type of "timestamp" from int to date :
kafka_data = kafka_data.withColumn(
    "timestamp",
    date_format(from_unixtime(col("timestamp").cast("double")), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
)

# Select my data for my index :
movies_user_rating = kafka_data.select("userId", "movieId", "rating", "timestamp", "age", "gender")

movies_info = kafka_data.select("movieId", "movie_title", "release_date", "genre", "IMDb_URL")

##################  Elasticsearch  agregation  ##################

# get rating from elasticsearch and generat the avrage :
# es = Elasticsearch(cloud_id=cloud_id, api_key=api_key)
# agregation :
# movies_ratings_df =  movies_ratings_df.withColumn("timestamp")

#################################################################

# Print the schema of my Data :
movies_user_rating.printSchema()
movies_info.printSchema()


def write_to_elasticsearch(df, index_name, id_option=True):

    query = df.writeStream \
        .outputMode("append") \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", index_name) \
        .option("es.nodes", endpoint_elastic) \
        .option("es.port", "9243") \
        .option("es.net.http.auth.user", "elastic") \
        .option("es.net.http.auth.pass", password) \
        .option("es.nodes.wan.only", "true") \
        .option("es.write.operation", "index") \
        .option("checkpointLocation", f"./checkpoint_{index_name}/")
    
    if id_option != True :
        query.option("es.mapping.id", id_option)
    
    query.start().awaitTermination()

# Separate threads for each DataFrame :
thread_movies_user_rating = threading.Thread(target=write_to_elasticsearch, args=(movies_user_rating, "movies_user_rating"))
thread_movies_info = threading.Thread(target=write_to_elasticsearch, args=(movies_info, "movies_info","movieId"))

# Start both threads :
thread_movies_user_rating.start()
thread_movies_info.start()

# Wait for both threads to finish :
thread_movies_user_rating.join()
thread_movies_info.join()