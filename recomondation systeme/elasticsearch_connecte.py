import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
from elasticsearch import Elasticsearch

# Specify the Elasticsearch Cloud credentials and endpoint :
cloud_id = "fe0889c7219f4abda8c4968ab721e709:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQxMzRhYTE1ODE1Y2M0NGJmODQxNmFkNmI2NGEzM2JhZSQwZTYyYWY0Mjk0ZWE0Y2RhODBiMmYxZDZiM2U0OTEwNA=="
api_key = "UVZWTlA0d0JQazllek5fZ3h2WDU6VWZYdHlVOHdTQS1MMXJFLXlQbzI2Zw=="

es = Elasticsearch(cloud_id=cloud_id, api_key=api_key)

index_name = "movies-user_rating"

title = "Seven Years in Tibet (1997)"

def get_my_movie_detaile() :
     pass

def get_user_ids(title, index_name,es) :
      
  query =  { "query": {
      "match": {"movie_title": str(title).strip()}}
      }

  response = es.search(index=index_name, body=query)

  userid_list = []

  if response['hits']['total']['value'] > 0 :
        movie_info = response['hits']['hits'] 

        for i in movie_info :
            userid_list.append(i['_source']['userId'])

        return userid_list
  else :
      print("Movie not found")

print(get_user_ids(title, index_name,es))


def get_moviesids() :
    # Create a Spark session :
    spark = SparkSession.builder.appName("ModelUsage").getOrCreate()

    model_path= 'C:/Users/Youcode/Desktop/Devlepeure Data/Project_Breif/film-recommender_with_ia/ALS/best_model'

    # Load the ALS model :
    loaded_model = ALSModel.load(model_path)

    # Get the top 10 movies for each user :
    top_movies = loaded_model.recommendForAllUsers(10)







def get_movies_information(movieId_list,index_name) :

        query = {
          "_source": ["userId", "movie_title", "rating","release_date"],
          "query": {
            "terms": {
              "movieId": movieId_list
            }
          }
        }

        response2 = es.search(index=index_name, body=query)

        movies_list = []
        for j in response2['hits']['hits'] :
            movies_list.append(j['_source'])

        return(movies_list)