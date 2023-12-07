from elasticsearch import Elasticsearch

# Specify the Elasticsearch Cloud credentials and endpoint :
cloud_id = "fe0889c7219f4abda8c4968ab721e709:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQxMzRhYTE1ODE1Y2M0NGJmODQxNmFkNmI2NGEzM2JhZSQwZTYyYWY0Mjk0ZWE0Y2RhODBiMmYxZDZiM2U0OTEwNA=="
api_key = "UVZWTlA0d0JQazllek5fZ3h2WDU6VWZYdHlVOHdTQS1MMXJFLXlQbzI2Zw=="

es = Elasticsearch(cloud_id=cloud_id, api_key=api_key)
index_name = "movierec"

query = {
    "match_all": {}
}

movies = es.search(index=index_name,query=query)

# print(movies)
for movie in movies['hits']['hits'] :
    print(movie['_source'])


