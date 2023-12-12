from flask import Flask, render_template, request, jsonify
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired
from elasticsearch import Elasticsearch
import secrets

app = Flask(__name__, template_folder='.')
app.config['SECRET_KEY'] = secrets.token_hex(16)


# connect to elasticsearch :
# Specify the Elasticsearch Cloud credentials and endpoint :
cloud_id = "fe0889c7219f4abda8c4968ab721e709:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQxMzRhYTE1ODE1Y2M0NGJmODQxNmFkNmI2NGEzM2JhZSQwZTYyYWY0Mjk0ZWE0Y2RhODBiMmYxZDZiM2U0OTEwNA=="
api_key = "UVZWTlA0d0JQazllek5fZ3h2WDU6VWZYdHlVOHdTQS1MMXJFLXlQbzI2Zw=="

es = Elasticsearch(cloud_id=cloud_id, api_key=api_key)

def get_movie_recommendations(title):
    # Elasticsearch query to find a movie by title :
    query =  { "query": {
    "match": {"movie_title": title}}
    }

    # Execute the query :
    response = es.search(index="movies-user_rating", body=query)


    userid_list = []
    if response['hits']['total']['value'] > 0 :
            
            # select the data from response :
            movie_info = response['hits']['hits'] 

            # save the all ids in list :
            for i in movie_info :
                userid_list.append(i['_source']['userId'])



            query2 = {
            "_source": ["userId", "movie_title", "rating"],
            "query": {
              "terms": {
                "userId": userid_list
              }
            }
            }

            response = es.search(index="movies-user_rating", body=query2)

            movies_list = []
            for j in response['hits']['hits'] :
                movie_info.append(j['_source'])

            print(movies_list)
            
            return {'Your Movies': title ,'recommendations': movie_info}

    else :
          return None
########################### Last exemple #########################
    # # Extract movie information from the response  :
    # if response['hits']['total']['value'] > 0 :
    #     movie_info = response['hits']['hits'][0]['_source']

    #     ######################## Recomendation Movies ######################################

    #     # Elasticsearch query to find similar movies
    #     recommendation_query = {
    #         "query": {
    #             "bool": {
    #                 "must_not": [
    #                     {"match": {"title": title}}
    #                 ],

    #                 "filter": [
    #                     {"terms": {"genre_ids": movie_info.get('genre_ids', [])}},
    #                     {"range": {"vote_average": {"gte": movie_info.get('vote_average', 0)}}},
    #                     {"range": {"popularity": {"gte": movie_info.get('popularity', 0)}}}
    #                 ]
    #             }
    #         },
    #     }

    #     recommendation_response = es.search(index='movies', body=recommendation_query,size=5)

    #     Movies = []
    #     # Extract movie recommendations from the response :
    #     recommendations = [hit['_source'] for hit in recommendation_response['hits']['hits']]

    #     for movie in recommendations :
    #         del movie["adult"]
    #         del movie["genre_ids"]
    #         del movie["overview"]
    #         del movie["backdrop_path"]
    #         del movie["original_title"]
    #         del movie["video"]
    #         del movie["description"]
    #         # del movie["poster_path"]
    #         Movies.append(movie)

    #     # #####################################################################################
    #     return {'Your Movies': movie_info,'recommendations': Movies}
    # else:
    #     return None

# ############################ Inpute ###############################
class MovieForm(FlaskForm):
    movie_name = StringField('Movie Name', validators=[DataRequired()])
    submit = SubmitField('Get Recommendations')

@app.route('/', methods=['GET', 'POST'])
def index():
    form = MovieForm()
    result = None

    if form.validate_on_submit():
        movie_name = form.movie_name.data
        result = get_movie_recommendations(movie_name)

    return render_template('index.html', form=form, result=result)

@app.route('/movie/<string:title>')
def get_movie_info(title):
    result = get_movie_recommendations(title)
    if result:
        return jsonify(result)
    else:
        return jsonify({'title': title, 'message': 'Movie not found'})

if __name__ == '__main__':
    app.run(debug=True)