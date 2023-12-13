from flask import Flask, render_template, request, jsonify
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired
from elasticsearch import Elasticsearch
import secrets
from elastic_cloud import cloud_id,api_key
from recomondation_functions import *


app = Flask(__name__, template_folder='.')
app.config['SECRET_KEY'] = secrets.token_hex(16)

# connect to elasticsearch :
es = Elasticsearch(cloud_id=cloud_id, api_key=api_key)

def get_movie_recommendations(title):

        movie_info = get_my_movie_detaile(es,title)

        if movie_info :

            userId_list = get_user_ids(title,es)

            Movieid_list = get_moviesids(userId_list)

            recommendations = get_movies_information(Movieid_list,es)

            return {'Your Movies': movie_info,'recommendations': recommendations}
        else :
            return None

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