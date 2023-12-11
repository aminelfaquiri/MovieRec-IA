import json
from flask import Flask, jsonify, abort

app = Flask(__name__)

# Read JSON data from file :
with open('api/movies_shuffled.json') as f:
    json_data = json.load(f)


@app.route('/api/movies/<int:page>', methods=['GET'])
def get_movies_page(page):
    if page <= len(json_data) / 10:
        start = page * 10
        end = start + 9
        return jsonify(json_data[start:end])
    else :
        abort(404, f'Page {page} not found')


@app.route('/api/user/<int:user_id>', methods=['GET'])
def get_user_by_id(user_id):
    user_movies = [item for item in json_data if item["userId"] == user_id]
    if user_movies:
        return jsonify(user_movies)
    else :
        abort(404, f'User with ID {user_id} not found')

@app.route('/api/movie/<int:movie_id>', methods=['GET'])
def get_movie_by_id(movie_id) :
    movies = [item for item in json_data if item["movieId"] == movie_id]
    if movies:
        return jsonify(movies)
    else:
        abort(404, f'Movie with ID {movie_id} not found')



if __name__ == '__main__':
    app.run(debug=True)