
import pandas as pd
import json


# change txt file to dataframe :
def change_to_df(filpath,splitter,columns) :

    df = pd.read_csv(filpath, sep=splitter, header=None, names=columns,encoding='latin-1')
    return df

# define the column name :
movies_review = ["movieId","movie_title","release_date","error","IMDb_URL","unknown","Action","Adventure","Animation","Children's","Comedy","Crime","Documentary","Drama","Fantasy","Film-Noir","Horror","Musical","Mystery","Romance","Sci-Fi","Thriller","War","Western"]
user_rating = ['userId', 'movieId', 'rating', 'timestamp']
user_info = ["userId","age","gender","occupation","zip"]


# Change all file to dataframe :
movies_df = change_to_df('api/data/item.txt','|',movies_review).drop('error', axis=1)
rating_df = change_to_df('api/data/data.txt','\t',user_rating)
user_df = change_to_df('api/data/user.txt','|',user_info)

# merge between user and rating dataframe :
rating_df = pd.merge(rating_df, user_df, on="userId").drop(['occupation','zip'], axis=1)

i = 0

# change the dataframe to json file :
result = []
for _, entry in rating_df.iterrows():
    user_id = entry['userId']
    movie_id = entry['movieId']
    rating = entry['rating']
    timestamp = entry['timestamp']
    age = entry['age']
    gender = entry['gender']

    # Check if movie_id exists in item_df :
    movie_data = movies_df[movies_df['movieId'] == movie_id]
    if not movie_data.empty:
        movie_data = movie_data.iloc[0]
        title = movie_data['movie_title']
        genres = [genre for genre, value in movie_data.items() if value == 1 and genre != 'movieId']

        fishies_entry = {
            'userId': user_id,
            'age': age,
            'gender' :gender,
            'movie': {
                'movieId': movie_id,
                'title': title,
                'genres': genres
            },
            'rating': rating,
            'timestamp': timestamp
        }

        result.append(fishies_entry)
        print(i)
        i += 1

# Save this to a JSON file :
with open('movies_rating.json', 'w') as f:
    json.dump(result, f)
