import pandas as pd
import json,uuid

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
movies_df = pd.merge(movies_df, rating_df, on="movieId")

print(movies_df.isna().sum())

# Convert DataFrame to JSON and save it to a file
movies_df.to_json('api/movies.json', orient='records', lines=False)