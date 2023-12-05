
import pandas as pd
import json

def change_to_df(filpath,splitter,columns) :

    df = pd.read_csv(filpath, sep=splitter, header=None, names=columns)
    return df

user_rating = ['userId', 'movieId', 'rating', 'timestamp']
movies_review = ["movieId","movie_title","release_date","error","IMDb_URL","unknown","Action","Adventure","Animation","Children's","Comedy","Crime","Documentary","Drama","Fantasy","Film-Noir","Horror","Musical","Mystery","Romance","Sci-Fi","Thriller","War","Western"]

user_df = change_to_df('api/data/data.txt','\t',user_rating)
# item_df = change_to_df('api/data/item.txt','|',movies_review)