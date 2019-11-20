import tmdbsimple as tmdb
import json
import numpy as np
import pandas as pd
import time

LIMIT = 40 # per 10 seconds limit
TOTAL_COUNT = 0
tmdb.API_KEY = '7882fbc1410fc3c555a98f3686553fd8'

def get_revenue_from_api(wiki_movies):
	search = tmdb.Search()
	current_count = 0
	global TOTAL_COUNT
	current_total_count = 0
	total = len(wiki_movies.index)
	for index, row in wiki_movies.iterrows():
		title = row['Title']
		if row['Revenue'] != -1 or row['Budget'] != -1:
			current_total_count+=1
			print("Skipping.. {}".format(current_total_count))
			continue
		current_count+=1
		if current_count == LIMIT:
			print("{} movies left Waiting 10 secs....".format(total-TOTAL_COUNT))
			time.sleep(10)
			current_count = 1 
		try:
			search.movie(query=title)
		except:
			print("Caught exception waiting 10 secs...")
			time.sleep(10)
			search.movie(query=title)
		if len(search.results)<1:
			continue
		s = search.results[0]
		
		current_count+=1
		if current_count == LIMIT:
			print("{} movies left Waiting 10 secs....".format(total-TOTAL_COUNT))
			time.sleep(10)
			current_count = 1 
		try:
			response = (tmdb.Movies(s['id'])).info()
		except:
			print("Caught exception waiting 10 secs...")
			time.sleep(10)
			response = (tmdb.Movies(s['id'])).info()
		revenue = response['revenue']
		budget = response['budget']
		current_count+=1
		row['Revenue'] = revenue
		row['Budget'] = budget
		TOTAL_COUNT+=1
		current_total_count+=1
		wiki_movies.at[index,'Revenue'] = revenue
		wiki_movies.at[index,'Budget'] = budget
		
	return wiki_movies

if __name__ == '__main__':
	wiki_movies = pd.read_csv('wiki_movie_plots_deduped.csv')
	wiki_movies['Revenue'] = -1
	wiki_movies['Budget'] = -1
	wiki_movies = wiki_movies[wiki_movies['Release Year']>2000]
	print("Length of wiki_movies: {}".format(len(wiki_movies.index)))
	try_count = 0
	while(TOTAL_COUNT < len(wiki_movies.index)):
		try:
			wiki_movies = get_revenue_from_api(wiki_movies)
		except:
			print("Total count is: {}".format(TOTAL_COUNT))
			wiki_movies.to_csv('wiki_movies'+str(try_count)+".csv")
			print('Waiting 20 secs before continuing as a precaution')
			time.sleep(20)
			try_count+=1
			if(try_count>1000):
				break
	print("Finished While loop with {} tries".format(try_count))
	wiki_movies.to_csv('wiki_movies_last.csv')


	
	
	