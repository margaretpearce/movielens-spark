from pyspark import SparkContext, SQLContext

# Initialize Spark (take parameters from command line)
sc = SparkContext(appName="MovieRatingsAnalysis")
sqlContext = SQLContext(sc)

# Read in the movies data set
movies = sqlContext.read\
          .format("com.databricks.spark.csv")\
          .option("header", "true")\
          .load("../MovieLensData/movies.csv")
print("---Movies---")
movies.show(3, False)

# Read in the ratings data set
ratings = sqlContext.read\
          .format("com.databricks.spark.csv")\
          .option("header", "true")\
          .load("../MovieLensData/ratings.csv")
print("---Ratings---")
ratings.show(3, False)

# Compute the average rating by movie
avg_rating = ratings.groupBy("movieId")\
    .agg({"rating": "avg", "userId": "count"})\
    .withColumnRenamed("avg(rating)", "avgRating")\
    .withColumnRenamed("count(userId)", "numRatings")
print("---Average Rating---")
avg_rating.show(5, False)

# Join the ratings with the name of the movie
ratings_with_titles = avg_rating.join(movies, "movieId")
print("---Average Rating (with Title)---")
ratings_with_titles.show(5, False)

# Show top 5 rated movies
top_rated = ratings_with_titles.orderBy("avgRating", ascending=False).limit(5)
print("--Top 5 rated movies--")
top_rated.show(5, False)

# Many of the top rated movies only have one rating
# What if we filter for movies with more than 20 reviews?
top_rated_filtered = ratings_with_titles.filter(ratings_with_titles["numRatings"] > 20)\
                                        .orderBy("avgRating", ascending=False)\
                                        .limit(5)
print("--Top 5 rated movies (>20 reviews)--")
top_rated_filtered.show(5, False)

# How about the lowest rated movies?
lowest_rated_filtered = ratings_with_titles.filter(ratings_with_titles["numRatings"] > 20)\
                                        .orderBy("avgRating", ascending=True)\
                                        .limit(5)
print("--Lowest 5 rated movies (>20 reviews)--")
lowest_rated_filtered.show(5, False)

# Get all genres
from operator import add
genres = movies.select("genres").distinct().rdd.map(lambda x: x[0])
genre_counts = genres.flatMap(lambda x: x.split('|')).map(lambda x: (x, 1)).reduceByKey(add)
print("--Genres with movie counts--")
print(genre_counts.take(5))


def genre_is_action(genre_row):
    genre_list = genre_row.split("|")
    if "Action" in genre_list:
        return 1
    return 0

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

action_udf = udf(genre_is_action, IntegerType())

print("--Action genre column (non-generic)--")
top_rated.withColumn("genre_action", action_udf(top_rated['genres'])).show(5, False)


# Try to generalize the function
def check_genre(genre_row, genre_value):
    genre_list = genre_row.split("|")
    if genre_value in genre_list:
        return 1
    return 0

genre_udf = udf(check_genre, IntegerType())
from pyspark.sql import functions

print("--Action genre column (generic)--")
top_rated.withColumn("genre_action", genre_udf(top_rated['genres'], functions.lit("Action"))).show(5, False)


# Run the function over multiple genres
def process_genres(movies, genre_value):
    new_col_name = "genre_" + str(genre_value).strip().replace(' ', '').replace('(', '').replace(')', '').lower()
    movies = movies.withColumn(new_col_name, genre_udf(movies['genres'], functions.lit(genre_value)))
    return movies

# Testing with 10 genres as a list
genres_subset = genre_counts.collect()[:10]
movies_subset = top_rated

# Apply to each genre one by one for now
for x in genres_subset:
    movies_subset = process_genres(movies_subset, x[0])

print("--Multiple genre columns--")
print(movies_subset.columns)

# Run the function over the entire data set
all_genres = genre_counts.collect()
movies_add_genres = ratings_with_titles

for genre in all_genres:
    movies_add_genres = process_genres(movies_add_genres, genre[0])

print("--Genres function applied to all movies--")
movies_add_genres.show(20, False)


# Extract the year from the movie title
def extract_year(title_and_year):
    if "(" in str(title_and_year):
        return str(title_and_year).split("(")[-1].replace(")", "")
    else:
        return None

from pyspark.sql.functions import StringType
year_udf = udf(extract_year, StringType())

movies_with_year = movies_add_genres.withColumn("year", year_udf(movies_add_genres["title"]))
print("--Movie years--")
movies_with_year.show(20, False)

movies_pandas = movies_with_year.toPandas()
movies_pandas.to_csv("movies_pandas.csv")
