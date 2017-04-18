from pyspark import SparkContext, SQLContext

# Initialize Spark (take parameters from command line)
sc = SparkContext(appName="MovieRatingsAnalysis")
# sc.setLogLevel("INFO")
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