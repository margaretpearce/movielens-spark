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
movies.show(3)

# Read in the ratings data set
ratings = sqlContext.read\
          .format("com.databricks.spark.csv")\
          .option("header", "true")\
          .load("../MovieLensData/ratings.csv")
print("---Ratings---")
ratings.show(3)

# Compute the average rating by movie
avg_rating = ratings.groupBy("movieId").agg({"rating": "avg", "userId": "count"})\
    .withColumnRenamed("avg(rating)", "avgRating")\
    .withColumnRenamed("count(userId)", "numRatings")
print("---Average Rating---")
avg_rating.show(5)

# Join the ratings with the name of the movie
ratings_with_titles = avg_rating.join(movies, "movieId")
print("---Average Rating (with Title)---")
ratings_with_titles.show(5)
