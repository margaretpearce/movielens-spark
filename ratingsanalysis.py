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
print(movies.head())

# Read in the ratings data set
ratings = sqlContext.read\
          .format("com.databricks.spark.csv")\
          .option("header", "true")\
          .load("../MovieLensData/ratings.csv")
print("---Ratings---")
print(ratings.head())

# Compute the average rating by movie
avg_rating = ratings.groupBy("movieId").agg({"rating": "avg", "userId": "count"})
print("---Average Rating---")
print(avg_rating.show(5))
