import pyspark
# importing required libraries

# create a spark session
spark = pyspark.sql.SparkSession \
    .builder \
    .appName("Python Spark Basic Example") \
    .config('spark.driver.extraClassPath','C:/Users/ANOOP/Downloads/postgresql-42.6.0.jar') \
    .getOrCreate()

# read movie table from db using spark
movies_df=spark.read \
    .format("jdbc") \
    .option("url","jdbc:postgresql://localhost:5432/movies") \
    .option("dbtable","movie") \
    .option("user","postgres") \
    .option("password","aforapple") \
    .option("driver","org.postgresql.Driver") \
    .load()

# read users table from db using spark
users_df= spark.read \
    .format("jdbc") \
    .option("url","jdbc:postgresql://localhost:5432/movies") \
    .option("dbtable","users") \
    .option("user","postgres") \
    .option("password","aforapple") \
    .option("driver","org.postgresql.Driver") \
    .load()

# transforming tables
avg_rating = users_df.groupBy("movie_id").mean("rating")

# join the movies_df and avg_rating table on movie_id
df = movies_df.join(avg_rating,movies_df.id == avg_rating.movie_id)

# print all the tables
print(movies_df.show())
print(users_df.show())
print(df.show())









