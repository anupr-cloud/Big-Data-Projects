import pyspark.sql
# importing required libraries

# create a spark session
spark = pyspark.sql.SparkSession \
    .builder \
    .appName("Python Spark Basic Example") \
    .config('spark.driver.extraClassPath','C:/Users/ANOOP/Downloads/postgresql-42.6.0.jar') \
    .getOrCreate()

# read movie table from db using spark
def extract_movies_to_df():
    movies_df=spark.read \
        .format("jdbc") \
        .option("url","jdbc:postgresql://localhost:5432/movies") \
        .option("dbtable","movie") \
        .option("user","<username>") \
        .option("password","<password>") \
        .option("driver","org.postgresql.Driver") \
        .load()
    return movies_df


# read users table from db using spark
def extract_users_to_df():

    users_df= spark.read \
        .format("jdbc") \
        .option("url","jdbc:postgresql://localhost:5432/movies") \
        .option("dbtable","users") \
        .option("user","<username>") \
        .option("password","<password>") \
        .option("driver","org.postgresql.Driver") \
        .load()
    return users_df


# transforming the users and movie table 
def transform_avg_ratings(movies_df,users_df):

    # transforming tables
    avg_rating = users_df.groupBy("movie_id").mean("rating")

    # join the movies_df and avg_rating table on movie_id
    df = movies_df.join(avg_rating,movies_df.id == avg_rating.movie_id)
    return df


# load transformed dataframe into the Postgresql database
def load_to_db(df):
    properties = {
        "user":"<username>",
        "password":"<password>",
        "driver":"org.postgresql.Driver"
    }
    df.write.jdbc(url="jdbc:postgresql://localhost:5432/movies",
                  table="avg_rating",
                  mode="overwrite",
                  properties=properties
                  )
if __name__=="__main__":
    movies_df = extract_movies_to_df()
    users_df = extract_users_to_df()
    ratings_df = transform_avg_ratings(movies_df,users_df)
    load_to_db(ratings_df)










