from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
import logging

logging.basicConfig(filename=r"C:\pyspark\SparkRepo\logs\spark.log", filemode="w",level=logging.INFO)
log = logging.getLogger()
log.info("This is an INFO-level message.")

#Function To start the Session
def start_session():
    log.info("Spark Session created")
    return SparkSession.builder.appName("SparkByAssignment").getOrCreate()


#Function To stop the Session
def stop_session(spark):
    log.info("Spark Session Ended")
    return spark.stop()

# Count of unique locations where each product is sold.
# Find out products bought by each user.
# Total spending done by each user on each product.
# Function for above three questions
def product_analysis(transactions_df,users_df):

    # Join users and transactions data
    joined_data = transactions_df.join(users_df, transactions_df["userid"] == users_df["user_id"])
    log.info("joined dataframe for user and transaction")

    # Group by product_id and count distinct locations,
    product_locations = joined_data.groupBy("product_id").agg(countDistinct("location ").alias("UniqueLocations"))
    log.info("Count of unique locations where each product is sold")

    # Group by userid and count distinct locations,
    user_products = joined_data.groupBy("userid").agg({"product_description": "collect_list"})
    log.info("products bought by each user.")

    # Group by userid and count product_description,
    user_product_spending = joined_data.groupBy("userid", "product_description").agg({"price": "sum"})
    log.info("Total spending done by each user on each product.")

    #Show The Data_Frame
    transactions_df.show()
    users_df.show()

    return product_locations , user_products ,user_product_spending







