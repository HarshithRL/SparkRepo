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

#function to read the files
def csv_data_frame(spark,path_user,path_transaction):
    users_df = spark.read.csv(path_user, header=True, inferSchema=True)
    transactions_df = spark.read.csv(path_transaction, header=True, inferSchema=True)
    log.info("Read the Files")
    transactions_df.show()
    users_df.show()
    return users_df,transactions_df


#Function to join the table
def jont_table(transactions_df,users_df,primary_key,forigen_key):
    # Join users and transactions data
    joined_data = transactions_df.join(users_df, transactions_df[primary_key] == users_df[forigen_key])
    log.info("joined dataframe for user and transaction")
    return joined_data

#Function to Count of unique locations where each product is sold.
def product_locations(joined_data,group_by_column,count_by_column):
    group_by_column = group_by_column.strip()
    product_locations_df = joined_data.groupBy(group_by_column).agg(countDistinct(count_by_column))
    log.info("Count of unique locations where each product is sold")
    return product_locations_df

#Function to Find out products bought by each user.
def user_produts(joined_data,group_by_,agg):
    # Group by userid and count distinct locations,
    user_products = joined_data.groupBy(group_by_).agg({agg: "collect_list"})
    log.info("products bought by each user.")
    return user_products

#Function to Total spending done by each user on each product.
def user_product_spending(joined_data,group_by_c1,group_by_c2,agg):
    # Group by userid and count product_description,
    log.info("Total spending done by each user on each product.")
    user_product_spending_df = joined_data.groupBy(group_by_c1, group_by_c2).agg({agg: "sum"})
    return user_product_spending_df

#Function To stop the Session
def stop_session(spark):
    log.info("Spark Session Ended")
    return spark.stop()







