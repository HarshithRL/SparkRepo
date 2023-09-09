from pyspark.sql import SparkSession
from SparkRepo.src.assignment_1.utils import *

# Initialize SparkSession
spark=start_session()

users_df = spark.read.csv(r"C:\pyspark\SparkRepo\resource\user.csv", header=True, inferSchema=True)
transactions_df = spark.read.csv(r"C:\pyspark\SparkRepo\resource\transaction.csv", header=True, inferSchema=True)

# Call the product_analysis function
product_locations,products,user_product_spending = product_analysis(transactions_df,users_df)

# Results of the analysis

# Count of unique locations where each product is sold.
product_locations.show()

# Find out products bought by each user.
products.show()

# Total spending done by each user on each product.
user_product_spending.show()

# ending the Session
stop_session(spark)