from pyspark.sql import SparkSession
from SparkRepo.src.assignment_1.utils import *
# Initialize SparkSession
spark = SparkSession.builder.appName("SparkByAssignment").getOrCreate()

#reading the CSV files
users_df = spark.read.csv(r"C:\Users\Harshith\PycharmProjects\pyspark\SparkRepo\resource\user.csv",header=True, inferSchema=True)
transactions_df = spark.read.csv(r"C:\Users\Harshith\PycharmProjects\pyspark\SparkRepo\resource\transaction.csv",header=True, inferSchema=True)

product_locations,products,user_product_spending = product_analysis(users_df, transactions_df)

# Results of the analysis

product_locations.show()
products.show()
user_product_spending.show()

# Show the DF

transactions_df.show()
users_df.show()

# ending the Session

spark.stop()