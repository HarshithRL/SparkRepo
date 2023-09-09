import unittest
from pyspark.sql import SparkSession
from SparkRepo.src.assignment_1.utils import product_analysis
from pyspark.sql.types import *


class MyTestCase(unittest.TestCase):
    def test_product_analysis(self):
        spark = SparkSession.builder.appName("TestApp").getOrCreate()
        user_schema = StructType([
            StructField('user_id', IntegerType(), True),
            StructField('emailid', StringType(), True),
            StructField('nativelanguage', StringType(), True),
            StructField('location ', StringType(), True)
        ])
        # actual data for users
        user_data = [(101, "abc.123@gmail.com", "hindi", "mumbai"),
                     (102, "jhon@gmail.com", "english", "usa"),
                     (103, "madan.44@gmail.com", "marathi", "nagpur"),
                     (104, "local.88@outlook.com", "tamil", "chennai"),
                     (105, "sahil.55@gmail.com", "english", "usa")
                     ]
        # creating two dataframes
        users_df = spark.createDataFrame(data=user_data, schema=user_schema)

        transaction_schema = StructType([
            StructField('transaction_id', IntegerType(), True),
            StructField('product_id', IntegerType(), True),
            StructField('userid', IntegerType(), True),
            StructField('price', IntegerType(), True),
            StructField('product_description', StringType(), True)
        ])
        # actual dataframe data for transaction
        transaction_data = [(3300101, 1000001, 101, 700, "mouse"),
                            (3300102, 1000002, 102, 900, "keyboard"),
                            (3300103, 1000003, 103, 34000, "tv"),
                            (3300104, 1000004, 101, 35000, "fridge"),
                            (3300105, 1000005, 105, 55000, "sofa")
                            ]
        transactions_df = spark.createDataFrame(data=transaction_data, schema=transaction_schema)

        location_df, user_products, product_spending = product_analysis(transactions_df, users_df)

        # Test 1: Check the count of unique locations where each product is sold
        actual_location_data = location_df.select('product_id', 'UniqueLocations').collect()
        expected_location_data = [(1000001, 1), (1000002, 1), (1000003, 1), (1000004, 1), (1000005, 1)]
        self.assertEqual(sorted(actual_location_data), sorted(expected_location_data))

        # Test 2: Check products bought by each user
        actual_user_products = user_products.select('userid', 'collect_list(product_description)').collect()
        expected_user_products = [(101, ["mouse", "fridge"]), (102, ["keyboard"]), (103, ["tv"]), (105, ["sofa"])]
        self.assertEqual(sorted(actual_user_products), sorted(expected_user_products))

        # Test 3: Check total spending done by each user on each product
        actual_product_spending = product_spending.select('userid', 'product_description', 'sum(price)').collect()
        expected_product_spending = [(101, "fridge", 35000), (101, "mouse", 700),(102, "keyboard", 900), (103, "tv", 34000), (105, "sofa", 55000)]
        self.assertEqual(sorted(actual_product_spending), sorted(expected_product_spending))

        spark.stop()

if __name__ == '__main__':
    unittest.main()

