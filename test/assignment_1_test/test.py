import unittest
from SparkRepo.src.assignment_1.utils import *
from pyspark.sql.types import *

class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("TestApp").getOrCreate()
        #user table
        self.user_schema = StructType([
            StructField('user_id', IntegerType(), True),
            StructField('emailid', StringType(), True),
            StructField('nativelanguage', StringType(), True),
            StructField('location', StringType(), True)  # Removed extra space here
        ])

        self.user_data = [(101, "abc.123@gmail.com", "hindi", "mumbai"),
                     (102, "jhon@gmail.com", "english", "usa"),
                     (103, "madan.44@gmail.com", "marathi", "nagpur"),
                     (104, "local.88@outlook.com", "tamil", "chennai"),
                     (105, "sahil.55@gmail.com", "english", "usa")]
        #transaction table
        self.transaction_schema = StructType([
            StructField('transaction_id', IntegerType(), True),
            StructField('product_id', IntegerType(), True),
            StructField('userid', IntegerType(), True),
            StructField('price', IntegerType(), True),
            StructField('product_description', StringType(), True)
        ])

        self.transaction_data = [(3300101, 1000001, 101, 700, "mouse"),
                            (3300102, 1000002, 102, 900, "keyboard"),
                            (3300103, 1000003, 103, 34000, "tv"),
                            (3300104, 1000004, 101, 35000, "fridge"),
                            (3300105, 1000005, 105, 55000, "sofa")]

        # Total spending done by each user on each product
        self.exp_total_schema = StructType([
            StructField('userid', IntegerType(), True),
            StructField('product_description', StringType(), True),
            StructField('sum(price)', LongType(), True)
        ])

        self.exp_total_data = [(101, "fridge", 35000),
                               (101, "mouse",700),
                               (102, "keyboard", 900),
                               (103, "tv", 34000),
                               (105, "sofa", 55000)]

        #products bought by each user.
        self.exp_product_schema = StructType([
            StructField('userid', IntegerType(), True),
            StructField('collect_list(product_description)', ArrayType(StringType()), True),
        ])
        self.exp_product_data = [(101, ['mouse', 'fridge']),
                            (103, ['tv']),
                            (102, ['keyboard']),
                            (105, ['sofa'])]

        #unique locations where each product is sold
        self.exp_location_schema = StructType([StructField('product_id', IntegerType(), True),StructField('count_by_column', IntegerType(), True)])

        self.exp_location_data = [(1000005, 1),
                             (1000003, 1),
                             (1000001, 1),
                             (1000002, 1),
                             (1000004, 1)
                             ]

        self.users_df = self.spark.createDataFrame(data=self.user_data, schema=self.user_schema)
        self.transactions_df = self.spark.createDataFrame(data=self.transaction_data, schema=self.transaction_schema)
        self.joined_data = self.transactions_df.join(self.users_df,self.transactions_df["userid"] == self.users_df["user_id"])
        self.actural_user_product_spending = self.spark.createDataFrame(data=self.exp_total_data,schema=self.exp_total_schema)
        self.actural_user_produts = self.spark.createDataFrame(data=self.exp_product_data,schema=self.exp_product_schema)
        self.expected_product_locations = self.spark.createDataFrame(data=self.exp_location_data,schema=self.exp_location_schema)
    def tearDown(self):
        self.spark.stop()

    #Test for product_locations Function
    def test_product_locations(self):
        actual_result=product_locations(self.joined_data,"product_id","location")
        self.assertEqual(sorted(self.expected_product_locations.collect()),sorted(actual_result.collect()))

    # Test for user_produts Function
    def test_user_produts(self):
        group_by_ = "userid"
        agg = "product_description"
        expected_result=user_produts(self.joined_data,group_by_,agg)
        self.assertEqual(sorted(self.actural_user_produts.collect()), sorted(expected_result.collect()))

    # Test for user_product_spending Function
    def test_user_product_spending(self):
        group_by_c1 = "userid"
        group_by_c2 = "product_description"
        agg = "price"
        expected_result = user_product_spending(self.joined_data,group_by_c1,group_by_c2,agg)
        self.assertEqual(sorted(self.actural_user_product_spending.collect()), sorted(expected_result.collect()))

if __name__ == '__main__':
    unittest.main()


