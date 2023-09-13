import unittest
from pyspark.sql import SparkSession
from SparkRepo.src.assignment_2.utils import *

class SparkFunctionsTestCase(unittest.TestCase):
    def setUp(self):
        self.spark = start_session()
        self.log_rdd = read_file(self.spark)
        count_lines = 281234
        #Testing the count of lines in rdd
        actual_input_count = Count_RDD(self.log_rdd)
        expected_output = count_lines
        self.assertEqual(actual_input_count, expected_output)
    def tearDown(self):
        self.spark.stop()

    def test_count_rdd(self):
        expected_count = 281234
        actual_count = Count_RDD(self.log_rdd)
        self.assertEqual(actual_count, expected_count)

    def test_num_warning(self):
        expected_count = 3811
        actual_count = NUM_warning(self.log_rdd)
        self.assertEqual(actual_count, expected_count)

    def test_count_repositories_processed(self):
        expected_count = 37595
        actual_count = count_repositories_processed(self.log_rdd)
        self.assertEqual(actual_count, expected_count)

    def test_find_most_active_client(self):
        expected_client = "api_client.rb: Unauthorised request with token: 46f11b5791b7db9077f4d9a9ab27f93e89dccad4"
        actual_client = find_most_active_client(self.log_rdd)
        self.assertEqual(actual_client, expected_client)

    def test_find_most_failed_client(self):
        expected_client = "api_client.rb: Failed request. URL: https://api.github.com/repos/greatfakeman/Tabchi/commits?sha=Tabchi&per_page=100, Status code: 404, Status: Not Found, Access: ac6168f8776, IP: 0.0.0.0, Remaining: 1749"
        actual_client = find_most_failed_client(self.log_rdd)
        self.assertEqual(actual_client, expected_client)

    def test_find_most_active_hour(self):
        expected_hour = "10"
        actual_hour = find_most_active_hour(self.log_rdd)
        self.assertEqual(actual_hour, expected_hour)

    def test_count_active_repositories(self):
        expected_count = 130023
        actual_count = count_active_repositories(self.log_rdd)
        self.assertEqual(actual_count, expected_count)

if __name__ == '__main__':
    unittest.main()


