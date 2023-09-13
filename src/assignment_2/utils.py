from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import countDistinct
import logging


logging.basicConfig(filename=r"C:\pyspark\SparkRepo\logs\spark.log", filemode="w",level=logging.INFO)
log = logging.getLogger()
log.info("This is an INFO-level message.")


def start_session():
    log.info("Spark Session created")
    return SparkContext(conf=SparkConf().setAppName("SparkByAssignment"))


def read_file(log_file_path,spark):
    log.info("Read The Csv")
    log_rdd = spark.textFile(log_file_path)
    return log_rdd

def Count_RDD(log_rdd):
    log.info("Count the lines in RDD")
    total_lines = log_rdd.count()
    return total_lines
def NUM_warning(log_rdd):
    log.info("Number Of warning logs")
    warning_lines = log_rdd.filter(lambda line: "WARN" in line).count()
    return warning_lines

def count_repositories_processed(log_rdd):
    log.info("count repositories processed")
    api_client_lines = log_rdd.filter(lambda line: "api_client.rb" in line).count()
    return api_client_lines

def find_most_active_client(log_rdd):
    log.info("most active client")
    api_clients = log_rdd.filter(lambda line: "api_client.rb" in line).map(lambda line: (line.split("--")[1].strip(), 1))
    most_active_client = api_clients.reduceByKey(lambda a, b: a + b).max(key=lambda x: x[1])
    return most_active_client[0]

def find_most_failed_client(log_rdd):
    log.info("most failed request")
    failed_requests = log_rdd.filter(lambda line: "Failed request" in line)
    failed_clients = failed_requests.map(lambda line: (line.split("--")[1].strip(), 1))
    most_failed_client = failed_clients.reduceByKey(lambda a, b: a + b).max(key=lambda x: x[1])
    return most_failed_client[0]

def find_most_active_hour(log_rdd):
    log.info("Most active hour of the day")
    valid_log_lines = log_rdd.filter(lambda line: len(line.split(",")) >= 2)
    hour_counts = valid_log_lines.map(lambda line: (line.split(",")[1].split("T")[1][:2], 1))
    hourly_counts = hour_counts.reduceByKey(lambda a, b: a + b)
    most_active_hour = hourly_counts.max(key=lambda x: x[1])
    return most_active_hour[0]

def count_active_repositories(log_rdd):
    log.info("Most active Repo")
    repository_active = log_rdd.filter(lambda x: 'ghtorrent.rb' in x and 'exists' in x).count()
    return repository_active
def stop_session(spark):
    log.info("Spark Session Ended")
    return spark.stop()
