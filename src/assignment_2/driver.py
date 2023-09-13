from SparkRepo.src.assignment_2.utils import *
sc = start_session()
log_file_path = r"C:\pyspark\SparkRepo\resource\ghtorrent-logs.txt"
log_rdd = read_file(log_file_path, sc)
# Task 2: Count the total number of lines in the RDD
total_lines = Count_RDD(log_rdd)
total_lines.collect()

# Task 3: Count the number of WARNING messages
warning_lines = NUM_warning(log_rdd)
warning_lines.collect()

# Task 4: Count the number of repositories processed
total_repositories_processed = count_repositories_processed(log_rdd)
total_repositories_processed.collect()

# Task 5: Find the most active client
most_active_client = find_most_active_client(log_rdd)
most_active_client.collect()

# Task 6: Find the most failed client
most_failed_client = find_most_failed_client(log_rdd)
most_failed_client.collect()

# Task 7: Find the most active hour
most_active_hour = find_most_active_hour(log_rdd)
most_active_hour.collect()

#Find the number of active repositories
repository_active = count_active_repositories(log_rdd)
repository_active.collect()

# Stop the SparkContext when done
sc.stop()

