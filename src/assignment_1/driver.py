from SparkRepo.src.assignment_1.utils import *

# Initialize SparkSession
spark=start_session()

#path of the csv files
path_user_file=r"C:\pyspark\SparkRepo\resource\user.csv"
path_transaction_path=r"C:\pyspark\SparkRepo\resource\transaction.csv"
#Read the files
users_df,transactions_df=csv_data_frame(spark,path_user_file,path_transaction_path)

#Join the User & Transaction tables
primary_key="userid"
forigen_key="user_id"
joined_data=jont_table(transactions_df,users_df,primary_key,forigen_key)

#Count of unique locations where each product is sold.
group_by_column="product_id"
count_by_column="location "
y=product_locations(joined_data,group_by_column,count_by_column)
y.show()
y.printSchema()

#Find out products bought by each user
group_by_="userid"
agg="product_description"
x=user_produts(joined_data,group_by_,agg)
x.show()
x.printSchema()

#Total spending done by each user on each product.
group_by_c1="userid"
group_by_c2="product_description"
agg="price"
w=user_product_spending(joined_data,group_by_c1,group_by_c2,agg)
w.show()
w.printSchema()

# Ending the Session
stop_session(spark)