
from pyspark.sql.functions import countDistinct

# Count of unique locations where each product is sold.
# Find out products bought by each user.
# Total spending done by each user on each product.
# Function for above three questions
def product_analysis(users_df, transactions_df):
    # Join users and transactions data
    joined_data = transactions_df.join(users_df, transactions_df["userid"] == users_df["user_id"])

    # Group by product_id and count distinct locations
    product_locations = joined_data.groupBy("product_id").agg(countDistinct("location ").alias("UniqueLocations"))
    user_products = joined_data.groupBy("userid").agg({"product_description": "collect_list"})
    user_product_spending = joined_data.groupBy("userid", "product_description").agg({"price": "sum"})
    return product_locations , user_products ,user_product_spending







