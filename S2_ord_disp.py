import os
import urllib.request
import ssl
import sys

from pyspark.sql.functions import explode, isnull
from pyspark.sql.types import StringType

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

hadoop_home = os.path.abspath("hadoop")   # <-- absolute path
os.makedirs(os.path.join(hadoop_home, "bin"), exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt":os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe":os.path.join(hadoop_home, "bin", "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll":os.path.join(hadoop_home, "bin", "hadoop.dll")
}

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()

for url, path in urls_and_paths.items():
    # Use the unverified context with urlopen
    with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
        data = response.read()
        out_file.write(data)

# ======================================================================================
python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ["HADOOP_HOME"] = r"C:\hadoop"
#os.environ['HADOOP_HOME'] = hadoop_home
os.environ['JAVA_HOME'] = r'C:\Users\1767\.jdks\corretto-1.8.0_462'        #  <----- 🔴JAVA PATH🔴
# Set HADOOP_HOME to the folder ABOVE the bin folder
# Add the bin folder to your System Path
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"
######################🔴🔴🔴################################
#Write code from below

print("\nScenario 2: Need the dates and status when the status gets changed from Ordered to Dispatched")
#Usual sequence: Customer ORDERED -> DISPATCHED -> Shipped -> Delivered

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#Spark Setup
spark = SparkSession.builder.appName("Scenario-2").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

order_details = [(1, "1-Jan", "Ordered"),
                 (1, "2-Jan", "Dispatched"),
                 (1, "3-Jan", "Dispatched"),
                 (1, "4-Jan", "Shipped"),
                 (1, "5-Jan", "Shipped"),
                 (1, "6-Jan", "Delivered"),
                 (2, "1-Jan", "Ordered"),
                 (2, "2-Jan", "Dispatched"),
                 (2, "3-Jan", "Shipped"),
                 (2, "4-Jan", "Delivered") ]
columns = ["ord_id", "status_date", "status"]
order_details_df = spark.createDataFrame(order_details, columns)

print("\nThe status date and status when changed from Ordered to Dispatched")
print("\nUsing Spark DSL")
order_details_df_dsl = (order_details_df.alias("A").join(order_details_df.filter(col("status") == "Ordered").select("ord_id").alias("B"),
                                                        col("A.ord_id") == col("B.ord_id"),
                                                        "semi")
                        .filter(col("A.status") == "Dispatched")
                        .select("status_date", "status"))
order_details_df_dsl.show()
#Note: Here we used "SEMI" join. What Semi join does is it fetches the matching records from the left table only, unlike in the INNER join,
# where the Inner fetches the records from both the tables
# This is just like
# [SELECT *
# FROM left_table
# WHERE ord_id IN (SELECT ord_id FROM right_table)]
#Semi Join = IN clause in SQL

print("Using Spark SQL")
order_details_df.createOrReplaceTempView("order_dispatch")
spark.sql(" select status_date, status"
          " from order_dispatch"
          " where status = 'Dispatched' and "
          " ord_id in ("
          " select ord_id from order_dispatch"
          " where status = 'Ordered')").show()

'''Note:
How Spark Executes This (Catalyst Optimizer):
Chat GPT Explained well-

🔍 Step-by-Step Execution Inside Spark
1️⃣ Logical Plan (What you wrote)

Spark first sees:

Join (type = LeftSemi)
  Left: order_details
  Right: filtered(order_details where status='Ordered')
Filter: status='Dispatched'

👉 Just a plan, not executed yet

2️⃣ Catalyst Optimization

Catalyst Optimiser now rewrites your plan smartly:

🔥 Optimization 1: Predicate Pushdown

👉 Moves filter early

Filter status='Dispatched' → applied BEFORE join

✔ Less data processed

🔥 Optimization 2: Column Pruning

👉 Keeps only needed columns

Left → only ord_id, status_date, status
Right → only ord_id

✔ Reduces memory

🔥 Optimization 3: Convert to Efficient Join Strategy

Spark may choose:

Broadcast Hash Join (if right side small)
Shuffle Hash Join
Sort Merge Join
💡 Example Optimized Plan
BroadcastHashJoin (LeftSemi)
  Left: Filter(status='Dispatched')
  Right: Broadcast(Filter(status='Ordered').select(ord_id))
🚀 3. Physical Plan (Actual Execution)

👉 Now Spark decides HOW to run:

Case 1: Small right table
BroadcastHashJoin

✔ Right table sent to all workers
✔ Very fast ⚡

Case 2: Large data
SortMergeJoin (LeftSemi)

✔ Both sides shuffled
✔ Sorted and matched

🔥 Key Insight (Very Important)

👉 In Semi Join:

Spark does NOT build full joined rows
It only checks: “Does a match exist?”'''