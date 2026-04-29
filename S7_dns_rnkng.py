import os
import urllib.request
import ssl
import sys

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

#Create an unverified SSL context
ssl_context = ssl._create_unverified_context()

for url, path in urls_and_paths.items():
    #Use the unverified context with urlopen
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

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Scenario-7").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("\nScenario-7: We need to find the top sales based on quantities for each year")

sales = [(1, 100, 2010, 25, 5000),
         (2, 100, 2011, 16, 5000),
         (3, 100, 2012, 8, 5000),
         (4, 200, 2010, 10, 9000),
         (5, 200, 2011, 15, 9000),
         (6, 200, 2012, 20, 7000),
         (7, 300, 2010, 20, 7000),
         (8, 300, 2011, 18, 7000),
         (9, 300, 2012, 20, 7000),
         ]
columns = ("sale_id", "product_id", "year", "quantity", "price")
sales_df = spark.createDataFrame(sales, columns)
sales_df.show()

print("We need to find the top sales based on quantities for each year")
print("By using Spark SQL")
sales_df.createOrReplaceTempView("sales_table")
spark.sql("""
with cte as (
select *, dense_rank() over (partition by year order by quantity desc) as dnsrnk
from sales_table
order by year desc
)

select sale_id, product_id, year, quantity, price
from cte
where dnsrnk = 1;
""").show()

print("By using Spark DSL")
sales_df_1 = (sales_df.withColumn("dnsrnk", dense_rank().over(Window.partitionBy("year").orderBy(col("quantity").desc())))
              .filter(col("dnsrnk") == 1)
              .drop("dnsrnk")
              .orderBy(col("year").desc()))
sales_df_1.show()

'''
This is classic Window - Dense Rank concept where we need to identify the top sales per year.
Remember, in DSLs, while we are performing certain operations on any columns, we need to use 'col("column_name")'. 
If we dont perform any operations on the columns, then just "column_name" is enough.
'''