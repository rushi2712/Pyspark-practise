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

#Spark Setup
spark = SparkSession.builder.appName("Scenario-4").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("\nScenario-4: We need to identify the unique customer names in the table along with the number of addresses associated with each customer")

cust_data = [(1, "Mark Ray", "Miyapur"),
             (2, "Peter Smith", "Ameerpet"),
             (1, "Mark Ray", "Saroor Nagar"),
             (2, "Peter Smith", "Erragadda"),
             (2, "Peter Smith", "Ameerpet"),
             (3, "Katy Perry", "Kukatpally")]
columns = ["cust_id", "name", "address"]
cust_data_df = spark.createDataFrame(cust_data, columns)
print("The customer data DF is:")
cust_data_df.show()

print("The unique customer names along with addresses")
print("Using Spark SQL")
cust_data_df.createOrReplaceTempView("cust_name_addr")
spark.sql(" select cust_id, name, "
          " collect_set(address) as address,"
          " size(collect_set(address)) as no_of_address_cnt"
          " from cust_name_addr"
          " group by cust_id, name"
          " order by cust_id").show(truncate=False)

print('Using Spark DSL')
cust_data_df_final = (cust_data_df.groupBy("cust_id", "name")
                      .agg(collect_set("address").alias("address"))
                      .withColumn("no_of_address_cnt", size("address"))
                      .orderBy("cust_id")
                      )
cust_data_df_final.show(truncate=False)

'''Things I've learned, here:
1. To find the values belonging to a name for ex: here, we see that we need to find the addresses of the customers, right, we need to used "collect_set()"
2. There's one more concept called "collect_list()". 
The difference between both is, "collect_set()" doesn't show repetitive values, where as "collect_list()" shows the duplicate values also
3. To find the count of the collect_set() or collect-list() values, we use size() keyword

4. One more difference to notice is in the SQL, we cannot use the alias names in the same query, where as in DSL, we can.
This is because the SQL execution order is like this: FROM -> WHERE -> GROUP BY -> HAVING -> SELECT -> ORDER BY
Alias is created at the end of the SELECT. That's why we can't use the same alias.
In DSL, Each step creates a new DataFrame. So, alias column exists before you use it
'''