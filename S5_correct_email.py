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

#Spark Libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#Spark Setup
spark = SparkSession.builder.appName("Scenario-5").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("\nScenario-5: we need to combine two table values, and write only the correct mail id values to the result table")

table_1 = [(1, "abc", 31, "abc@gmail.com"),
           (2, "def", 23, "defyahoo.com"),
           (3, "xyz", 26, "xyz@gmail.com"),
           (4, "qwe", 34, "qwegmail.com"),
           (5, "iop", 24, "iop@gmail.com"),
           (6, "zxc", 30, "zxc.com")
           ]
table_1_columns = ["id", "name", "age", "email"]
df_1 = spark.createDataFrame(table_1, table_1_columns)

table_2 = [
    (11, "jkl", 22, "abc@gmail.com", 1000),
    (12, "vbn", 33, "vbn@yahoo.com", 3000),
    (13, "wer", 27, "wer", 2000),
    (14, "zxc", 30, "zxc.com", 2000),
    (15, "lkj", 29, "lkj@outlook.com", 2000)
]
table_2_columns = ["id", "name", "age", "email", "salary"]
df_2 = spark.createDataFrame(table_2, table_2_columns)

#We have a set of instructions to follow:
#1. To find the number of partitions in the table_1
no_of_partitions = df_1.rdd.getNumPartitions()
print("number of partitions in table_1: " + str(no_of_partitions))

#2. Create a new dataframe df1 from table_1_df, along with a new column "salary", and keep it constant 1000
print("Create a new dataframe df1 from table_1_df, along with a new column 'salary', and keep it constant 1000")
df_3 = df_1.withColumn("salary", lit(1000))
df_3.show()

#3. Append df_2, df_3 and form df_4
print("Append df_2, df_3 and form df_4")
df_4 = df_2.union(df_3).orderBy("id")
df_4.show()

#4. Remove records which have invalid email from df4, emails with @ are considered to be valid.
print("Remove records which have invalid email")
df_5 = df_4.filter(col("email").rlike("@"))
df_5.show()

#5. Write df4 to a target location, by partitioning on salary.
print("Writing the result to dest folder")
df_5.write.mode("overwrite").partitionBy("salary").parquet(r"D:\Users\1767\Desktop\Cloud Data Engineering\temp")
print("Writing completed!")