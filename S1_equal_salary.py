
import os
import urllib.request
import ssl

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
#import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://raw.githubusercontent.com/saiadityaus1/test1/refs/heads/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://github.com/saiadityaus1/SparkCore1/raw/refs/heads/master/data.orc": "data.orc", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/refs/heads/master/rm.json": "rm.json"}.items()]

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os
import urllib.request
import ssl

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ["HADOOP_HOME"] = r"C:\hadoop"
#os.environ['HADOOP_HOME'] = hadoop_home
os.environ['JAVA_HOME'] = r'C:\Users\1767\.jdks\corretto-1.8.0_462'        #  <----- 🔴JAVA PATH🔴
# Set HADOOP_HOME to the folder ABOVE the bin folder
# Add the bin folder to your System Path
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
#sc = SparkContext(conf=conf)

#spark = SparkSession.builder.appName("FIFA Pipeline").getOrCreate()
# 🔥 Important setting
#spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
#Enabling dynamic partition overwrite using Spark config. So, only affected partitions are updated instead of rewriting the entire dataset.
#THis will be useful while REWRITING(re-saving) by partitions after transformations. Without this, whenever we re-write the entire transformations,
#the partitioned folders will be created again and again at the destination path, thus results in more cost, concurrent issues, and any other risks while dealing with huge data
#=============================================================#
#Write code from below

print("Scenario 1: Query to get who are getting equal salary")


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Scenario-1").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data = [(1, "Monika", "Arora", 100000, "2014-02-20 09:00:00", "HR"),
        (2, "Niharika", "Deol", 300000, "2014-06-11 09:00:00", "Admin"),
        (3, "Vishal", "Varma", 300000, "2014-02-20 09:00:00", "HR"),
        (4, "Amithabh", "Singh", 500000, "2014-02-20 09:00:00", "Admin"),
        (5, "Vivek", "Deva", 500000, "2014-06-11 09:00:00", "Admin")]
columns = ("id", "first_name", "last_name", "salary", "joining_date", "role")
data_df = spark.createDataFrame(data, columns)
print("The employee DF is")
data_df.show()
data_df.printSchema()

print("The employees with the same salary are")
print("Using Spark DSL")
#Here, we shall use self-join as we have to compare the same column values
data_df_eq_sal = (data_df.alias("A").join(data_df.alias("B"), (col("A.salary") == col("B.salary")) & (col("A.id") != col("B.id")), 'inner')
                  .select("A.id", "A.first_name", "A.last_name", "A.salary", "A.joining_date", "A.role"))
data_df_eq_sal.show()

print("Using Spark SQL")
data_df.createOrReplaceTempView("equal_salary")
spark.sql("select eq1.id, eq1.first_name, eq1.last_name, eq1.salary, eq1.joining_date, eq1.role"
          " from equal_salary as eq1"
          " join equal_salary as eq2 on eq1.salary = eq2.salary"
          " where eq1.id != eq2.id"
          " order by eq1.id").show()