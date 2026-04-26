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

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

#Spark Setup
spark = SparkSession.builder.appName("Scenario-3").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("\nScenario 3: We need to find for each sensor, compute how much the value has changed compared to the next reading in time")

sensor = [(111, "2021-01-15", 10),
          (111, "2021-01-16", 15),
          (111, "2021-01-17", 30),
          (112, "2021-01-15", 10),
          (112, "2021-01-15", 20),
          (112, "2021-01-15", 30)
          ]
columns = ["sensor_id", "timestamp", "values"]
sensor_df = spark.createDataFrame(sensor, columns)
sensor_df.show()

print("\nhow much the values have changed compared to the next reading in time")
print("Using Spark DSL")
window = Window.partitionBy("sensor_id").orderBy("timestamp")
sensor_df_new = (sensor_df.withColumn("new_column", lead("values").over(window))
                            .filter(col("new_column").isNotNull())
                            .withColumn("values", expr("new_column - values"))
                            .orderBy("values")
                            .select("sensor_id", "timestamp", "values"))
sensor_df_new.show()

print("Using Spark SQL")
sensor_df.createOrReplaceTempView("sensor_df_sql")
spark.sql(" with cte as ("
          " select *, "
          "         lead(values) over (partition by sensor_id order by timestamp) as new_values"
          " from sensor_df_sql)"
          " select sensor_id, timestamp, (new_values - values) as value_diff"
          " from cte"
          " where (new_values - values) is Not Null"
          " order by sensor_id, (new_values - values)").show()