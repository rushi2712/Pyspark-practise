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

spark = SparkSession.builder.appName("Scenario-7").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", 5)

print("\nScenario-8: We need to schedule a tournament among the 5 teams such that each team plays one match with opponents")

teams = [("India",),
         ("Srilanka",),
         ("Pakistan",),
         ("Bangladesh",),
         ("Afghanistan",)]
columns = ["countries"]
teams_df = spark.createDataFrame(teams, columns)

print("We need to schedule a tournament among the 5 teams")
print("By using Spark SQL")
teams_df.createOrReplaceTempView("asia_cup")
spark.sql("""
select concat(A.countries, ' Vs ', B.countries) as fixtures
from asia_cup as A
join asia_cup as B on A.countries < B.countries;
""").show(truncate=False)

print("By using Spark DSL")
teams_df_1 = teams_df.alias('A').join(teams_df.alias('B'), col("A.countries") < col("B.countries"), "inner")
teams_df_final = teams_df_1.withColumn("fixtures", expr("concat(A.countries, ' Vs ', B.countries)"))
teams_df_final.drop("countries").show(truncate=False)


"""
Things I learned:
1. Shuffling is the costliest thing in Spark as it demands data movement between the machines among a cluster. 
It also writes the intermediate data to the disk, and reads back from it which is time consuming.
As the data is serialised and deserialised, CPU overhead increases.
Shuffling requires sorting.
"""