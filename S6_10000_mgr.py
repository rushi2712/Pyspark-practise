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
spark = SparkSession.builder.appName("Scenario-6").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("Scenario-6: We need to mark the employees whose salary is > 100000, as 'Manager'")

employees = [(1, "A", 100000),
             (2, "B", 50000),
             (3, "C", 15000),
             (4, "D", 25000),
             (5, "E", 50000),
             (6, "F", 100000)]
columns = ("emp_id", "emp_name", "emp_sal")
emp_df = spark.createDataFrame(employees, columns)
emp_df.show()

print("Marking the employees whose salary is >= 100000, as 'Managers' and >= 50000 as 'Leads'")
print("\nBy using Spark SQL")
emp_df.createOrReplaceTempView("manager_100000")
spark.sql("""
select *, 
        case 
            when emp_sal >= 100000 then 'Manager'
            when emp_sal >= 50000 then 'Team lead'
            else 'Employee'
        end as Designation
from manager_100000
order by emp_sal desc
""").show()

print("By using Spark DSL")
emp_df_manager = (emp_df.withColumn("Designation", expr("case when emp_sal >= 100000 then 'Manager' when emp_sal >= 50000 then 'Team Lead' else 'Employee' end"))
                  .orderBy(desc("emp_sal")))
emp_df_manager.show()

'''
This is an easy scenario.
usage of CASE WHEN THEN ELSE END is the syntax. 
'''