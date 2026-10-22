"""Build-time helper: resolve extra Spark packages with Ivy and copy them into pyspark/jars.

Jars whose artifact already ships with pyspark are skipped, so we never end up with two
versions of the same library on the classpath.
"""

import glob
import os
import re
import shutil

import pyspark
from pyspark.sql import SparkSession

PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
    "io.delta:delta-spark_2.13:4.4.1",
    "org.apache.hadoop:hadoop-aws:3.5.0",
    # Delta pulls an old jackson-datatype-jdk8 (2.13); align it with Spark's Jackson (2.21).
    "com.fasterxml.jackson.datatype:jackson-datatype-jdk8:2.21.2",
]
IVY_DIR = "/tmp/ivy"


def artifact(filename: str) -> str:
    """'org.apache.kafka_kafka-clients-3.9.1.jar' or 'kafka-clients-3.9.1.jar' -> 'kafka-clients'."""
    name = os.path.basename(filename).removesuffix(".jar")
    if "_" in name and "." in name.split("_", 1)[0]:     # Ivy prefixes the groupId
        name = name.split("_", 1)[1]
    return re.sub(r"-\d[\w.\-]*$", "", name)


spark = (SparkSession.builder.master("local[1]")
         .config("spark.jars.packages", ",".join(PACKAGES))
         .config("spark.jars.ivy", IVY_DIR)
         .getOrCreate())
spark.stop()

target = os.path.join(os.path.dirname(pyspark.__file__), "jars")
present = {artifact(j) for j in glob.glob(f"{target}/*.jar")}
for jar in sorted(glob.glob(f"{IVY_DIR}/jars/*.jar")):
    if artifact(jar) in present:
        print(f"skip (already in pyspark): {os.path.basename(jar)}")
        continue
    shutil.copy(jar, target)
    print(f"added: {os.path.basename(jar)}")
