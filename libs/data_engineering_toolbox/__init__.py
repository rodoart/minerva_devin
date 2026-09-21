import os
import sys


sys.path.insert(0, os.path.join(os.environ['PYLIB'], 'py4j-0.10.9.5-src.zip'))
sys.path.insert(0, os.path.join(os.environ['PYLIB'], 'pyspark.zip'))
assert os.environ['HADOOP_CONF_DIR'], "HADOOP_CONF_DIR environment variable is not set"
