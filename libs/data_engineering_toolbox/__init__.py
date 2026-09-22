import logging
import os
import sys

logger = logging.getLogger(__name__)

# Solo en el cluster: PYLIB apunta a los zips de pyspark/py4j del entorno.
if "PYLIB" in os.environ:
    sys.path.insert(0, os.path.join(os.environ["PYLIB"], "py4j-0.10.9.5-src.zip"))
    sys.path.insert(0, os.path.join(os.environ["PYLIB"], "pyspark.zip"))

if "HADOOP_CONF_DIR" not in os.environ:
    logger.warning(
        "HADOOP_CONF_DIR environment variable is not set: "
        "las operaciones HDFS (path.HivePath.*) no funcionaran."
    )
