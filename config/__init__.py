##########################################################################################
# PATH CONFIGURATION AND DECLARATION:
##########################################################################################

import os
import sys

from datetime import datetime

# Asegura que el root del proyecto esté en sys.path (imports tipo `libs.*`, `config.*`)
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)


# check if os.environ['RG49392_TODAY'] exists:
if 'RG49392_TODAY' not in os.environ:
    raise EnvironmentError("The environment variable 'RG49392_TODAY' is not set. Please set it before running the code.")

##########################################################################################
# CONFIGURATION AND PARAMETERS:
##########################################################################################
from .job import DATE_STANDARD_FORMAT # IMPORTANT: this import must be after the path configuration, otherwise it will fail because job.py won't be found
# check if os.environ['RG49392_TODAY'] is in yyyy-mm-dd format
try:
    datetime.strptime(os.environ['RG49392_TODAY'], DATE_STANDARD_FORMAT)
except ValueError:
    raise ValueError("The environment variable 'RG49392_TODAY' is not in the correct format. Please set it in 'yyyy-mm-dd' format.")

# check if pyspark lib is loaded
try:
    import pyspark  # noqa: F401
except ImportError:
    raise ImportError("The 'pyspark' library is not installed or not found in the PYTHONPATH. Please ensure it is installed and accessible.")
