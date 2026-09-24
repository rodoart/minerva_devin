##########################################################################################
# PATH CONFIGURATION AND DECLARATION:
##########################################################################################

import os
import sys

from datetime import datetime

# Asegura que el root del proyecto esté en sys.path (imports tipo `libs.*`, `config.*`)
current_dir = os.path.dirname(os.path.abspath(__file__))  # directorio de este fichero (.../config)
parent_dir = os.path.dirname(current_dir)                 # raíz del repositorio

if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)


# check if os.environ['MINERVA_TODAY'] exists:
# MINERVA_TODAY = fecha de ejecución del job ("hoy"); de ella derivan el
# vintage (mes de corte de los datos) y la process_date de todas las salidas.
if 'MINERVA_TODAY' not in os.environ:
    raise EnvironmentError("The environment variable 'MINERVA_TODAY' is not set. Please set it before running the code.")

##########################################################################################
# CONFIGURATION AND PARAMETERS:
##########################################################################################
from .job import DATE_STANDARD_FORMAT # IMPORTANT: this import must be after the path configuration, otherwise it will fail because job.py won't be found
# check if os.environ['MINERVA_TODAY'] is in yyyy-mm-dd format
# (se valida parseándola con DATE_STANDARD_FORMAT="%Y-%m-%d", definido en job.py)
try:
    datetime.strptime(os.environ['MINERVA_TODAY'], DATE_STANDARD_FORMAT)
except ValueError:
    raise ValueError("The environment variable 'MINERVA_TODAY' is not in the correct format. Please set it in 'yyyy-mm-dd' format.")

# check if pyspark lib is loaded
# (fail-fast en el driver: todo el pipeline depende de pyspark)
try:
    import pyspark  # noqa: F401
except ImportError:
    raise ImportError("The 'pyspark' library is not installed or not found in the PYTHONPATH. Please ensure it is installed and accessible.")
