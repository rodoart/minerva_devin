"""job.py de ejemplo — parámetros de fecha y rutas del job.

En un proyecto real este módulo deriva el *vintage* (mes de corte) de la fecha
de ejecución (`APP_TODAY`) y define la raíz HDFS donde se persisten todas las
salidas. El framework solo exige que `date_treatment` contenga las claves que
`libs.framework.build_step_init_kwargs` consume.
"""
import os

from datetime import datetime
from dateutil.relativedelta import relativedelta

from libs.data_engineering_toolbox.path import HivePath
from libs.data_engineering_toolbox.general.date_treatment import (
    DATE_STANDARD_FORMAT, DATE_MONTH_FORMAT)

# Fecha de ejecución del job en formato yyyy-mm-dd (en producción: APP_TODAY).
_today = os.environ.get("APP_TODAY") or datetime.today().strftime(DATE_STANDARD_FORMAT)

# parameters
MAIN_LAG_IN_MONTHS = 1   # lag principal: el vintage cae 1 mes atrás de hoy
COHORT = "SBX"           # cohorte/entorno; se escribe como literal en las salidas
IS_DYNAMIC = True        # modo dinámico: recargar outputs persistidos (pipeline reanudable)


# date treatment
_today_date = datetime.strptime(_today, DATE_STANDARD_FORMAT).date()
_current_month_date = _today_date - relativedelta(day=1) - relativedelta(months=MAIN_LAG_IN_MONTHS)
_first_day_of_next_month_date = _current_month_date + relativedelta(months=1)
_last_day_of_current_month_date = _first_day_of_next_month_date - relativedelta(days=1)
_process_date = _today_date
vintage = str(_current_month_date.strftime(DATE_MONTH_FORMAT))

date_treatment = {  # paquete de fechas que cada Step recibe como input_parameters
"today_date_str": str(_today_date.strftime(DATE_STANDARD_FORMAT)),
"current_month_date_str": str(_current_month_date.strftime(DATE_STANDARD_FORMAT)),
"last_day_of_current_month_date_str": str(_last_day_of_current_month_date.strftime(DATE_STANDARD_FORMAT)),
"process_date_str": str(_process_date.strftime(DATE_STANDARD_FORMAT)),
"process_date": _process_date,
"vintage": vintage,
"vintage_date": _current_month_date
}


job = {  # configuración del job; los módulos config.* la importan como job_config
"general_root_hdfs": HivePath(
    os.environ.get("APP_OUTPUT_ROOT_HDFS", f"/tmp/framework_example/{vintage}")),
"main_lag_months": MAIN_LAG_IN_MONTHS,
"is_dynamic": IS_DYNAMIC,
"cohort": COHORT
}
