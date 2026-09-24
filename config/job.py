import os

from datetime import datetime
from dateutil.relativedelta import relativedelta

from libs.data_engineering_toolbox.path import HivePath

_today = os.environ['MINERVA_TODAY']  # fecha de ejecución del job en formato yyyy-mm-dd (validada en config/__init__.py)


# parameters
DATE_STANDARD_FORMAT = "%Y-%m-%d"          # formato de fecha completa para datetime de Python (strptime/strftime)
DATE_MONTH_FORMAT = "%Y%m"                 # formato de mes para datetime de Python; define el vintage (p.ej. "202507")
DATE_STANDARD_SPARK_FORMAT = "yyyy-MM-dd"  # equivalente Spark (to_date/date_format) del formato de fecha completa
DATE_MONTH_SPARK_FORMAT = "yyyyMM"         # equivalente Spark del formato de mes; formato de la columna-partición mis_date
MAIN_LAG_IN_MONTHS = 1                     # lag principal del job: el vintage cae 1 mes atrás de hoy (los datos se cierran a mes vencido)
COHORT="SBX"                               # cohorte/entorno de ejecución (sandbox); se escribe como literal en la columna "cohort" de las salidas
IS_DYNAMIC = True                          # modo dinámico: los steps reutilizan parquets/particiones ya persistidos (pipeline reanudable); False = recomputar siempre


# date treatment
_today_date = datetime.strptime(os.environ['MINERVA_TODAY'], DATE_STANDARD_FORMAT).date()  # "hoy" como objeto date
_current_month_date = _today_date - relativedelta(day=1) - relativedelta(months=MAIN_LAG_IN_MONTHS)  # día 1 del mes vintage (mes objetivo = hoy - MAIN_LAG_IN_MONTHS)
_first_day_of_next_month_date = _current_month_date + relativedelta(months=1)  # primer día del mes siguiente al vintage (cota superior exclusiva del periodo)
_last_day_of_current_month_date = _first_day_of_next_month_date - relativedelta(days=1)  # último día del mes vintage (cierre del periodo de datos)
_process_date = _today_date                # fecha de proceso/carga = día de ejecución; alimenta la partición process_date
vintage = str(_current_month_date.strftime(DATE_MONTH_FORMAT))  # etiqueta del corte mensual en formato yyyymm

date_treatment = {  # paquete de fechas que cada Step recibe como input_parameters (ver build_step_init_kwargs en libs/framework)
"today_date_str": str(_today_date.strftime(DATE_STANDARD_FORMAT)),                           # hoy, como string yyyy-mm-dd
"current_month_date_str": str(_current_month_date.strftime(DATE_STANDARD_FORMAT)),           # día 1 del mes vintage, como string
"last_day_of_current_month_date_str": str(_last_day_of_current_month_date.strftime(DATE_STANDARD_FORMAT)),  # último día del mes vintage, como string
"process_date_str": str(_process_date.strftime(DATE_STANDARD_FORMAT)),                       # fecha de proceso como string (se escribe como literal en los DataFrames)
"process_date": _process_date,                                                               # fecha de proceso como date (partición process_date de los outputs)
"vintage": vintage,                                                                          # etiqueta del corte mensual yyyymm (columna "vintage" y nombre de la carpeta raíz)
"vintage_date": _current_month_date                                                          # fecha ancla del vintage; base sobre la que se aplican lag/history al resolver particiones
}


sbx = {  # configuración del job para la cohorte SBX; los módulos config.* la importan como job_config
"general_root_hdfs": HivePath(f"/data/gcprcmsbx/work/hive/gcprcmsbx_work/rg49392/minerva/ceps_history/{vintage}"),  # raíz HDFS de TODAS las salidas del pipeline para este vintage
"main_lag_months": MAIN_LAG_IN_MONTHS,  # lag principal en meses (meses de desfase entre ejecución y corte de datos)
"is_dynamic": IS_DYNAMIC,               # flag de modo dinámico: reutilizar outputs persistidos vs recomputar
"cohort": COHORT                        # cohorte/entorno de ejecución
}
