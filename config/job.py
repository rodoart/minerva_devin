import os

from datetime import datetime
from dateutil.relativedelta import relativedelta

from libs.data_engineering_toolbox.path import HivePath

_today = os.environ['RG49392_TODAY']


# parameters
DATE_STANDARD_FORMAT = "%Y-%m-%d"
DATE_MONTH_FORMAT = "%Y%m"
DATE_STANDARD_SPARK_FORMAT = "yyyy-MM-dd"
DATE_MONTH_SPARK_FORMAT = "yyyyMM"
MAIN_LAG_IN_MONTHS = 1
COHORT="SBX"
IS_DYNAMIC = True


# date treatment
_today_date = datetime.strptime(os.environ['RG49392_TODAY'], DATE_STANDARD_FORMAT).date()
_current_month_date = _today_date - relativedelta(day=1) - relativedelta(months=MAIN_LAG_IN_MONTHS)
_first_day_of_next_month_date = _current_month_date + relativedelta(months=1)
_last_day_of_current_month_date = _first_day_of_next_month_date - relativedelta(days=1)
_process_date = _today_date
vintage = str(_current_month_date.strftime(DATE_MONTH_FORMAT))

date_treatment = {
"today_date_str": str(_today_date.strftime(DATE_STANDARD_FORMAT)),
"current_month_date_str": str(_current_month_date.strftime(DATE_STANDARD_FORMAT)),
"last_day_of_current_month_date_str": str(_last_day_of_current_month_date.strftime(DATE_STANDARD_FORMAT)),
"process_date_str": str(_process_date.strftime(DATE_STANDARD_FORMAT)),
"process_date": _process_date,
"vintage": vintage,
"vintage_date": _current_month_date
}


sbx = {
"general_root_hdfs": HivePath(f"/data/gcprcmsbx/work/hive/gcprcmsbx_work/rg49392/minerva/ceps_history/{vintage}"),
"main_lag_months": MAIN_LAG_IN_MONTHS,
"is_dynamic": IS_DYNAMIC,
"cohort": COHORT
}
