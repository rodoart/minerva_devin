from typing import Tuple
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from typing import Union


_YYYYMM_FORMAT = '%Y%m'

def vintage_yyyymm_to_date(yyyymm:str) -> datetime:
    return datetime.strptime(yyyymm, _YYYYMM_FORMAT).date()

def vintage_yyyymm_to_yy_and_mm(yyyymm:str)->Tuple[str]:
    date_object = vintage_yyyymm_to_date(yyyymm)
    return date_object.strftime('%y'), date_object.strftime('%m')

def vintage_yyyymm_plus_months(yyyymm:str, months:Union[str, int]) -> str:
    if isinstance(months, str):
        months = int(months)

    date_object = vintage_yyyymm_to_date(yyyymm) + relativedelta(months=months)
    return date_object.strftime(_YYYYMM_FORMAT)

def get_last_day_of_the_month(date_:date) -> date:
    """
    Obtiene el último día del mes actual utilizando relativedelta.

    Returns:
        date: Fecha correspondiente al último día del mes actual.
    """
    today = date_
    last_day_of_month = today + relativedelta(day=31)
    return last_day_of_month

def get_first_day_of_the_month(date_: date) -> date:
    """
    Obtiene el primer día del mes actual utilizando relativedelta.

    Args:
        date_ (date): Fecha de referencia.

    Returns:
        date: Fecha correspondiente al primer día del mes actual.
    """
    first_day_of_month = date_ + relativedelta(day=1)
    return first_day_of_month

def make_date_interval_months(current_date:date, history:int) -> Tuple[date, date]:
    """
    Crea un intervalo de fechas basado en la fecha actual y el historial en meses.
    #
    Args:
        current_date (date): La fecha actual a partir de la cual se calculará el intervalo.
        history (int): El historial en meses hacia atrás o hacia adelante.
    #
    Returns:
        Tuple[date, date]: Una tupla con la fecha de inicio y la fecha de fin del intervalo calculado.
    """
    assert history != 0, "History must be different than 0"
    start_of_current_month = get_first_day_of_the_month(current_date)
    end_of_current_month = get_last_day_of_the_month(current_date)
    if history > 0:
        end_of_the_interval = end_of_current_month + relativedelta(days=1)
        start_of_the_interval = start_of_current_month - relativedelta(months=history-1)
    elif history == -1:
        end_of_the_interval = end_of_current_month + relativedelta(days=1)
        start_of_the_interval = start_of_current_month
    else:
        start_of_the_interval = start_of_current_month
        end_of_the_interval = start_of_current_month + relativedelta(months=-history)
    #
    return (start_of_the_interval, end_of_the_interval)

def make_date_interval_with_lag_months(current_date:date, history:int, lag:int) -> Tuple[date, date]:
    """
    Crea un intervalo de fechas basado en una fecha actual, un historial de meses y un retraso en meses.
    #
    Args:
        current_date (date): La fecha actual desde la cual se calculará el intervalo.
        history (int): El número de meses en el pasado que se incluirán en el intervalo.
        lag (int): El número de meses de retraso que se aplicará al intervalo.
    #
    Returns:
        Tuple[date, date]: Una tupla que contiene la fecha de inicio y la fecha de fin del intervalo.
    #
    Ejemplo:
        current_date = date(2023, 12, 31)
        history = 6
        lag = 2
    #
        El resultado será un intervalo de fechas que comienza 8 meses antes de la fecha actual (6 meses de historial + 2 meses de retraso)
        y termina 2 meses antes de la fecha actual.

        make_date_interval_with_lag_months(current_date, history, lag)
        # Resultado: (date(2023, 4, 30), date(2023, 10, 31))
    """
    current_date = current_date - relativedelta(months=lag)
    return make_date_interval_months(current_date, history)
