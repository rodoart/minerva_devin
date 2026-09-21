
from re import search
from pyspark.sql import Column



def get_column_alias(column: Column) -> str:
    """Extrae el alias declarado de una `Column` de PySpark.

    Parsea la representación interna (`Column._jc.toString()`) buscando el patrón
    `AS <alias>` que Spark genera al aplicar `.alias(...)`. Si la columna no tiene
    alias explícito, retorna su nombre/expresión tal cual.

    Parameters
    ----------
    column : Column
        Columna de la que extraer el alias (ej: `col("id_ban_ben").alias("id_ban_dst")`).

    Returns
    -------
    str
        El alias declarado (ej: "id_ban_dst"), o la expresión base si no hay alias.

    Examples
    --------
    >>> get_column_alias(col("id_ban_ben").alias("id_ban_dst"))
    'id_ban_dst'
    >>> get_column_alias(col("customer_id"))
    'customer_id'
    """
    expr = column._jc.toString()
    match = search(r" AS (\w+)$", expr)
    return match.group(1) if match else expr
