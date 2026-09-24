import re
import unicodedata
from typing import Union

import re
import unicodedata
from typing import Union

Sanitizable = Union[str, int, float, bool, None]


def sanitize_path_name(value: Sanitizable) -> str:
    """
    Convierte un valor arbitrario en un path seguro conservando la
    estructura de directorios.
    """
    if value is None:
        return "empty"

    value = str(value)

    # quitar acentos
    value = unicodedata.normalize("NFKD", value)
    value = value.encode("ascii", "ignore").decode("ascii")

    # conservar letras, números, _, -, ., /
    value = re.sub(r"[^a-zA-Z0-9_./-]+", "_", value)

    # limpiar repeticiones de /
    value = re.sub(r"/+", "/", value)

    # limpiar repeticiones de _
    value = re.sub(r"_+", "_", value)

    return value.strip("/")


def sanitize_property_name(value: Sanitizable) -> str:
    if value is None:
        return "empty"
    #
    value = str(value).lower()
    #
    value = unicodedata.normalize("NFKD", value)
    value = value.encode("ascii", "ignore").decode("ascii")
    #
    # solo letras, números y _
    value = re.sub(r"[^a-z0-9]+", "_", value)
    #
    value = re.sub(r"_+", "_", value)
    value = value.strip("_")
    #
    if value and value[0].isdigit():
        value = f"prop_{value}"
    #
    return value or "empty"


SYMBOL_REPLACEMENTS = {
    "+": "_plus_",
    "-": "_minus_",
    "*": "_times_",
    "/": "_div_",
    "%": "_pct_",
    "=": "_eq_",
    "!=": "_neq_",
    ">=": "_gte_",
    "<=": "_lte_",
    ">": "_gt_",
    "<": "_lt_",
    "&": "_and_",
    "|": "_or_",
    "@": "_at_",
    "$": "_dollar_",
    "#": "_num_",
    ".": "_dot_",
}


def sanitize_column_name(value: Sanitizable) -> str:
    if value is None:
        return "empty"

    if isinstance(value, bool):
        value = str(value).lower()
    else:
        value = str(value)

    # quitar acentos
    value = unicodedata.normalize("NFKD", value)
    value = value.encode("ascii", "ignore").decode("ascii")
    #
    value = value.lower()
    #
    # operadores largos primero
    for old in ("!=", ">=", "<="):
        value = value.replace(old, SYMBOL_REPLACEMENTS[old])

    # luego operadores de un caracter
    for old, new in SYMBOL_REPLACEMENTS.items():
        if old not in ("!=", ">=", "<="):
            value = value.replace(old, new)

    # resto de caracteres raros
    value = re.sub(r"[^a-z0-9_]+", "_", value)
    #
    # colapsar _
    value = re.sub(r"_+", "_", value)
    #
    value = value.strip("_")
    #
    # evitar empezar con número
    if value and value[0].isdigit():
        value = f"col_{value}"

    return value or "empty"
