import re
import unicodedata
from typing import Union



def sanitize_name(value: Union[str, int, float, bool, None]) -> str:
    #
    if value is None:
        return "empty"
    if isinstance(value, bool):
        value = str(value).lower()
    else:
        value = str(value)
    #
    # quitar acentos
    value = unicodedata.normalize("NFKD", value)
    value = value.encode("ascii", "ignore").decode("ascii")
    #
    # reemplazar caracteres no permitidos
    value = re.sub(r"[^a-zA-Z0-9_-]+", "_", value)
    #
    # colapsar múltiples _
    value = re.sub(r"_+", "_", value)
    #
    # quitar _ al inicio y final
    value = value.strip("_")
    #
    return value or "empty"
