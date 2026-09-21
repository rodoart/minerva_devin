import random
from typing import Any, Callable


def cached_property(func: Callable[[Any], Any]) -> property:
    """
    Decorator to cache the value of a property.

    This decorator can be used to cache the result of a property method,
    so that the method is only called once and the result is stored for
    future access. This can improve performance by avoiding repeated
    calculations.

    Args:
        func (Callable[[Any], Any]): The property method to be cached.

    Returns:
        property: A property that caches its value after the first access.
    """
    attr_name = f"_{func.__name__}"

    @property
    def cached(self: Any) -> Any:
        if not hasattr(self, attr_name):
            setattr(self, attr_name, func(self))
        return getattr(self, attr_name)

    return cached


def random_table_name() -> str:
    """
    Genera un nombre de tabla aleatorio.

    Esta función genera un nombre de tabla con
    el prefijo 'tmp_rg49392_' seguido de una cadena de caracteres aleatorios
    en minúsculas. Esto puede ser útil para los nombres
    temporales en pruebas o entornos de
    desarrollo.

    Returns:
        str: Un nombre de tabla aleatorio.
    """
    # Generar una cadena de 10 caracteres aleatorios en minúsculas
    random_suffix: str = "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=10))
    # Concatenar el prefijo con la cadena aleatoria
    table_name: str = "tmp_rg49392_" + random_suffix
    return table_name
