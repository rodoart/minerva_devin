
from pyspark.sql.types import StructType


def assertSchemasEqual(schema1: StructType, schema2: StructType) -> None:
    """Compara dos esquemas de DataFrames para verificar si son iguales,
                                                      independientemente del orden de las columnas.

    Args:
        schema1 (StructType): Primer esquema.
        schema2 (StructType): Segundo esquema.

    Raises:
        AssertionError: Si los esquemas no son iguales.
    """
    # Convertir los esquemas a conjuntos de tuplas (nombre, tipo)
    schema1_set = {(field.name, field.dataType) for field in schema1}
    schema2_set = {(field.name, field.dataType) for field in schema2}
    #
    # Comparar los conjuntos
    if schema1_set != schema2_set:
        raise AssertionError(f"Esquemas diferentes: {schema1} != {schema2}")
