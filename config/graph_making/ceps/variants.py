"""Variantes del grafo CEPS: coexistencia de grafos sobre la misma fuente.

Variantes conocidas (`GRAPH_VARIANTS`):
- "rfc": grafo RFC-RFC histórico. Los nodos son RFC/CURP imputados (fallback
  cta_<cta> / nom_<nom>) y el ensamblado final traduce a cta/numcliente vía
  los arrays de `group_by_id`. CONSERVA las rutas históricas en HDFS: no
  recomputa nada de lo ya persistido.
- "cta": grafo cta-cta. Los nodos son las cuentas en crudo (fallback
  nom_<nom>) y el numcliente se imputa al final con la tabla
  `mapeo_cta_numcliente` (salida del step txn_replacement, sustituible por
  una tabla externa cambiando la clave homónima del input de
  special_treatment). Sus rutas insertan el segmento "cta" tras el
  directorio del dataset (p.ej. `graph/ceps/cta/group_by/...`).

`ENABLED_VARIANTS` lista las cadenas que construye `main.py`; las clases de
pipeline son compartidas y solo cambian resolución de ids, columnas de
split y rutas (namespacing por variante).
"""
from typing import Tuple

GRAPH_VARIANTS = ["rfc", "cta"]      # variantes conocidas del grafo CEPS
ENABLED_VARIANTS = ["rfc", "cta"]    # cadenas que construye main.py
DEFAULT_VARIANT = "rfc"              # conserva las rutas históricas (cero recomputo)


def check_variant(variant: str) -> str:
    """Valida una variante de grafo y la devuelve; ValueError si es desconocida."""
    if variant not in GRAPH_VARIANTS:
        raise ValueError(
            f"Variante de grafo desconocida: {variant!r} "
            f"(conocidas: {GRAPH_VARIANTS})")
    return variant


def variant_subpath(base: str, variant: str, *segments: str) -> str:
    """Ruta namespaced por variante: `base/segments` para la variante por
    defecto; `base/<variant>/segments` para el resto.

    La variante por defecto ("rfc") NO inserta segmento: sus rutas quedan
    byte a byte como las históricas, de modo que todo lo ya persistido se
    reutiliza sin recomputar.
    """
    check_variant(variant)
    if variant == DEFAULT_VARIANT:
        return "/".join([base, *segments])
    return "/".join([base, variant, *segments])


def namespaced(base: str, variant: str, *segments: str) -> Tuple[str, str]:
    """Par (subpath, variante) ya validado; atajo para configs."""
    return variant_subpath(base, variant, *segments), variant
