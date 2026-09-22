"""
Librerías de funciones reutilizables del pipeline.

- `missing_treatment`: funciones de imputación de nulos y su registro/dispatcher.
- `features`: funciones de features de grafo (GraphFrame -> DataFrame) y de
  propagación de target, más el catálogo de stats para aristas ponderadas.
- `weights`: constructores de columnas de peso y helpers del mapa `weights`.
"""
from . import missing_treatment
from . import features
from . import weights
