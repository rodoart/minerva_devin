# minerva_devin

Pipeline DataPull en PySpark + GraphFrames para generar features de grafo a
partir de transferencias CEPS (SPEI) y propagar un target de fraude (Lovelace)
hasta nivel `numcliente`, listo para `pyspark.ml` (`VectorAssembler`).

## Quickstart

```bash
conda env create -f environment.yaml && conda activate minerva_devin

# env vars requeridas (ver notebook.md/uso.md §1)
export MINERVA_TODAY=2025-08-31  # ... y el resto

python main.py                   # ejecuta el flujo completo (9 steps)
python main.py --build-only      # smoke test de la cadena sin ejecutar
python -m pytest tests/          # tests unitarios
```

## Estructura

- `main.py` — entry point con logging: valida env vars, crea la SparkSession
  remota y ejecuta la cadena completa (reanudable: cada etapa recarga su
  parquet/partición si ya existe).
- `run_order.py` — driver de desarrollo interactivo que encadena los steps.
- `config/` — dicts `input`/`output` por step (rutas Hive/HDFS, particiones,
  lags, history, schemas) + parámetros de features/propagación/assembler.
- `libs/framework` — framework de ETL reanudable (`Step`, decoradores
  `dynamic_*`, `cached_property`, helpers de substep).
- `libs/functions` — librerías importables: `missing_treatment`, `features`
  (grafo + propagación), `weights`, `assembly`.
- `libs/data_engineering_toolbox` — toolbox: paths HDFS (`HivePath`), contexto
  Spark, IO de particiones (`SparkLoad*`/`SparkTwoPartition*`), history,
  compare/dynamic/testing.
- `pipelines/` — clases `Standard*` genéricas + subclases por fuente
  (`ceps/`, `lovelace/`).
- `tests/` — suite pytest (markers `spark`, `graphframes`; se saltan solos si
  falta la dependencia).
- `environment.yaml` — entorno conda (spark 3.3.2, python 3.10, graphframes).
- `notebook.md/` — documentación: `concept_desing.md` (diseño),
  `uso.md` (**guía de uso completa: ejecución, config, extensión,
  troubleshooting**), `features.md` (**diccionario de features: definiciones y
  nombres finales de columna**), `estado_actual.md` (qué hace el proyecto),
  `arquitectura.md` (estructura y decisiones de refactor), `screenshots/`
  (capturas fuente del código).

Variables de entorno requeridas: `MINERVA_WORKSPACE_DIR_LINUX`, `PYSPARK_QUEUE`,
`PYSPARK_PORT`, `MINERVA_NAME`, `MINERVA_TODAY`, `MINERVA_VENV_TAR_GZ_LINUX`,
`MINERVA_VENV_TAR_GZ_HDFS`, `GRAPHFRAMES_JAR` (+ `HADOOP_CONF_DIR` para operaciones HDFS).
