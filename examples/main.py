#!/usr/bin/env python3
"""main.py de ejemplo — punto de entrada del pipeline de transacciones.

Encadena los steps con `previous_step` y ejecuta el último: `Step.execute()`
resuelve recursivamente la cadena (reanudable: cada etapa recarga su
parquet/partición si ya existe).

Uso:
    python examples/main.py [--local] [--build-only]
                          [--cleanup | --cleanup-only]

`--local` crea una SparkSession local[2] en vez de la sesión YARN del cluster
(la lectura/escritura sigue yendo a las rutas `table_or_hdfs` configuradas —
con un fs local si se apuntan a /tmp o file://).
"""
import argparse
import os
import sys

# Asegura que el root del repo esté en sys.path (imports `libs.*`, `examples.*`).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from libs.data_engineering_toolbox.context.logging import get_logger, setup_logging
logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ejecuta el pipeline de ejemplo del framework.")
    parser.add_argument("--local", action="store_true",
        help="SparkSession local[2] en vez de YARN")
    parser.add_argument("--build-only", action="store_true",
        help="Construye la cadena de steps sin ejecutarla")
    parser.add_argument("--cleanup", action="store_true",
        help="Tras ejecutar, borra los parquets intermedios")
    parser.add_argument("--cleanup-only", action="store_true",
        help="Solo borra los parquets intermedios")
    return parser.parse_args()


def build_spark(local: bool):
    """SparkSession del cluster (env PYSPARK_*) o local con --local."""
    from libs.data_engineering_toolbox.context import SparkSessionBuilder
    if local:
        os.environ.setdefault("PYLIB", "/dev/null")
        return SparkSessionBuilder(master="local[2]").build()
    return SparkSessionBuilder().build()


def build_pipeline(spark):
    """Construye la cadena extract -> group_by y devuelve el último step."""
    from examples.config import job as cj
    from examples.config import transactions_etl as cfg
    from examples.pipelines.transactions_etl import (
        ExampleExtractStep, ExampleGroupByStep)

    extract_step = ExampleExtractStep(
        date_treatment=cj.date_treatment,
        input_hive=cfg.extract_input,
        output_hive=cfg.extract_output,
        cohort=cj.COHORT,
        is_dynamic=cj.IS_DYNAMIC,
        sqlContext=spark,
    )
    group_by_step = ExampleGroupByStep(
        date_treatment=cj.date_treatment,
        input_hive=cfg.group_by_input,
        output_hive=cfg.group_by_output,
        cohort=cj.COHORT,
        is_dynamic=cj.IS_DYNAMIC,
        sqlContext=spark,
        previous_step=[extract_step],
    )
    return group_by_step


def main() -> int:
    args = parse_args()
    setup_logging(level="INFO", force_reload=True)

    spark = build_spark(local=args.local)
    last_step = build_pipeline(spark)
    if args.build_only:
        logger.info("--build-only: cadena construida, no se ejecuta")
        return 0
    if args.cleanup_only:
        last_step.delete_tmp_paths()
        return 0
    last_step.execute()
    if args.cleanup:
        last_step.delete_tmp_paths()
    return 0


if __name__ == "__main__":
    sys.exit(main())
