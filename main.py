#!/usr/bin/env python3
"""main.py — Punto de entrada del pipeline Minerva.

Ejecuta el flujo completo de extracción, features de grafo, propagación de
target y VectorAssembler a nivel numcliente.

Requiere las variables de entorno:
    MINERVA_WORKSPACE_DIR_LINUX, PYSPARK_QUEUE, PYSPARK_PORT, MINERVA_NAME,
    MINERVA_TODAY, MINERVA_VENV_TAR_GZ_LINUX, MINERVA_VENV_TAR_GZ_HDFS, GRAPHFRAMES_JAR

Uso:
    python main.py [--level INFO] [--log-file run.log] [--build-only]
                   [--cleanup | --cleanup-only]
"""
import argparse
import os
import sys
import time

from typing import Optional

from libs.data_engineering_toolbox.context.logging import get_logger, setup_logging
logger = get_logger(__name__)

REQUIRED_ENV_VARS = [
    "MINERVA_WORKSPACE_DIR_LINUX",
    "PYSPARK_QUEUE",
    "PYSPARK_PORT",
    "MINERVA_NAME",
    "MINERVA_TODAY",
    "MINERVA_VENV_TAR_GZ_LINUX",
    "MINERVA_VENV_TAR_GZ_HDFS",
    "MINERVA_GRAPHFRAMES_JAR_HDFS",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ejecuta el flujo completo del pipeline Minerva."
    )
    parser.add_argument("--level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nivel de logging (default: INFO)")
    parser.add_argument("--log-file", default=None,
        help="Ruta opcional de fichero de log")
    parser.add_argument("--build-only", action="store_true",
        help="Construye la cadena de steps sin ejecutar el flujo")
    parser.add_argument("--cleanup", action="store_true",
        help="Tras ejecutar, borra los parquets intermedios "
            "(outputs keep_or_delete='delete')")
    parser.add_argument("--cleanup-only", action="store_true",
        help="Borra los parquets intermedios sin ejecutar el flujo")
    return parser.parse_args()


def configure_logging(level:str, log_file:Optional[str] = None) -> None:
    setup_logging(level=level, force_reload=True, log_file=log_file)


def check_environment() -> None:
    missing = [v for v in REQUIRED_ENV_VARS if v not in os.environ]
    if missing:
        raise EnvironmentError(f"Faltan variables de entorno: {missing}")
    logger.debug("Variables de entorno: %s",
        {v: os.environ[v] for v in REQUIRED_ENV_VARS})


def build_spark():
    """Crea la SparkSession remota del clúster (mismos parámetros que run_order)."""
    from libs.data_engineering_toolbox.context import SparkSessionBuilder
    spark = SparkSessionBuilder().build()
    logger.info("SparkSession creada (app=%s, queue=datalabs)",
        os.environ["MINERVA_NAME"])
    return spark


def build_pipeline(spark):
    """Construye la cadena completa de steps y devuelve el último.

    `Step.execute()` ejecuta recursivamente los `previous_step`, por lo que
    ejecutar el último step lanza todo el flujo (reanudable: cada etapa
    recarga su parquet/partición si ya existe).
    """
    # --- pipelines ---
    import pipelines.ceps.rfc_nom_ranking as p_c_rnr
    import pipelines.ceps.txn_replacement as p_c_txn_rpl
    import pipelines.graph_making.ceps.special_treatment as p_gm_st
    import pipelines.graph_making.ceps.group_by as p_gm_gb
    import pipelines.graph_making.ceps.edges_and_nodes as p_gm_en
    import pipelines.features.ceps.graph_features as p_f_cgf
    import pipelines.target_propagation.lovelace.special_treatment as p_tp_l_st
    import pipelines.features.ceps.target_propagation_features as p_f_tp
    import pipelines.features.ceps.vector_assembler as p_f_va
    # --- configs ---
    import config.job as cj
    import config.ceps.rfc_nom_ranking as ccrbr
    import config.ceps.txn_replacement as cctr
    import config.graph_making.ceps.special_treatment as ccspt
    import config.graph_making.ceps.group_by as ccgb
    import config.graph_making.ceps.edges_and_nodes as ccgen
    import config.features.ceps.graph_features as cfgf
    import config.target_propagation.lovelace.special_treatment as ctp_l_st
    import config.features.ceps.target_propagation_features as cfcf
    import config.features.ceps.vector_assembler as cvas
    #
    configure_logging(level="WARNING")
    logger.info("Construyendo cadena de steps del pipeline")

    ceps_rfs_nom_ranking_step = p_c_rnr.CepsRfcNomRankingStep(
        date_treatment=cj.date_treatment,
        input_hive=ccrbr.input,
        output_hive=ccrbr.output,
        is_dynamic=True,
        cohort=cj.COHORT,
        sqlContext=spark,
    )
    ceps_txn_replacement_step = p_c_txn_rpl.CepsTxnReplacementStep(
        date_treatment=cj.date_treatment,
        input_hive=cctr.input,
        output_hive=cctr.output,
        sqlContext=spark,
        is_dynamic=cj.IS_DYNAMIC,
        cohort=cj.COHORT,
        previous_step=[ceps_rfs_nom_ranking_step],
    )
    graph_special_treatment_step = p_gm_st.CepsSpecialTreatment(
        date_treatment=cj.date_treatment,
        input_hive=ccspt.input,
        output_hive=ccspt.output,
        is_dynamic=True,
        cohort=cj.COHORT,
        sqlContext=spark,
        previous_step=[ceps_txn_replacement_step],
    )
    graph_group_by_step = p_gm_gb.CepsGroupByStep(
        date_treatment=cj.date_treatment,
        input_hive=ccgb.input,
        output_hive=ccgb.output,
        is_dynamic=True,
        cohort=cj.COHORT,
        sqlContext=spark,
        previous_step=[graph_special_treatment_step],
    )
    graph_edges_and_nodes_step = p_gm_en.CepsEdgesAndNodesStep(
        date_treatment=cj.date_treatment,
        input_hive=ccgen.input,
        output_hive=ccgen.output,
        is_dynamic=True,
        cohort=cj.COHORT,
        sqlContext=spark,
        previous_step=[graph_group_by_step],
    )
    graph_features_step = p_f_cgf.CepsGraphFeaturesStep(
        date_treatment=cj.date_treatment,
        input_hive=cfgf.input,
        output_hive=cfgf.output,
        is_dynamic=True,
        cohort=cj.COHORT,
        sqlContext=spark,
        previous_step=[graph_edges_and_nodes_step],
    )
    target_propagation_step = p_tp_l_st.LovelaceTargetPropagationSpecialTreatmentStep(
        date_treatment=cj.date_treatment,
        input_hive=ctp_l_st.input,
        output_hive=ctp_l_st.output,
        is_dynamic=True,
        cohort=cj.COHORT,
        sqlContext=spark,
        previous_step=[graph_features_step],
    )
    target_propagation_features_step = p_f_tp.CepsTargetPropagationFeaturesStep(
        date_treatment=cj.date_treatment,
        input_hive=cfcf.input,
        output_hive=cfcf.output,
        is_dynamic=True,
        cohort=cj.COHORT,
        sqlContext=spark,
        previous_step=[target_propagation_step],
    )
    vector_assembler_step = p_f_va.CepsVectorAssemblerStep(
        date_treatment=cj.date_treatment,
        input_hive=cvas.input,
        output_hive=cvas.output,
        is_dynamic=True,
        cohort=cj.COHORT,
        sqlContext=spark,
        previous_step=[target_propagation_features_step],
    )

    logger.info("Cadena construida: 9 steps (último: %s)",
        vector_assembler_step.step_name)
    return vector_assembler_step


def main() -> int:
    args = parse_args()

    logger.info("=== Inicio del pipeline Minerva ===")
    start = time.time()
    try:
        check_environment()
        spark = build_spark()
        last_step = build_pipeline(spark)
        if args.build_only:
            logger.info("--build-only: cadena construida, no se ejecuta")
            return 0
        if args.cleanup_only:
            deleted = last_step.delete_tmp_paths()
            logger.info("Limpieza: %d parquets intermedios borrados", len(deleted))
            return 0
        logger.info("Ejecutando flujo completo (último step: %s)",
            last_step.step_name)
        last_step.execute()
        if args.cleanup:
            deleted = last_step.delete_tmp_paths()
            logger.info("Limpieza post-flujo: %d parquets intermedios borrados",
                len(deleted))
    except Exception:
        logger.exception("El pipeline falló tras %.1fs", time.time() - start)
        return 1
    logger.info("=== Pipeline completado en %.1fs ===", time.time() - start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
