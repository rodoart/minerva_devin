"""Pipeline de ejemplo: transacciones crudas -> limpieza -> agregados por arista.

Demuestra el patrón del framework:

- Un `Step` orquestador por etapa (`ExampleExtractStep`, `ExampleGroupByStep`),
  cuyo `step_action` dispara un `SubStep` y colecta su salida.
- Los substeps heredan los atributos del padre (`inherit_parent_step_attributes`)
  y exponen propiedades `@cached_property` decoradas con
  `@dynamic_partitioned_table_or_parquet(path_key=...)`: si la partición de la
  ventana ya existe se RECARGA; si no, se computa y se escribe (pipeline
  reanudable).
- Agregación incremental: `txns_monthly` guarda parciales combinables por
  arista-mes y `ensure_monthly_partitions` computa SOLO los meses ausentes;
  `merge_monthly_group_by` combina los parciales en el agregado de la ventana.
"""
from typing import Dict, Any, List
from datetime import datetime

from dateutil.relativedelta import relativedelta

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, to_date, date_format
from pyspark.sql.types import StringType

import libs.framework as ppf
import libs.functions.aggregations as lfa
import pipelines.graph_making.special_treatment as p_gm_sp
import pipelines.graph_making.group_by as p_gm_gb
from libs.data_engineering_toolbox.general.date_treatment import (
    DATE_STANDARD_FORMAT, DATE_STANDARD_SPARK_FORMAT, DATE_MONTH_SPARK_FORMAT)

from examples.config import transactions_etl as cfg


###############################################################################
# STEP 1: EXTRACT (special treatment)
###############################################################################

class ExampleExtractStep(p_gm_sp.StandardSpecialTreatment):
    """Step orquestador de la extracción/limpieza de transacciones."""
    def step_action(self) -> Dict[str, Any]:
        return ppf.run_substep_and_collect(
            self, self.extract_substep, "extract_substep")
        #
    @ppf.cached_property
    def extract_substep(self) -> "ExampleExtractSubStep":
        return ExampleExtractSubStep(self)


class ExampleExtractSubStep(p_gm_sp.StandardExtractSubStep):
    """Carga `raw_txns`, calcula `tfrom_*` y aplica missing treatment."""
    def step_action(self) -> Dict[str, Any]:
        return ppf.collect_step_output(self, self.txns_clean, "txns_clean")
        #
    @ppf.cached_property
    def raw_txns(self) -> DataFrame:
        """Histórico crudo con `tfrom_months`/`tfrom_days` calculados."""
        return self.input_table_historic("raw_txns").transform(
            lambda df: self.calculate_tfroms(df, "raw_txns"))
        #
    @ppf.cached_property
    @ppf.dynamic_partitioned_table_or_parquet(path_key="txns_clean")
    def txns_clean(self) -> DataFrame:
        """Transacciones tratadas: nulos imputados + columnas de partición.

        El decorador particionado recarga la partición
        (mis_date=vintage, process_date=hoy) si existe; si no, ejecuta esta
        función, estampa las columnas de partición y sobreescribe solo esa
        partición.
        """
        return (self.raw_txns
            .transform(lambda df: self.standard_missing_treatment(
                df, cfg.MISSING_TREATMENT))
            # Reglas de ejemplo: descartar txns sin identificadores.
            .filter(col("id_src").isNotNull() & col("id_dst").isNotNull())
            .withColumn("process_date",
                lit(self.parent.date_treatment["process_date_str"]))
            .withColumn("mis_date",
                date_format(to_date(col("information_date"),
                    DATE_STANDARD_SPARK_FORMAT), DATE_MONTH_SPARK_FORMAT))
            .withColumn("vintage",
                lit(self.parent.date_treatment["vintage"]).cast(StringType()))
            .withColumn("cohort", lit(self.cohort).cast(StringType()))
        )


###############################################################################
# STEP 2: GROUP BY (incremental mensual)
###############################################################################

class ExampleGroupByStep(p_gm_gb.StandardGroupByStep):
    """Step orquestador de la agregación por arista."""
    def step_action(self) -> Dict[str, Any]:
        return ppf.run_substep_and_collect(
            self, self.group_by_substep, "group_by_substep")
        #
    @ppf.cached_property
    def group_by_substep(self) -> "ExampleGroupBySubStep":
        return ExampleGroupBySubStep(self)


class ExampleGroupBySubStep(p_gm_gb.StandardGroupBySubStep):
    """Agrega las transacciones por (id_src, id_dst) de forma incremental.

    Flujo: `txns_clean` -> parciales por arista-mes (`txns_monthly`, solo los
    meses que falten) -> agregado de la ventana (`txns_grouped`) combinando
    parciales.
    """
    def step_action(self) -> Dict[str, Any]:
        return ppf.collect_step_output(
            self,
            {"txns_monthly": self.txns_monthly,
             "txns_grouped": self.txns_grouped},
            "txns_grouped")
        #
    @ppf.cached_property
    def txns_clean(self) -> DataFrame:
        """Recarga la salida `txns_clean` del step de extracción."""
        return self.get_previous_step("txns_clean")
        #
    @ppf.cached_property
    def tfrom_reference_date(self) -> str:
        """Fecha de referencia de `tfrom_days` (la misma que `calculate_tfroms`)."""
        lag = cfg.GLOBAL_LAG
        reference = (datetime.strptime(
            self.parent.date_treatment["last_day_of_current_month_date_str"],
            DATE_STANDARD_FORMAT) - relativedelta(months=lag))
        return reference.strftime(DATE_STANDARD_FORMAT)
        #
    @ppf.cached_property
    def txns_monthly(self) -> DataFrame:
        """Parciales por arista-mes; solo se computan los meses ausentes."""
        def compute_missing_months(missing_months: List[str]) -> DataFrame:
            return self.standard_group_by_txn_monthly(
                input_df=self.txns_clean,
                value_columns=cfg.VALUE_COLUMNS,
                txn_id_columns=cfg.GROUPING_VARS,
                months=missing_months)
        return ppf.ensure_monthly_partitions(
            step=self,
            path_key="txns_monthly",
            compute_missing_months=compute_missing_months)
        #
    @ppf.cached_property
    @ppf.dynamic_partitioned_table_or_parquet(path_key="txns_grouped")
    def txns_grouped(self) -> DataFrame:
        """Agregado de la ventana combinando los parciales mensuales.

        Si la config pidiera una agregación sin combinador mensual
        (`countDistinct`, `last`, `first`...), `merge_monthly_group_by` lanza
        `ValueError` y se hace fallback al group-by completo.
        """
        feature_names = lfa.select_group_by_features(cfg.GROUP_BY_ENABLED_FEATURES)
        try:
            return self.standard_merge_group_by_txn_monthly(
                monthly_df=self.txns_monthly,
                aggregations=cfg.GROUP_TXN_AGGREGATIONS,
                feature_names=feature_names,
                reference_date=self.tfrom_reference_date,
                txn_id_columns=cfg.GROUPING_VARS)
        except ValueError:
            aggregations = lfa.resolve_group_by_expressions(
                cfg.GROUP_TXN_AGGREGATIONS, feature_names)
            return self.standard_group_by_txn(
                input_df=self.txns_clean,
                aggregations=aggregations,
                auxiliary_columns=lfa.recency_auxiliary_columns(
                    tfrom_column="tfrom_days"),
                txn_id_columns=cfg.GROUPING_VARS)
