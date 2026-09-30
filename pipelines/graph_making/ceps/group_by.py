
##########################################################################
# LIBRARIES
##########################################################################

# ------------------------------------------------------------------------
# General
# ------------------------------------------------------------------------
from typing import Dict, Any, List
from datetime import datetime

from dateutil.relativedelta import relativedelta

# ------------------------------------------------------------------------
# Pyspark
# ------------------------------------------------------------------------
from pyspark.sql import DataFrame

from pyspark.sql.functions import (col, when, lit
)
from pyspark.sql.types import  StringType
# ------------------------------------------------------------------------
# Custom
# ------------------------------------------------------------------------
import libs.framework as ppf
import libs.functions.aggregations as lfa
from libs.functions.aggregations import resolve_group_by_expressions
import config.job as cj
import config.graph_making as c_gmc
import config.graph_making.ceps.group_by as ccgb
import pipelines.graph_making.group_by as p_gm_gb

##########################################################################
# CLASSES
##########################################################################


class CepsGroupByStep(p_gm_gb.StandardGroupByStep):
    """Step orquestador del group-by del grafo CEPS (por arista y por nodo)."""
    def step_action(self) -> Dict[str, Any]:
        """Ejecuta el substep de agregación (`ceps_group_by_step`)."""
        return ppf.run_substep_and_collect(self, self.ceps_group_by_step, "ceps_group_by_step")
        #
    @ppf.cached_property
    def ceps_group_by_step(self) -> "CepsGroupBySubStep":
        """Substep de agregación CEPS (lazy, cacheado) con dependencia del step previo."""
        return CepsGroupBySubStep(self, previous_step=self.previous_step[0])


class CepsGroupBySubStep(p_gm_gb.StandardGroupBySubStep):
    """Substep que agrega las transacciones CEP por arista (`group_by_txn`) y por nodo (`group_by_id`).

    Las agregaciones se materializan incrementalmente: primero se computan y
    persisten agregados parciales por mes (`group_by_txn_monthly`,
    `group_by_id_monthly`), computando únicamente los meses ausentes de la
    ventana; después el agregado completo se obtiene combinando esas
    parciales (ver `libs.framework.ensure_monthly_partitions` y
    `libs.functions.aggregations.merge_monthly_group_by`).
    """
    def step_action(self) -> Dict[str, Any]:
        """Materializa y colecta `group_by_txn` y `group_by_id` (ambos se
        recargan o recomputan de forma independiente)."""
        return ppf.collect_step_output(
            self, {"group_by_txn": self.group_by_txn, "group_by_id": self.group_by_id,
                "group_by_txn_monthly": self.group_by_txn_monthly,
                "group_by_id_monthly": self.group_by_id_monthly},
            "group_by_id")
        #
    @ppf.cached_property
    def missing_treatment(self) -> DataFrame:
        """Recarga la salida `missing_treatment` del step de *special treatment*."""
        return self.get_previous_step("missing_treatment")
        #
    @ppf.cached_property
    def group_by_value_columns(self) -> List[str]:
        """Variables de valor pedidas por `GROUP_TXN_AGGREGATIONS` (sin `tfrom_days`)."""
        value_columns: List[str] = []
        for agg_name in ccgb.GROUP_TXN_AGGREGATIONS:
            _, variable = lfa.split_group_by_name(agg_name, ccgb.GROUP_BY_TXN_FEATURES)
            if variable != "tfrom_days" and variable not in value_columns:
                value_columns.append(variable)
        return value_columns
        #
    @ppf.cached_property
    def tfrom_reference_date(self) -> str:
        """Fecha de referencia de `tfrom_days`, idéntica a la de `calculate_tfroms`.

        Referencia = último día del mes vintage desplazado por el lag global
        del grafo (meses).
        """
        lag = c_gmc.GRAPH_GLOBAL_LAG
        reference = (datetime.strptime(
            self.parent.date_treatment["last_day_of_current_month_date_str"],
            cj.DATE_STANDARD_FORMAT) - relativedelta(months=lag))
        return reference.strftime(cj.DATE_STANDARD_FORMAT)
        #
    @ppf.cached_property
    def group_by_txn_monthly(self) -> DataFrame:
        """Agregados parciales por arista-mes, persistidos incrementalmente.

        Solo se computan y escriben los meses de la ventana que aún no tienen
        partición; el resto se recarga desde disco.
        """
        def compute_missing_months(missing_months: List[str]) -> DataFrame:
            return self.standard_group_by_txn_monthly(
                input_df=self.missing_treatment,
                value_columns=self.group_by_value_columns,
                txn_id_columns=ccgb.GROUP_BY_TXN_GROUPING_VARS,
                months=missing_months)
        return ppf.ensure_monthly_partitions(
            step=self,
            path_key="group_by_txn_monthly",
            compute_missing_months=compute_missing_months)
        #
    @ppf.cached_property
    @ppf.dynamic_partitioned_table_or_parquet(path_key="group_by_txn")
    def group_by_txn(self) -> DataFrame:
        """Agrega las transacciones tratadas por arista con `GROUP_TXN_AGGREGATIONS` de config.

        Combina los agregados parciales mensuales (`group_by_txn_monthly`).
        Si la configuración pide agregaciones sin combinador mensual, hace
        fallback al group-by completo sobre `missing_treatment`.
        """
        try:
            group_by_txn:DataFrame = self.standard_merge_group_by_txn_monthly(
                monthly_df=self.group_by_txn_monthly,
                aggregations=ccgb.GROUP_TXN_AGGREGATIONS,
                feature_names=ccgb.GROUP_BY_TXN_FEATURES,
                reference_date=self.tfrom_reference_date,
                txn_id_columns=ccgb.GROUP_BY_TXN_GROUPING_VARS,
            )
        except ValueError:
            # GROUP_TXN_AGGREGATIONS son nombres "{func}_{variable}" -> Column
            # vía GROUP_BY_TXN_FEATURES (prefijo de función más largo primero).
            aggregations = resolve_group_by_expressions(
                ccgb.GROUP_TXN_AGGREGATIONS, ccgb.GROUP_BY_TXN_FEATURES)
            group_by_txn = self.standard_group_by_txn(
                input_df=self.missing_treatment,
                aggregations=aggregations,
                txn_id_columns=ccgb.GROUP_BY_TXN_GROUPING_VARS,
                auxiliary_columns=ccgb.GROUP_BY_AUXILIARY_COLS
            )
        #
        return group_by_txn
        #
    @ppf.cached_property
    def txn_src(self) -> DataFrame:
        """Proyección del lado ordenante; `numcliente` solo se conserva si `cve_tipo_orden` == "E"."""
        missing_treatment: DataFrame = self.missing_treatment
        return (self.standard_txn_src_split(
            input_df=missing_treatment,
            id_columns=ccgb.ID_SRC_COLUMNS,
            unique_columns=ccgb.SRC_COLUMNS,
            common_columns=ccgb.COMMON_COLUMNS,
            suffix="_src"
            )
            # custom imputation for ceps
            .withColumn("numcliente",  when((col("cve_tipo_orden") == "E"),  col("numcliente")).otherwise(lit(None).cast(StringType())))
        )
        #
    @ppf.cached_property
    def txn_dst(self) -> DataFrame:
        """Proyección del lado beneficiario; `numcliente` solo se conserva si `cve_tipo_orden` == "R"."""
        missing_treatment: DataFrame = self.missing_treatment
        return (self.standard_txn_dst_split(
            input_df=missing_treatment,
            id_columns=ccgb.ID_DST_COLUMNS,
            unique_columns=ccgb.DST_COLUMNS,
            common_columns=ccgb.COMMON_COLUMNS,
            suffix="_dst"
            )
            .withColumn("numcliente",  when((col("cve_tipo_orden") == "R"),  col("numcliente")).otherwise(lit(None).cast(StringType())))
        )
    @ppf.cached_property
    def group_by_id_monthly(self) -> DataFrame:
        """Agregados parciales por nodo-mes, persistidos incrementalmente.

        Solo se computan y escriben los meses de la ventana que aún no tienen
        partición; el resto se recarga desde disco.
        """
        def compute_missing_months(missing_months: List[str]) -> DataFrame:
            return self.standard_group_by_id_monthly(
                input_src_df=self.txn_src,
                input_dst_df=self.txn_dst,
                set_columns=ccgb.GROUP_ID_SET_COLUMNS,
                sum_columns=ccgb.GROUP_ID_SUM_COLUMNS,
                id_columns="id",
                months=missing_months)
        return ppf.ensure_monthly_partitions(
            step=self,
            path_key="group_by_id_monthly",
            compute_missing_months=compute_missing_months)
        #
    @ppf.cached_property
    @ppf.dynamic_partitioned_table_or_parquet(path_key="group_by_id")
    def group_by_id(self) -> DataFrame:
        """Agrega la unión de `txn_src` y `txn_dst` por `id` para formar los nodos.

        Combina los agregados parciales mensuales (`group_by_id_monthly`);
        hace fallback al group-by completo si la combinación falla.
        Materializa también `group_by_txn`, del que dependen las aristas.
        """
        _:DataFrame = self.group_by_txn
        try:
            group_by_id: DataFrame = self.standard_merge_group_by_id_monthly(
                monthly_df=self.group_by_id_monthly,
                set_columns=ccgb.GROUP_ID_SET_COLUMNS,
                sum_columns=ccgb.GROUP_ID_SUM_COLUMNS,
                id_columns="id",
            )
        except ValueError:
            group_by_id = self.standard_group_by_id(
                input_src_df=self.txn_src,
                input_dst_df=self.txn_dst,
                aggregations=ccgb.GROUP_ID_AGGREGATIONS,
                id_columns="id",
            )
        return group_by_id
