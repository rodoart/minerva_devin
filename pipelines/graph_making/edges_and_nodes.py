
########################################################################
# SPECIAL TREATMENT
########################################################################

# ----------------------------------------------------------------------
# General
# ----------------------------------------------------------------------
from typing import List, Dict, Any, Optional, Union, Dict


# ----------------------------------------------------------------------
# Pyspark
# ----------------------------------------------------------------------
from pyspark.sql import DataFrame, Column

from pyspark.sql.functions import (col,to_date, lit, date_format,
    max as spark_max, create_map)
from pyspark.sql.types import StringType
# ----------------------------------------------------------------------
# Custom
# ----------------------------------------------------------------------
from data_engineering_toolbox.path import HivePath


import libs.framework as ppf
import config.job as cj

from importlib import reload
reload(cj)
########################################################################
# Process
########################################################################


class SubStep(ppf.Step):
    """
    """
    def __init__(self, parent: "StandardEdgesAndNodesStep", *args, **kwargs) -> None:
        super().__init__(parent, *args, **kwargs)
        ppf.inherit_parent_step_attributes(self, parent)


class StandardEdgesAndNodesStep(ppf.Step):
    """
    """
    def __init__(self,
        date_treatment: Dict[str,str],
        input_hive:Dict[str, HivePath],
        output_hive:Dict[str,HivePath],
        cohort:str,
        is_dynamic: bool = True,
        *args, **kwargs
    ) -> None:
        self.date_treatment = date_treatment
        self.is_dynamic = is_dynamic
        self.cohort = cohort
        super_class_kwargs = ppf.build_step_init_kwargs(
            date_treatment=date_treatment,
            input_hive=input_hive,
            output_hive=output_hive,
            step_name_prefix="Standard_Edges_And_Nodes_Step",
            extra_kwargs=kwargs,
        )
        super().__init__(*args, **super_class_kwargs)
        #
    def standard_load_parquet_or_table(self, config_dict_key, input_or_output:str = "input") -> DataFrame:
        config_dict = self.input_hive if input_or_output == "input" else self.output_hive
        return ppf.standard_load_parquet_or_table(
            config_dict=config_dict,
            config_dict_key=config_dict_key,
            current_date=self.input_parameters['vintage_date'],
            session=self.sqlContext,
        )
        #
    # For testting purposes only
    @ppf.cached_property
    def standard_groupby_step(self) -> "StandardEdgesAndNodesSubStep":
        return StandardEdgesAndNodesSubStep(self)


class StandardEdgesAndNodesSubStep(SubStep):
    # previous_step
    def get_previous_step(self, key:str) -> DataFrame:
        result:DataFrame = (
            self.standard_load_parquet_or_table(key)
        )
        return result
        #
        #
    def standard_edges(self,
        input_df: DataFrame,
        edges_vars: List[Union[str, Column]],
        weight_columns: Dict[str, Column],
        txn_id_columns:Optional[List[str]]=["id_src", "id_dst"],
        date_columns:List[str] = ["process_date", "mis_date", "vintage", "cohort"]
    ) -> DataFrame:
        #
        #
        if txn_id_columns is None or len(txn_id_columns) == 0:
            txn_id_columns = ["id_src", "id_dst"]
        #
        #
        if isinstance(edges_vars, str):
            edges_vars = [edges_vars]
        #
        if edges_vars == ["*"]:
            edges_vars = [c for c in input_df.columns if c not in (txn_id_columns + date_columns)]
        #
        #
        weight_map_column = create_map(*[x for k, v in weight_columns.items() for x in (lit(k), v)]).alias("weights")
        # Select final vars to the edges, this vars can be used to estimate the weight.
        final_edges_vars = txn_id_columns + edges_vars + [weight_map_column] + date_columns
        #
        return (input_df
            .select(*final_edges_vars)
        )
        #
    def standard_nodes(self,
        input_df: DataFrame,
        node_vars: List[Union[str, Column]],
        node_id_column:Union[str, List[str]] = "id",
        date_columns:List[str] = ["process_date", "mis_date", "vintage", "cohort"]
    ) -> DataFrame:
        if isinstance(node_vars, str):
            node_vars = [node_vars]
        #
        if isinstance(node_id_column, str):
            node_id_column = [node_id_column]
        #
        if node_vars == ["*"]:
            node_vars = [c for c in input_df.columns if c not in (date_columns + node_id_column)]
        #
        final_node_vars = node_id_column + node_vars + date_columns
        #
        return (input_df
            .select(*final_node_vars)
        )
