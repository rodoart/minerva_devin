
########################################################################
# SPECIAL TREATMENT
########################################################################

# ----------------------------------------------------------------------
# General
# ----------------------------------------------------------------------
from typing import List, Dict, Optional, Union


# ----------------------------------------------------------------------
# Pyspark
# ----------------------------------------------------------------------
from pyspark.sql import DataFrame, Column

# ----------------------------------------------------------------------
# Custom
# ----------------------------------------------------------------------
from libs.data_engineering_toolbox.path import HivePath
from libs.functions.weights import build_weights_map


import libs.framework as ppf
########################################################################
# Process
########################################################################


class SubStep(ppf.Step):
    """Clase base para las sub-etapas del pipeline `StandardEdgesAndNodesStep`.

    Hereda los atributos del step padre (rutas Hive, tratamiento de fechas,
    cohorte, etc.) mediante `ppf.inherit_parent_step_attributes`.
    """
    def __init__(self, parent: "StandardEdgesAndNodesStep", *args, **kwargs) -> None:
        super().__init__(parent, *args, **kwargs)
        ppf.inherit_parent_step_attributes(self, parent)


class StandardEdgesAndNodesStep(ppf.Step):
    """Step orquestador base de la construcción de aristas y nodos del grafo.

    Expone el substep `standard_groupby_step`; las implementaciones concretas
    definen su `step_action` y las variables de aristas/nodos a conservar.
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
        """Carga estándar de una tabla/parquet particionado con validación de historial.

        Args:
            config_dict_key: Clave de la tabla dentro de `input_hive`/`output_hive`.
            input_or_output: "input" lee de `input_hive`; otro valor usa `output_hive`.

        Returns:
            DataFrame cargado para la fecha `vintage_date` de los parámetros de entrada.
        """
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
        """Substep de aristas y nodos (lazy, cacheado); solo para pruebas."""
        return StandardEdgesAndNodesSubStep(self)


class StandardEdgesAndNodesSubStep(SubStep):
    """Substep base que deriva el DataFrame de aristas y el de nodos del grafo."""
    # previous_step
    def get_previous_step(self, key:str) -> DataFrame:
        """Carga la salida persistida del step previo identificada por `key`."""
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
        """Construye el DataFrame de aristas seleccionando ids, variables y pesos.

        Args:
            input_df: Transacciones agregadas por arista.
            edges_vars: Variables de arista a conservar; `["*"]` toma todas las
                columnas salvo ids y fechas.
            weight_columns: Expresiones de peso empaquetadas en un mapa por
                `build_weights_map`.
            txn_id_columns: Columnas identificadoras de la arista
                (por defecto `["id_src", "id_dst"]`).
            date_columns: Columnas de particionado a conservar.

        Returns:
            DataFrame de aristas con ids + `edges_vars` + mapa de pesos + fechas.
        """
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
        weight_map_column = build_weights_map(weight_columns)
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
        """Construye el DataFrame de nodos seleccionando id, variables y fechas.

        Args:
            input_df: Datos agregados por nodo.
            node_vars: Variables de nodo a conservar; `["*"]` toma todas las
                columnas salvo id y fechas.
            node_id_column: Columna(s) identificadora(s) del nodo
                (por defecto `"id"`).
            date_columns: Columnas de particionado a conservar.

        Returns:
            DataFrame de nodos con id + `node_vars` + fechas.
        """
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
