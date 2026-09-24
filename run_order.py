##########################################################################
# Spark Session and Environment Setup
##########################################################################

# Libraries
import os
from pathlib import Path

# Read environment variables
WORKSPACE = Path(os.environ['MINERVA_WORKSPACE_DIR_LINUX'])
spark_queue = os.environ['PYSPARK_QUEUE']
spark_port = int(os.environ['PYSPARK_PORT'])
spark_name = os.environ['MINERVA_NAME']
__today = os.environ['MINERVA_TODAY']

# Add workspace to sys.path
os.chdir(WORKSPACE)
ven_zip_linux = os.environ['MINERVA_VENV_TAR_GZ_LINUX']
ven_zip_hdfs = os.environ['MINERVA_VENV_TAR_GZ_HDFS']

jar_linux = os.environ['GRAPHFRAMES_JAR']

# Custom libraries
from libs.data_engineering_toolbox.context import SparkSessionBuilder


# spark.stop()
spark = SparkSessionBuilder().build()


##########################################################################
# Libraries and Custom Modules Import
##########################################################################

# ----------------------------------------------------------------------------
# Standard
# ----------------------------------------------------------------------------

from importlib import reload

# ----------------------------------------------------------------------------
# Custom
# ----------------------------------------------------------------------------
import pipelines.ceps.rfc_nom_ranking as p_c_rnr
import pipelines.ceps.txn_replacement as p_c_txn_rpl

import pipelines.graph_making.group_by as p_gm_gb_or

import pipelines.graph_making.ceps.special_treatment as p_gm_st
import pipelines.graph_making.ceps.group_by as p_gm_gb
import pipelines.graph_making.ceps.edges_and_nodes as p_gm_en
import pipelines.features.ceps.graph_features as p_f_cgf
import pipelines.target_propagation.lovelace.special_treatment as p_tp_l_st
import pipelines.features.ceps.target_propagation_features as p_f_tp
import pipelines.features.ceps.vector_assembler as p_f_va

import libs.framework as fw

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


for lib_ in [p_c_rnr, cj, ccrbr, fw, p_c_txn_rpl, cctr,
    p_gm_st, ccspt, p_gm_gb, ccgb, p_gm_gb_or, ccgen, p_gm_en, cfgf, p_f_cgf,
    p_tp_l_st, ctp_l_st, cfcf, p_f_tp, p_f_va, cvas
]:
    reload(lib_)

##########################################################################
# Pipeline Steps Initialization
##########################################################################

ceps_rfs_nom_ranking_step = p_c_rnr.CepsRfcNomRankingStep(
    date_treatment=cj.date_treatment,
    input_hive=ccrbr.input,
    output_hive=ccrbr.output,
    is_dynamic=True,
    cohort=cj.COHORT,
    sqlContext=spark
)

ceps_txn_replacement_step = p_c_txn_rpl.CepsTxnReplacementStep(
    date_treatment=cj.date_treatment,
    input_hive=cctr.input,
    output_hive=cctr.output,
    sqlContext=spark,
    is_dynamic=cj.IS_DYNAMIC,
    cohort=cj.COHORT,
    previous_step=[ceps_rfs_nom_ranking_step]
)


graph_special_treatment_step = p_gm_st.CepsSpecialTreatment(
    date_treatment=cj.date_treatment,
    input_hive=ccspt.input,
    output_hive=ccspt.output,
    is_dynamic=True,
    cohort=cj.COHORT,
    sqlContext=spark
)


graph_group_by_step = p_gm_gb.CepsGroupByStep(
    date_treatment=cj.date_treatment,
    input_hive=ccgb.input,
    output_hive=ccgb.output,
    is_dynamic=True,
    cohort=cj.COHORT,
    sqlContext=spark,
    previous_step=[graph_special_treatment_step]
)

graph_edges_and_nodes_step = p_gm_en.CepsEdgesAndNodesStep(
    date_treatment=cj.date_treatment,
    input_hive=ccgen.input,
    output_hive=ccgen.output,
    is_dynamic=True,
    cohort=cj.COHORT,
    sqlContext=spark,
    previous_step=[graph_group_by_step]
)

graph_features_step = p_f_cgf.CepsGraphFeaturesStep(
    date_treatment=cj.date_treatment,
    input_hive=cfgf.input,
    output_hive=cfgf.output,
    is_dynamic=True,
    cohort=cj.COHORT,
    sqlContext=spark,
    previous_step=[graph_edges_and_nodes_step]
)

target_propagation_step = p_tp_l_st.LovelaceTargetPropagationSpecialTreatmentStep(
    date_treatment=cj.date_treatment,
    input_hive=ctp_l_st.input,
    output_hive=ctp_l_st.output,
    is_dynamic=True,
    cohort=cj.COHORT,
    sqlContext=spark,
    previous_step=[graph_features_step]
)

target_propagation_features_step = p_f_tp.CepsTargetPropagationFeaturesStep(
    date_treatment=cj.date_treatment,
    input_hive=cfcf.input,
    output_hive=cfcf.output,
    is_dynamic=True,
    cohort=cj.COHORT,
    sqlContext=spark,
    previous_step=[target_propagation_step]
)


#############################################################
# PROPAGATE TARGET
#############################################################

nodes_join_target = target_propagation_features_step.lovelace_ceps_join_graph_step.nodes_join_target[0]
edges = graph_edges_and_nodes_step.ceps_edges_and_nodes_step.edges[0]

nodes_join_target.show(10)

"""
+---+-----------+---+------------------+-------+----------------+-----------+-------+------+------------------+
|                id|numcliente|                              nom|                cta|   id_ban|information_date|     oper_mto|vintage|cohort|        target_lov...
+---+-----------+---+------------------+-------+----------------+-----------+-------+------+------------------+
|     #EMM9701268G9|        []|              [MELANY OSMARA NE...,| [021164065720498771]|   [40021]|      2025-07-14|         400.00| 202507|   SBX|3.201464369358513E-...
|014410337580115080|       [0]|             [PATRICIA VIANEY,...|[5204166176356433,..|[40014, 40021, 40,..|   2025-06-09|19140450893.58| 202507|   SBX|3.201464369358513E-...
|014416259000118001|       [0]|            [VAZQUEZ ROMERO M,..| [002180090285429669]|   [40002]|      2025-05-02|      10518.45| 202507|   SBX|           0.7177...
|              ...|       [0]|         [URDAPILLETA Y SA,..| [002540901255735240]|   [40002]|      2025-05-02|      10518.45| 202507|   SBX|3.201464369358513E-4...
"""


#############################################################
# VECTOR ASSEMBLER (features a nivel numcliente)
#############################################################

vector_assembler_step = p_f_va.CepsVectorAssemblerStep(
    date_treatment=cj.date_treatment,
    input_hive=cvas.input,
    output_hive=cvas.output,
    is_dynamic=True,
    cohort=cj.COHORT,
    sqlContext=spark,
    previous_step=[target_propagation_features_step]
)

numcliente_features_vector = vector_assembler_step.lovelace_ceps_assembler_step.numcliente_features_vector[0]
numcliente_features_vector.show(10)


#############################################################
# CLEANUP DE PARQUETS INTERMEDIOS
#############################################################

# Los outputs marcados "keep_or_delete": "delete" (edges_norm, checkpoint)
# se pueden borrar una vez finalizado el flujo:
#
# deleted = vector_assembler_step.delete_tmp_paths()
#
# equivalente a: python main.py --cleanup  (o --cleanup-only sin ejecutar)
