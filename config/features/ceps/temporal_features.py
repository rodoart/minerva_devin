##########################################################################################
# Libraries
##########################################################################################

from libs.data_engineering_toolbox.path import HivePath

import config.job as c_j
import config.graph_making.ceps.special_treatment as ccspt

##########################################################################################
# CONFIGURATION
##########################################################################################

job_config = c_j.sbx  # config del job (raíz HDFS, fechas, cohortes)

# --------------------------------------------------------------------------------------
# PARAMETERS
# --------------------------------------------------------------------------------------

# Ventanas de actividad reciente (días) para los conteos `tmp_txn_count_<w>d`
# y las velocidades relativas `tmp_velocity_<corta>d_<larga>d`.
WINDOW_DAYS = (1, 7, 30)

# Umbrales de reporte regulatorio (MXN): la feature `tmp_pct_below_threshold`
# mide la fracción de montos en [BELOW_THRESHOLD_BAND*U, U) — patrones de
# estructuración ("smurfing") justo bajo el umbral de aviso.
# AJUSTAR al umbral vigente (p.ej. el de avisos de la LFPIORPI en UMAs
# convertido a MXN; ~65k MXN aprox. en 2025).
REPORT_THRESHOLDS = [65000.0]
BELOW_THRESHOLD_BAND = 0.9     # banda: [0.9*U, U)

# Mínimo de transacciones para calcular burstiness (con menos, null).
MIN_TXN_FOR_BURSTINESS = 3

# Ventana (días) de receptores únicos por emisor: `tmp_unique_receivers_7d`.
UNIQUE_RECEIVERS_WINDOW = 7

# Horario nocturno para `tmp_pct_night` (cruza medianoche: 22:00-06:00).
NIGHT_START_HOUR = 22
NIGHT_END_HOUR = 6

# --------------------------------------------------------------------------------------
# RELATIVE PATHS
# --------------------------------------------------------------------------------------

from config.graph_making.ceps.variants import DEFAULT_VARIANT, variant_subpath  # namespacing por variante de grafo (rfc/cta)

_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))      # raíz HDFS de las salidas del job


def build(variant: str = DEFAULT_VARIANT):
    """(input, output) de temporal_features CEPS para una variante de grafo.

    - "rfc": rutas históricas intactas (cero recomputo de lo persistido).
    - otras: rutas namespaced bajo `features/ceps/<variant>/temporal_features`
      e insumo `missing_treatment` de la misma variante.
    """
    _spt_input, spt_output = ccspt.build(variant)

    current_hdfs = _general_root_hdfs.joinpath(
        variant_subpath("features/ceps", variant, "temporal_features"))  # directorio de salida del step

    input = {
        # Transacciones con ids de nodo resueltos (id_src/id_dst), montos,
        # information_date/hora_oper y tfrom_days (tabla particionada).
        "missing_treatment": spt_output["missing_treatment"],
    }

    output = {
        # Una fila por id: actividad, ritmo, horario, burstiness,
        # pass-through 24h, receptores únicos y bajo-umbral (tmp_*).
        "temporal_node_features": {"table_or_hdfs": current_hdfs.joinpath("temporal_node_features"),
            "keep_or_delete": "keep"
        },
        # Una fila por par (a, x): z-score de monto y razones a la mediana.
        "temporal_pair_features": {"table_or_hdfs": current_hdfs.joinpath("temporal_pair_features"),
            "keep_or_delete": "keep"
        },
        # Features de par agregadas a nivel nodo X: tmp_<f>_<stat> por id.
        "temporal_pair_node": {"table_or_hdfs": current_hdfs.joinpath("temporal_pair_node"),
            "keep_or_delete": "keep"
        },
    }
    return input, output


# Back-compat: el módulo expone el (input, output) de la variante por defecto.
input, output = build()
