########################################################################################################################
# Libraries
########################################################################################################################

# ----------------------------------------------------------------------------------------------------------------------
# General
# ----------------------------------------------------------------------------------------------------------------------
from typing import Dict, Any, Callable
from threading import Lock

write_lock = Lock()

# ----------------------------------------------------------------------------------------------------------------------
# Pyspark
# ----------------------------------------------------------------------------------------------------------------------
from pyspark.sql import DataFrame, Window

from pyspark.sql.functions import (col, regexp_replace, to_date, trim,
    when, lit, concat_ws,
    sum as spark_sum, row_number, create_map, count as spark_count, max as spark_max,
    upper, date_format, split, translate
)
from pyspark.sql.types import StringType
# ----------------------------------------------------------------------------------------------------------------------
# Custom
# ----------------------------------------------------------------------------------------------------------------------
from libs.data_engineering_toolbox.path import HivePath

import libs.framework as ppf

from config.ceps.rfc_nom_ranking import (CURP_PATTERN, RFC_FISICA_PATTERN,
    RFC_FISICA_SIN_HOMOCLAVE_PATTERN, RFC_MORAL_PATTERN, TC_PATTERN,
    CLABE_PATTERN, RFC_NULL_SYNONYMS, NOM_NULL_SYNONYMS,
    RFC_CURP_KIND_PRIORITY, RFC_BY_CTA_PRIORITY_WINDOW, RFC_BY_NOM_PRIORITY_WINDOW
)
from config.job import DATE_STANDARD_SPARK_FORMAT, DATE_MONTH_SPARK_FORMAT

########################################################################################################################
# FUNCTIONS
########################################################################################################################

norm = lambda raw_col: upper(regexp_replace(trim(col(raw_col)), r"[A-Z]$", ""))

def add_id_validation_flags(df: DataFrame, raw_col: str, suffix: str) -> DataFrame:
    """Clasifica el tipo de identificador fiscal/bancario de una columna de texto.

    Normaliza la columna in-place (`norm`: trim + upper + sin sufijo de un
    carácter) y añade flags binarios `is_<tipo>_<suffix>` para CURP, RFC física,
    RFC moral, RFC física sin homoclave, tarjeta (TC) y CLABE, además de
    `is_any_valid_<suffix>` y la etiqueta categórica `id_kind_<suffix>`
    ("invalid" si ningún patrón coincide).

    Args:
        df: DataFrame de entrada.
        raw_col: Nombre de la columna de texto a clasificar.
        suffix: Sufijo de las columnas generadas (ej. "ord", "ben").

    Returns:
        DataFrame con la columna normalizada y los flags de clasificación.
    """
    #
    normalized = norm(raw_col)
    return (
        df
        .withColumn(raw_col, normalized)                                                                          # normalizado in-place
        .withColumn(f"is_curp_{suffix}",                     when(normalized.rlike(CURP_PATTERN),                 lit(1)).otherwise(lit(0)))
        .withColumn(f"is_rfc_fisica_{suffix}",               when(normalized.rlike(RFC_FISICA_PATTERN),           lit(1)).otherwise(lit(0)))
        .withColumn(f"is_rfc_moral_{suffix}",                when(normalized.rlike(RFC_MORAL_PATTERN),            lit(1)).otherwise(lit(0)))
        .withColumn(f"is_rfc_fisica_sin_homoclave_{suffix}", when(normalized.rlike(RFC_FISICA_SIN_HOMOCLAVE_PATTERN), lit(1)).otherwise(lit(0)))
        .withColumn(f"is_tc_{suffix}",                       when(normalized.rlike(TC_PATTERN),                   lit(1)).otherwise(lit(0)))
        .withColumn(f"is_clabe_{suffix}",                    when(normalized.rlike(CLABE_PATTERN),                lit(1)).otherwise(lit(0)))
        .withColumn(
            f"is_any_valid_{suffix}",
            when(
                col(f"is_curp_{suffix}")
                + col(f"is_rfc_fisica_{suffix}")
                + col(f"is_rfc_moral_{suffix}")
                + col(f"is_tc_{suffix}")
                + col(f"is_rfc_fisica_sin_homoclave_{suffix}")
                + col(f"is_clabe_{suffix}")
                > 0,
                lit(1)
            ).otherwise(lit(0))
        )
        .withColumn(
            f"id_kind_{suffix}",
            when(col(f"is_curp_{suffix}")                      == 1, lit("curp"))
            .when(col(f"is_rfc_fisica_{suffix}")               == 1, lit("rfc_fisica"))
            .when(col(f"is_rfc_moral_{suffix}")                == 1, lit("rfc_moral"))
            .when(col(f"is_rfc_fisica_sin_homoclave_{suffix}") == 1, lit("rfc_fisica_sin_homoclave"))
            .when(col(f"is_tc_{suffix}")                       == 1, lit("tc"))
            .when(col(f"is_clabe_{suffix}")                    == 1, lit("clabe"))
            .otherwise(lit("invalid"))
        )
    )


def banamex_nom_reorder(col_name:str) -> Callable[..., DataFrame]:
    """Factory de transformación que limpia delimitadores en nombres tipo Banamex.

    Solo si el valor contiene "," o "/", los sustituye por espacios y colapsa
    los espacios repetidos (ej. "APELLIDO,NOMBRE/APELLIDO2" ->
    "APELLIDO NOMBRE APELLIDO2"); en otro caso deja el valor intacto.

    Args:
        col_name: Nombre de la columna de nombre a transformar.

    Returns:
        Función `DataFrame -> DataFrame` aplicable con `DataFrame.transform`.
    """
    def _transform(df:DataFrame) -> DataFrame:
        return df.withColumn(
            col_name,
            when(
                col(col_name).rlike("[,/]+"),
                trim(
                    regexp_replace(
                        regexp_replace(col(col_name), "[,/]", " "),
                        " +",
                        " "
                    )
                )
            ).otherwise(col(col_name))
        )
    return _transform


def limpiar_acentos(col_name:str) -> Callable[..., DataFrame]:
    """Factory de transformación que translitera caracteres acentuados a ASCII.

    Sustituye vocales acentuadas, eñes, diéresis y "@" por su equivalente ASCII
    en la columna `col_name`.

    Args:
        col_name: Nombre de la columna a transliterar.

    Returns:
        Función `DataFrame -> DataFrame` aplicable con `DataFrame.transform`.
    """
    def _transform(df) -> Any:
        return df.withColumn(
            col_name,
            translate(
                col(col_name),
                "ÁÉÍÓÚÑáéíóúñÜü@",
                "AEIOUNaeiounUuN"
            )
        )
    return _transform


def normalizar_espacios(col_name:str) -> Callable[..., DataFrame]:
    """Factory de transformación que homologa separadores y colapsa espacios.

    Convierte los caracteres `-_/.,;:` en espacios, reduce secuencias de
    espacios a uno solo y aplica trim sobre la columna `col_name`.

    Args:
        col_name: Nombre de la columna a normalizar.

    Returns:
        Función `DataFrame -> DataFrame` aplicable con `DataFrame.transform`.
    """
    def _transform(df) -> Any:
        return df.withColumn(
            col_name,
            trim(
                regexp_replace(
                    regexp_replace(
                        col(col_name),
                        r"[-_/.,;:]+",      # caracteres que quieres tratar como espacio
                        " "
                    ),
                    r"\s+",                 # múltiples espacios → uno solo
                    " "
                )
            )
        )
    return _transform


mapping_RFC_CURP_KIND_PRIORITY = create_map([lit(x) for x in sum(RFC_CURP_KIND_PRIORITY.items(), ())])

########################################################################################################################
# Process
########################################################################################################################


class SubStep(ppf.Step):
    """Clase base para las sub-etapas del pipeline `CepsRfcNomRankingStep`.

    Hereda los atributos del step padre (rutas Hive, tratamiento de fechas,
    cohorte, etc.) mediante `ppf.inherit_parent_step_attributes`.
    """
    def __init__(self, parent: "CepsHistoryStep", *args, **kwargs) -> None:
        super().__init__(parent, *args, **kwargs)
        ppf.inherit_parent_step_attributes(self, parent)


class CepsRfcNomRankingStep(ppf.Step):
    """Step orquestador del pipeline de ranking RFC/CURP por nombre y cuenta (CEP).

    Encadena dos substeps: `CepsExtractStep` (extracción, limpieza y aplanado
    del historial CEP) y `CepsRankStep` (ranking del RFC/CURP canónico por
    cuenta y por nombre para los casos de reemplazo).
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
            step_name_prefix="Ceps_History_Step_",
            extra_kwargs=kwargs,
        )
        super().__init__(*args, **super_class_kwargs)

    def step_action(self) -> Dict[str, Any]:
        """Ejecuta el pipeline disparando el substep terminal (`ceps_rank_step`).

        Returns
        -------
        Dict[str, Any]
            Salida colectada del substep bajo la clave "ceps_rank_step".
        """
        return ppf.run_substep_and_collect(self, self.ceps_rank_step, "ceps_rank_step")
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
    @ppf.cached_property
    def ceps_extract_step(self) -> "CepsExtractStep":
        """Substep de extracción/preparación (lazy, cacheado).

        Returns
        -------
        CepsExtractStep
            Instancia del substep de extracción ligada a este step padre.
        """
        return CepsExtractStep(self)
    #
    @ppf.cached_property
    def ceps_rank_step(self) -> "CepsRankStep":
        """Substep de ranking (lazy, cacheado) con dependencia en la extracción.

        Returns
        -------
        CepsRankStep
            Instancia del substep de ranking; declara `ceps_extract_step` como
            `previous_step` para forzar el orden de ejecución.
        """
        return CepsRankStep(self, previous_step=[self.ceps_extract_step])


class CepsExtractStep(SubStep):
    """Substep de extracción y preparación del historial CEP (SPEI).

    Carga el historial crudo, normaliza nombres/RFC/cuentas, clasifica el tipo
    de identificador de ordenante y beneficiario y aplana el resultado a
    formato largo (`s264_ceps_flattened`).
    """
    def step_action(self) -> Dict[str, dict]:
        """Colecta el DataFrame aplanado (`s264_ceps_flattened`)."""
        return ppf.collect_step_output(self, self.s264_ceps_flattened, "s264_ceps_flattened")
    #
    @ppf.cached_property
    def s264_ceps(self) -> DataFrame:
        """Extrae el historial de CEPs (Comprobantes Electrónicos de Pago) desde Hive.

        Renombra las columnas del emisor con el sufijo `_ord` (ordenante) y las
        del receptor con `_ben` (beneficiario), y elimina la columna `tfrom`.
        """
        result:DataFrame = (
            self.standard_load_parquet_or_table("s264_ceps")
            .withColumnRenamed("nom_emisor", "nom_ord")
            .withColumnRenamed("id_ban_emi", "id_ban_ord")
            .withColumnRenamed("nom_ban_emi", "nom_ban_ord")
            .withColumnRenamed("id_ban_rec", "id_ban_ben")
            .withColumnRenamed("nom_ban_rec", "nom_ban_ben")
            .drop("tfrom")
        )
        return result

    @ppf.cached_property
    def s264_ceps_cleaned(self) -> DataFrame:
        """Limpia y normaliza los campos de texto del historial CEP.

        Aplica una cadena de transformaciones sobre `self.s264_ceps` para
        homologar nombres, RFCs y cuentas provenientes de distintos bancos,
        eliminando ruido que impide el matching posterior (sufijos espurios,
        formatos de nombre inconsistentes, placeholders de nulo, acentos, etc.).

        Transformaciones aplicadas (en orden)
        -------------------------------------
        1. **Normalización básica** (`norm`):
           - `trim` + `upper` + eliminación de sufijo de un solo carácter
             separado por espacio (artifact de Banamex, ej: "GUPL6105211K7 S" -> "GUPL6105211K7").
           - Se aplica a: `nom_ord`, `nom_ben`, `rfc_curp_ord`, `rfc_curp_ben`.
           - `cta_ord`, `cta_ben`: solo `trim`.

        2. **Null synonyms -> None**:
           - Nombres: valores en `NOM_NULL_SYNONYMS` (ej: "", "ND") -> null.
           - RFC/CURP: valores en `RFC_NULL_SYNONYMS` (ej: "", "ND",
             "RFC NO DISPONIBLE", "XEXX010101000") -> null.
           - Cuentas: cadena vacía -> null.

        3. **Reordenamiento de nombres Banamex** (`banamex_nom_reorder`):
           - Detecta nombres con `,` o `/` (formato Banamex: "APELLIDO,NOMBRE/APELLIDO2")
             y los convierte a `APELLIDO NOMBRE APELLIDO2` eliminando delimitadores.
           - Ejemplo: "LUIS ALBERTO,GUTIERREZ/PEREZ" -> "LUIS ALBERTO GUTIERREZ PEREZ".

        4. **Limpieza de acentos** (`limpiar_acentos`):
           - Transliteración: "ÁEIOÜN" -> "AEIOUN", elimina diéresis y á
             (sic) acentos.

        5. **Normalización de espacios** (`normalizar_espacios`):
           - Caracteres `-_/.,;:` -> espacio.
           - Múltiples espacios -> uno solo.
           - Trim final.

        Returns
        -------
        DataFrame
            Mismo schema que `s264_ceps` con valores de texto normalizados.

        Columnas (afectadas por la limpieza)
        ------------------------------------
        - nom_ord : str | null
            Nombre del ordenante normalizado. Null si era placeholder.
        - nom_ben : str | null
            Nombre del beneficiario normalizado. Null si era placeholder.
        - rfc_curp_ord : str | null
            RFC/CURP del ordenante sin sufijo espurio. Null si era placeholder.
        - rfc_curp_ben : str | null
            RFC/CURP del beneficiario sin sufijo espurio. Null si era placeholder.
        - cta_ord : str | null
            Cuenta del ordenante (trimmed). Null si estaba vacía.
        - cta_ben : str | null
            Cuenta del beneficiario (trimmed). Null si estaba vacía.

        Example
        -------
        Antes vs después de la limpieza:

        +----------------------------------+----------------------------------+
        | Antes (s264_ceps)                | Después (s264_ceps_cleaned)      |
        +----------------------------------+----------------------------------+
        | nom_ben: "LUIS ALBERTO,GUTIERREZ | nom_ben: "LUIS ALBERTO GUTIERREZ |
        |           /PEREZ"                |           PEREZ"                 |
        | rfc_curp_ben: "GUPL6105211K7 S"  | rfc_curp_ben: "GUPL6105211K7"    |
        | rfc_curp_ben: "RFC NO DISPONIBLE"| rfc_curp_ben: null               |
        | nom_ord: "ND"                    | nom_ord: null                    |
        +----------------------------------+----------------------------------+

        Sample Data
        -----------
        ``text
        +--------------+------------+---------------------------+---------------------------+---------------+---------------+-------------------+-------------------+----------+
        |cve_tipo_orden|customer_id |nom_ord                    |nom_ben                    |rfc_curp_ord   |rfc_curp_ben   |cta_ord            |cta_ben            |fec_oper  |
        +--------------+------------+---------------------------+---------------------------+---------------+---------------+-------------------+-------------------+----------+
        |R             |23274710    |LUIS ALBERTO GUTIERREZ PEREZ|LUIS ALBERTO GUTIERREZ PEREZ|GUPL6105211K7|GUPL6105211K7  |072500011749617958 |002600054749615340 |2024-09-02|
        |E             |112203971   |EDGAR RAYMUNDO MICHEL GONZALEZ|IVAN ALBERTO GOMEZ CRUZ |MIER7608120V9|null           |072200010046494208 |002650850587781970 |2024-09-02|
        |R             |144447678   |JJ PORQUE ES RENT A SA DE CV|COMERCIALIZADORA TR ZONE SA DE CV|JRR170407J01|CTZ190712VB1|012580001106787598|002650850587781970 |2024-09-02|
        |R             |141503886   |MARTIN FRANCISCO SASSO DE LA CRUZ|MARTIN FRANCISCO SASSO PULIDO|SACM900817VA|SAPM660801PP0|012794001532816552|5204166065239792|2024-09-02|
        |R             |6249936     |CONEJO AXOTLA RICARDO JOSUE|AMELCAR CRUZ SAMIENTO      |COAR9002192NA|CUSA650802SXXX |17218001399237995  |5204165890589696   |2024-09-02|
        +--------------+------------+---------------------------+---------------------------+---------------+---------------+-------------------+-------------------+----------+
        ```
        """
        s264_ceps:DataFrame = self.s264_ceps
        return (s264_ceps
            # normalización básica: trim + upper + eliminar sufijo de un solo carácter (Banamex)
            .withColumn("nom_ord", norm("nom_ord"))
            .withColumn("nom_ben", norm("nom_ben"))
            .withColumn("rfc_curp_ord", norm("rfc_curp_ord"))
            .withColumn("rfc_curp_ben", norm("rfc_curp_ben"))
            .withColumn("cta_ord", trim(col("cta_ord")))
            .withColumn("cta_ben", trim(col("cta_ben")))
            #
            # Null sinónimos para nombres: "", "ND"
            .withColumn("nom_ord", when(col("nom_ord").isin(NOM_NULL_SYNONYMS), None).otherwise(col("nom_ord")))
            .withColumn("nom_ben", when(col("nom_ben").isin(NOM_NULL_SYNONYMS), None).otherwise(col("nom_ben")))
            #
            # Null sinónimos para RFC/CURP: "", "ND", "RFC NO DISPONIBLE"
            .withColumn("rfc_curp_ord", when(col("rfc_curp_ord").isin(RFC_NULL_SYNONYMS), None).otherwise(col("rfc_curp_ord")))
            .withColumn("rfc_curp_ben", when(col("rfc_curp_ben").isin(RFC_NULL_SYNONYMS), None).otherwise(col("rfc_curp_ben")))
            .withColumn("cta_ord", when(col("cta_ord")=="", None).otherwise(col("cta_ord")))
            .withColumn("cta_ben", when(col("cta_ben")=="", None).otherwise(col("cta_ben")))
            #
            # Limpieza específica de Banamex: reordenar los nombres que vienen en
            # formato "APELLIDO,NOMBRE" o "NOMBRE/APELLIDO" y eliminar caracteres
            # problemáticos para el análisis como ",", "/", "-", "_", ";", ":"
            .transform(banamex_nom_reorder("nom_ord"))
            .transform(limpiar_acentos("nom_ord"))
            .transform(normalizar_espacios("nom_ord"))
            # Lo mismo para nom_ben
            .transform(banamex_nom_reorder("nom_ben"))
            .transform(limpiar_acentos("nom_ben"))
            .transform(normalizar_espacios("nom_ben"))
        )

    @ppf.cached_property
    @ppf.dynamic_partitioned_table_or_parquet(path_key="rfc_curp_analysis_s264_ceps")
    def rfc_curp_analysis_s264_ceps(self) -> DataFrame:
        """Clasifica el tipo de identificador (RFC/CURP/CLABE/TC/invalid) de cada transacción.

        Aplica `add_id_validation_flags` sobre `rfc_curp_ord` y `rfc_curp_ben` y
        añade las columnas de particionado `process_date`, `mis_date`,
        `vintage` y `cohort`.
        """
        s264_ceps_cleaned:DataFrame = self.s264_ceps_cleaned
        return (s264_ceps_cleaned
            .transform(lambda df: add_id_validation_flags(df, "rfc_curp_ord", "ord"))
            .transform(lambda df: add_id_validation_flags(df, "rfc_curp_ben", "ben"))
            .withColumn("process_date", lit(self.parent.date_treatment["process_date_str"]))
            .withColumn("mis_date", date_format(to_date(col("fec_oper"), DATE_STANDARD_SPARK_FORMAT), DATE_MONTH_SPARK_FORMAT))
            .withColumn("vintage", lit(self.parent.date_treatment["vintage"]).cast(StringType()))
            .withColumn("cohort", lit(self.cohort).cast(StringType()))
        )
    #
    @ppf.cached_property
    @ppf.dynamic_partitioned_table_or_parquet(path_key="s264_ceps_flattened")
    def s264_ceps_flattened(self) -> DataFrame:
        """Aplana la estructura ordenante/beneficiario a formato largo (una entidad por fila).

        Proyecta ordenante y beneficiario al esquema común `nom`, `cta`,
        `id_ban`, `tipo_cta`, `rfc_curp`, `rfc_curp_kind`, `is_rfc_curp_valid`,
        `fec_informacion`, `oper_mto`, `hora_oper`; los une con `unionByName` y
        añade las columnas de particionado.
        """
        rfc_curp_analysis_s264_ceps:DataFrame = self.rfc_curp_analysis_s264_ceps[0]
        #
        s264_ceps_flattened_ord = (rfc_curp_analysis_s264_ceps
            .select(
                col("nom_ord").alias("nom"),
                col("cta_ord").alias("cta"),
                col("id_ban_ord").alias("id_ban"),
                col("tipo_cta_ord").alias("tipo_cta"),
                col("rfc_curp_ord").alias("rfc_curp"),
                col("id_kind_ord").alias("rfc_curp_kind"),
                col("is_any_valid_ord").alias("is_rfc_curp_valid"),
                "fec_informacion",
                "oper_mto",
                "hora_oper"
            )
        )
        #
        s264_ceps_flattened_ben = (rfc_curp_analysis_s264_ceps
            .select(
                col("nom_ben").alias("nom"),
                col("cta_ben").alias("cta"),
                col("id_ban_ben").alias("id_ban"),
                col("tipo_cta_ben").alias("tipo_cta"),
                col("rfc_curp_ben").alias("rfc_curp"),
                col("id_kind_ben").alias("rfc_curp_kind"),
                col("is_any_valid_ben").alias("is_rfc_curp_valid"),
                "fec_informacion",
                "oper_mto",
                "hora_oper"
            )
        )
        #
        return (
            s264_ceps_flattened_ord.unionByName(s264_ceps_flattened_ben)
            .withColumn("process_date", lit(self.parent.date_treatment["process_date_str"]))
            .withColumn("mis_date", date_format(to_date(col("fec_informacion"), DATE_STANDARD_SPARK_FORMAT), DATE_MONTH_SPARK_FORMAT))
            .withColumn("vintage", lit(self.parent.date_treatment["vintage"]).cast(StringType()))
            .withColumn("cohort", lit(self.cohort).cast(StringType()))
        )


class CepsRankStep(SubStep):
    """Substep de ranking que determina el RFC/CURP canónico por cuenta y por nombre.

    Agrega el historial aplanado por combinación entidad-identificador, ordena
    los candidatos con las ventanas de prioridad de config y materializa los
    casos de reemplazo por cuenta (`rank_rfc_by_cta_cases_replace`) y por
    nombre (`rank_rfc_by_nom_cases_replace`).
    """
    def step_action(self) -> Dict[str, dict]:
        """Colecta los casos de reemplazo por nombre (`s264_ceps_flattened_rank_rfc_by_nom_cases_replace`)."""
        return ppf.collect_step_output(self, self.s264_ceps_flattened_rank_rfc_by_nom_cases_replace, "s264_ceps_flattened_rank_rfc_by_nom_cases_replace")
    #
    @ppf.cached_property
    @ppf.dynamic_partitioned_table_or_parquet(path_key="s264_ceps_flattened")
    def s264_ceps_flattened(self) -> DataFrame:
        """Recarga el DataFrame aplanado persistido por `CepsExtractStep`.

        Returns
        -------
        DataFrame
            `s264_ceps_flattened` sin la columna `tfrom`.
        """
        return (self.standard_load_parquet_or_table("s264_ceps_flattened")
            .drop("tfrom")
        )
        #   #
    @ppf.cached_property
    @ppf.dynamic_partitioned_table_or_parquet(path_key="s264_ceps_flattened_groupby")
    def s264_ceps_flattened_groupby(self) -> DataFrame:
        """Agrega el histórico aplanado por combinación única entidad-identificador.

        Agrupa por `cta`, `nom`, `id_ban`, `tipo_cta`, `rfc_curp` y
        `rfc_curp_kind` calculando el conteo de operaciones (`cnt`), el monto
        total (`tot_oper_mto`), la última fecha-hora de operación, la suma de
        validez del RFC, el flag de terminación "XXX" y la prioridad del tipo
        de identificador (`RFC_CURP_KIND_PRIORITY`).
        """
        s264_ceps_flattened:DataFrame = self.parent.ceps_extract_step.s264_ceps_flattened[0]
        #
        s264_ceps_flattened_prepared = (s264_ceps_flattened
            .withColumn("fec_informacion_hora_oper", concat_ws("#", col("fec_informacion"), col("hora_oper")))
            .drop("fec_informacion", "hora_oper")
            .withColumn("rfc_curp_ends_with_xxx", when(col("rfc_curp").endswith("XX"), 1).otherwise(0))
            .withColumn("RFC_CURP_KIND_PRIORITY", mapping_RFC_CURP_KIND_PRIORITY[col("rfc_curp_kind")])
        )
        #
        return (s264_ceps_flattened_prepared
            .groupBy("cta", "nom", "id_ban", "tipo_cta", "rfc_curp", "rfc_curp_kind")
            .agg(
                spark_count("*").alias("cnt"),
                spark_sum("oper_mto").alias("tot_oper_mto"),
                spark_max("fec_informacion_hora_oper").alias("lst_fec_informacion_hora_oper"),
                spark_sum("is_rfc_curp_valid").alias("is_rfc_curp_valid"),
                spark_max("rfc_curp_ends_with_xxx").alias("rfc_curp_ends_with_xxx"),
                spark_max("RFC_CURP_KIND_PRIORITY").alias("RFC_CURP_KIND_PRIORITY")
            )
            .withColumn("fec_informacion", split(col("lst_fec_informacion_hora_oper"), "#").getItem(0))
            .withColumn("hora_oper", split(col("lst_fec_informacion_hora_oper"), "#").getItem(1))
            .withColumn("process_date", lit(self.parent.date_treatment["process_date_str"]))
            .withColumn("mis_date", date_format(to_date(col("fec_informacion"), DATE_STANDARD_SPARK_FORMAT), DATE_MONTH_SPARK_FORMAT))
            .withColumn("vintage", lit(self.parent.date_treatment["vintage"]).cast(StringType()))
            .withColumn("cohort", lit(self.cohort).cast(StringType()))
        )
        #
    @ppf.cached_property
    @ppf.dynamic_partitioned_table_or_parquet(path_key="s264_ceps_flattened_ranks")
    def s264_ceps_flattened_ranks(self) -> DataFrame:
        """Rankea los RFC/CURP candidatos por cuenta y por nombre para elegir el canónico.

        Añade los conteos acumulados por cuenta (`cnt_rfc_by_cta`) y por nombre
        (`cnt_rfc_by_nom`) y los rankings `rank_rfc_by_cta` / `rank_rfc_by_nom`
        (row_number sobre las ventanas de prioridad de config), dejándolos en
        null cuando la clave de partición (`cta`/`nom`) es nula.
        """
        s264_ceps_flattened_groupby:DataFrame = self.s264_ceps_flattened_groupby[0]
        return (s264_ceps_flattened_groupby
            # window aplication
            .withColumn("cnt_rfc_by_cta", spark_sum("cnt").over(Window.partitionBy("cta")))
            .withColumn("cnt_rfc_by_nom", spark_sum("cnt").over(Window.partitionBy("nom")))
            # rank
            .withColumn("rank_rfc_by_cta", row_number().over(RFC_BY_CTA_PRIORITY_WINDOW))
            .withColumn("rank_rfc_by_cta", when(col("cta").isNull(), None).otherwise(col("rank_rfc_by_cta")))
            #
            .withColumn("rank_rfc_by_nom", row_number().over(RFC_BY_NOM_PRIORITY_WINDOW))
            .withColumn("rank_rfc_by_nom", when(col("nom").isNull(), None).otherwise(col("rank_rfc_by_nom")))
        )
        #
    @ppf.cached_property
    @ppf.dynamic_unpartitioned_parquet(path_key="s264_ceps_flattened_rank_rfc_by_cta_cases_replace")
    def s264_ceps_flattened_rank_rfc_by_cta_cases_replace(self) -> DataFrame:
        """Casos de reemplazo de RFC por cuenta: mejor candidato por (cta, nom, id_ban) y por (cta, id_ban).

        Conserva solo filas con `rfc_curp` y `cta` no nulos, calcula los
        rankings `_rank_by_nom_id_ban` y `_rank_by_id_ban` (row_number ordenado
        por `rank_rfc_by_cta`) y elimina las columnas auxiliares de
        agregación/ranking.
        """
        s264_ceps_flattened_ranks:DataFrame = self.s264_ceps_flattened_ranks[0]
        #
        # REPLACE MISSING RFC CURP WITH VALID cta, id_ban  & marching nom
        cta_nom_id_ban_best_rfc_window = (
            Window
            .partitionBy("cta", "nom", "id_ban")
            .orderBy(col("rank_rfc_by_cta").asc_nulls_last())
        )
        #
        cta_id_ban_best_rfc_window = (
            Window
            .partitionBy("cta", "id_ban")
            .orderBy(col("rank_rfc_by_cta").asc_nulls_last())
        )
        #
        return (
            s264_ceps_flattened_ranks
            .filter(col("rfc_curp").isNotNull())                            # solo filas con RFC válido
            .filter(col("cta").isNotNull())                                 # evitar pares (null, id_ban)
            # .filter(col("nom").isNotNull())                               # evitar pares (null, id_ban)
            .withColumn(
                "_rank_by_nom_id_ban",
                row_number().over(cta_nom_id_ban_best_rfc_window)
            )
            .withColumn(
                "_rank_by_id_ban",
                row_number().over(cta_id_ban_best_rfc_window)
            )
            .drop("tipo_cta", "hora_oper","cnt_rfc_by_nom", "rank_rfc_by_nom",
                "cnt", "tot_oper_mto", "lst_fec_informacion_hora_oper",
                "is_rfc_curp_valid", "rfc_curp_ends_with_xxx",
                "RFC_CURP_KIND_PRIORITY", "cnt_rfc_by_cta", "process_date",
                "mis_date", "fec_informacion")
                .drop("tfrom")
        )
        #
    @ppf.cached_property
    @ppf.dynamic_unpartitioned_parquet(path_key="s264_ceps_flattened_rank_rfc_by_nom_cases_replace")
    def s264_ceps_flattened_rank_rfc_by_nom_cases_replace(self) -> DataFrame:
        """Casos de reemplazo de RFC por nombre: mejor candidato por (nom, id_ban).

        Conserva solo filas con `rfc_curp` y `nom` no nulos, calcula
        `_rank_by_id_ban` (row_number por `nom`+`id_ban` ordenado por
        `rank_rfc_by_nom`) y elimina las columnas auxiliares de
        agregación/ranking.

        Ejemplo de salida:

        +-------------------+--------------------------+-------+-------------+-------------+-------+---------------+----------------+
        |cta                |nom                       |id_ban |rfc_curp     |rfc_curp_kind|tfrom  |rank_rfc_by_nom|_rank_by_id_ban |
        +-------------------+--------------------------+-------+-------------+-------------+-------+---------------+----------------+
        |021580040627537774 |*CASAS JAVER SA DE CV     |40021  |CJA961219KJ0 |rfc_moral    |0      |1              |1               |
        |002180002896125057 |017500110033222025        |40012  |REEMB SUB C41|invalid      |0      |2              |1               |
        |036180500581949948 |18 PIEDRAS SC             |40036  |DPI191216AI0 |rfc_moral    |0      |1              |1               |
        |072180013031773500 |2213 INMOBILIARIA SA DE CV|40072  |DMD241212DF6 |rfc_moral    |0      |1              |1               |
        |042180016006575579 |23 45 AIR TIME S DE RL    |40042  |VCC110908048 |rfc_moral    |0      |1              |1               |
        +-------------------+--------------------------+-------+-------------+-------------+-------+---------------+----------------+
        """
        s264_ceps_flattened_ranks:DataFrame = self.s264_ceps_flattened_ranks[0]
        s264_ceps_flattened_rank_rfc_by_cta_cases_replace:DataFrame = self.s264_ceps_flattened_rank_rfc_by_cta_cases_replace[0]
        #
        cta_id_ban_best_rfc_window = (
            Window
            .partitionBy("nom", "id_ban")
            .orderBy(col("rank_rfc_by_nom").asc_nulls_last())
        )
        return (
            s264_ceps_flattened_ranks
            .filter(col("rfc_curp").isNotNull())            # solo filas con RFC válido
            .filter(col("nom").isNotNull())                 # evitar pares (null, id_ban)
            .withColumn(
                "_rank_by_id_ban",
                row_number().over(cta_id_ban_best_rfc_window)
            )
            .drop("tipo_cta", "hora_oper","cnt_rfc_by_nom", "rank_rfc_by_cta",
                "cnt", "tot_oper_mto", "lst_fec_informacion_hora_oper",
                "is_rfc_curp_valid", "rfc_curp_ends_with_xxx",
                "RFC_CURP_KIND_PRIORITY", "cnt_rfc_by_cta", "process_date",
                "mis_date", "fec_informacion")
                .drop("tfrom")
        )
