# CEPS

Notas sobre el proceso de CEPS (Comprobantes Electrónicos de Pago, SPEI).

## Pipeline `rfc_nom_ranking`

Step: `CepsRfcNomRankingStep` (`pipelines/ceps/rfc_nom_ranking.py`), config en
`config/ceps/rfc_nom_ranking.py`. Dos substeps encadenados:

| Substep | Salida | Qué hace |
|---------|--------|----------|
| `CepsExtractStep` | `s264_ceps` | Historial crudo CEP: renombra emisor->`_ord`, receptor->`_ben` |
| | `s264_ceps_cleaned` | Limpieza de texto: `norm` (trim+upper+recorta última letra), null synonyms (`""`, `"ND"`, `"RFC NO DISPONIBLE"`), `banamex_nom_reorder` (`,`,`/` -> espacio), `limpiar_acentos`, `normalizar_espacios` (`-_/.,;:` -> espacio) |
| | `rfc_curp_analysis_s264_ceps` | `add_id_validation_flags` clasifica `rfc_curp_*`: curp / rfc_fisica / rfc_moral / rfc_fisica_siinvalid |n_homoclave / tc (16 díg) / clabe (18 díg) / 
| | `s264_ceps_flattened_monthly` | Aplana ord+ben a formato largo: `nom, cta, id_ban, tipo_cta, rfc_curp, rfc_curp_kind, is_rfc_curp_valid, fec_informacion, fst_fec_informacion_hora_oper, oper_mto, hora_oper, source` — **all-time incremental** por `month_partition` (reemplaza a la antigua salida `s264_ceps_flattened` de ventana corta) |
| `CepsRankStep` | `s264_ceps_flattened_groupby_monthly` | Parciales mensuales combinables del group-by (misma maquinaria incremental de `group_by`) |
| | `s264_ceps_flattened_groupby` | Agrupa recombinando TODAS las parciales mensuales por (cta, nom, id_ban, tipo_cta, rfc_curp, rfc_curp_kind, source): cnt, tot_oper_mto, lst_fec_informacion_hora_oper, RFC_CURP_KIND_PRIORITY, SOURCE_PRIORITY |
| | `s264_ceps_flattened_ranks` | + `cnt_rfc_by_cta/nom`, `rank_rfc_by_cta/nom` (row_number sobre ventanas de config) |
| | `s264_ceps_flattened_rank_rfc_by_{cta,nom}_cases_replace` | Mejor RFC por (cta, nom, id_ban) / (nom, id_ban); parquet simple que consume `txn_replacement` |

Prioridad de tipo de id (`RFC_CURP_KIND_PRIORITY`): rfc_moral=0 < rfc_fisica=1 <
curp=2 < rfc_fisica_sin_homoclave=3 < clabe=4 < tc=5 < invalid=6.

### Prioridad al tag viejo válido (`PREFER_OLDEST_VALID_TAG = True`)

Las ventanas `RFC_BY_{CTA,NOM}_PRIORITY_WINDOW` primero descartan nulos y
ordenan por validez (un RFC/CURP válido siempre gana a uno inválido o débil)
y luego —con `PREFER_OLDEST_VALID_TAG`— premian la **antigüedad** del tag: a
igual validez gana el `fst_fec_informacion_hora_oper` (primera fecha de
información del tag) más antiguo. Un RFC viejo y válido tiene prioridad sobre
uno recién aparecido.

## Catálogo Banxico `bxico_rfc_curp_cat`

Catálogo oficial de clientes que provee Banxico. Esquema:
`nombre_1, nombre_2, apellido_paterno, apellido_materno, curp, rfc, domicilio,
cta, banco_cta (nombre del banco), banco_reporte, fecha_de_subida`.

### Integración (en `CepsRankStep`, alimentando `s264_ceps_flattened_ranks`)

- **Input opcional**: `input["bxico_rfc_curp_cat"]` en config; se carga con
  `SubStep.optional_input` -> `None` si la tabla no existe o falla la carga y
  el pipeline sigue normal. Nombre de tabla placeholder sobreescribible con
  `MINERVA_BXICO_RFC_CURP_CAT_TABLE` (TODO: actualizar cuando Banxico publique
  el real). Partición `fecha_de_subida`, history=12m, `minimum_required_history=False`.
- **`prepare_bxico_catalog`** (función en el pipeline):
  - Dedup por `cta`: gana la `fecha_de_subida` más reciente (desempate rfc/curp).
  - Limpieza de nombres = MISMO proceso que s264 (`norm` -> null synonyms ->
    banamex -> acentos -> espacios) en cada parte; `nom` se arma con
    `merge_name_parts`, que NO duplica partes ya contenidas (nombre_1+nombre_2
    juntos, todo en nombre_1, ambos apellidos en apellido_paterno).
  - `rfc_curp` = RFC válido > CURP válido > primer no-nulo; validado con los
    mismos patrones vía `add_id_validation_flags`.
  - `id_ban` cruzando `banco_cta` con `build_ban_catalog` (id_ban<->nom_ban
    derivado de s264, clave normalizada acentos+espacios+upper).
  - Proyección al esquema aplanado + `source="bxico_rfc_curp_cat"`,
    `oper_mto=0.0` (tipo_cta/hora_oper/particiones llegan null por
    `unionByName(allowMissingColumns)`).
- **Columna `source`**: `s264_ceps` | `bxico_rfc_curp_cat`; entra al groupBy y
  sobrevive hasta los `cases_replace` (trazabilidad del origen).
- **Prioridad Banxico**: `SOURCE_PRIORITY` (bxico=0, s264=1) ordena las
  ventanas `RFC_BY_{CTA,NOM}_PRIORITY_WINDOW` justo tras las guardas de nulos:
  un candidato bxico con RFC válido siempre gana a s264; entre alternativas
  bxico mandan los demás criterios.

Tests: `tests/test_bxico_catalog.py` (merge de nombres, catálogo de bancos,
dedup, elección de rfc_curp, optional_input, prioridad en ventanas).

## Pipeline `txn_replacement` — historias independientes

Step: `CepsTxnReplacementStep` (`pipelines/ceps/txn_replacement.py`), config
en `config/ceps/txn_replacement.py`.

Dos ventanas de historia **independientes**:

| Constante | Valor | Controla |
|---|---|---|
| `CEPS_RANKING_HISTORY_IN_MONTHS` (rfc_nom_ranking) | 24 | meses de historia CEP leídos para construir los catálogos de reemplazo (`s264_ceps` input) |
| `TXN_REPLACED_HISTORY_IN_MONTHS` (txn_replacement) | 3 | meses de transacciones que conserva `rfc_curp_analysis_s264_ceps_replaced` (la tabla que alimenta el grafo) |

- El input `rfc_curp_analysis_s264_ceps` del step de reemplazo lee solo la
  partición del vintage (`history=1`): cada partición ya contiene la ventana
  completa del ranking.
- `limit_txn_history_window` (en el pipeline) recorta las txns por
  `fec_informacion` al intervalo `[vintage-lag-history+1m, vintage-lag+1m)`,
  con la misma aritmética que `make_date_interval_with_lag_months`.
- Como la partición de salida lleva solo 3 meses, el grafo sigue viendo la
  ventana completa acumulando las particiones de los últimos vintages (como
  hace `history` en los loaders de `edges_and_nodes`).

### Catálogo de imputación all-time e incremental

La ventana de 24 meses (`CEPS_RANKING_HISTORY_IN_MONTHS`) controla solo la
**lectura cruda**; los parciales aplanados/agrupados son **all-time** y viven
en una **raíz fija compartida entre vintages**
(`config/job.py::general_root_hdfs_fixed` → `.../ceps_history/all_time/`,
sin el segmento del vintage — si vivieran bajo la raíz del vintage no
acumularían nada entre corridas):

- `s264_ceps_flattened_monthly` y `s264_ceps_flattened_groupby_monthly` se
  guardan por `month_partition` con `CEPS_ALL_TIME_HISTORY_MONTHS = 1200` al
  releer. Cada corrida calcula **solo los meses faltantes** del vintage
  (`ensure_monthly_partitions`) y recombinan los parciales ya guardados
  (`combine_ceps_groupby`): mes a mes solo se escribe el incremento, no se
  recalcula la historia.
- Los meses esperados se derivan de las particiones de la fuente **acotadas
  al vintage** (`s264_available_months` corta en `vintage - lag`) y las
  lecturas usan la ventana del vintage: un vintage VIEJO ejecutado tras uno
  nuevo en la misma ubicación no ve los meses de su futuro (testeado en
  `tests/test_reload_resilience.py`).
- Las **vistas por vintage** (`s264_ceps_flattened_groupby`, `_ranks`,
  `cases_replace`, `rfc_curp_analysis_s264_ceps_replaced`) SIGUEN bajo la
  raíz del vintage: codifican "el mundo conocido a ese vintage" y no deben
  compartirse entre corridas de vintages distintos.
- La antigua salida `s264_ceps_flattened` (ventana corta) ya no se
  materializa.

### Marcadores de commit (anti-corrupción tras interrupciones)

Si el proceso muere a mitad de una escritura, la partición queda con archivos
parciales y un `_SUCCESS` raíz obsoleto de la escritura anterior: una recarga
la leería corrupta sin error. Para evitarlo, toda escritura exitosa crea un
marcador vacío `<tabla>/_committed/<info_col>=<v>/<process_date=<v>` y la
recarga **solo confía en particiones con marcador** — las que no lo tienen se
borran y recomputan (`libs/framework`: `write_commit_marker`,
`is_pair_committed`, usados por `dynamic_partitioned_table_or_parquet` y
`ensure_monthly_partitions`). Para parquets simples basta exigir `_SUCCESS`
(`_reload_or_recompute_parquet`). Migración: las particiones preexistentes
sin marcador se recomputan una vez; si una tabla no puede permitirse ese
recomputo, su config acepta `"trust_legacy_partitions": True`.

### Salida `mapeo_cta_numcliente`

Además de `rfc_curp_analysis_s264_ceps_replaced`, el step materializa
`mapeo_cta_numcliente` (una fila por cta → numcliente, construida con
`build_mapeo_cta_numcliente` sobre las txns reemplazadas). Es el mapeo que usa
la **variante `cta`** del grafo para imputar el numcliente al final; si algún
día existe otra tabla oficial de mapeo basta reapuntar la clave
`mapeo_cta_numcliente` del input de esa variante — el resto del flujo no
cambia.

Tests: `tests/test_txn_history_window.py`.

## Nota: `norm` corregido

`norm` = `upper(regexp_replace(trim(col), "\s+[A-Z]$", ""))` solo elimina un
sufijo de UNA letra separado por espacio (artefacto Banamex "GUPL6105211K7 S"
-> "GUPL6105211K7"). Antes recortaba la última letra de cualquier string
("PEREZ"->"PERE"); corregido — si existen parquets generados con la versión
antigua, sus nombres/rfc_curp conservan el truncado histórico.
