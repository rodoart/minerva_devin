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
| | `s264_ceps_flattened` | Aplana ord+ben a formato largo: `nom, cta, id_ban, tipo_cta, rfc_curp, rfc_curp_kind, is_rfc_curp_valid, fec_informacion, oper_mto, hora_oper, source` |
| `CepsRankStep` | `s264_ceps_flattened_groupby` | Agrupa por (cta, nom, id_ban, tipo_cta, rfc_curp, rfc_curp_kind, source): cnt, tot_oper_mto, lst_fec_informacion_hora_oper, RFC_CURP_KIND_PRIORITY, SOURCE_PRIORITY |
| | `s264_ceps_flattened_ranks` | + `cnt_rfc_by_cta/nom`, `rank_rfc_by_cta/nom` (row_number sobre ventanas de config) |
| | `s264_ceps_flattened_rank_rfc_by_{cta,nom}_cases_replace` | Mejor RFC por (cta, nom, id_ban) / (nom, id_ban); parquet simple que consume `txn_replacement` |

Prioridad de tipo de id (`RFC_CURP_KIND_PRIORITY`): rfc_moral=0 < rfc_fisica=1 <
curp=2 < rfc_fisica_sin_homoclave=3 < clabe=4 < tc=5 < invalid=6.

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

## Nota: `norm` corregido

`norm` = `upper(regexp_replace(trim(col), "\s+[A-Z]$", ""))` solo elimina un
sufijo de UNA letra separado por espacio (artefacto Banamex "GUPL6105211K7 S"
-> "GUPL6105211K7"). Antes recortaba la última letra de cualquier string
("PEREZ"->"PERE"); corregido — si existen parquets generados con la versión
antigua, sus nombres/rfc_curp conservan el truncado histórico.
