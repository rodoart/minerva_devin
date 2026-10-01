# Ejemplo de uso del framework

Mini-proyecto completo que muestra cómo montar un pipeline ETL reanudable
sobre `libs/framework` + `libs/data_engineering_toolbox` + `pipelines/`
(clases `Standard*`).

```
examples/
  config/job.py               date_treatment (vintage, process_date) + raíz HDFS
  config/transactions_etl.py  dicts input/output de cada step
  pipelines/transactions_etl.py  ExampleExtractStep + ExampleGroupByStep
  main.py                     driver: SparkSession + cadena de steps
```

## Flujo del ejemplo

```
raw_txns (parquet particionado information_date/process_date)
  -> ExampleExtractStep       select + tfrom_months/tfrom_days + missing treatment
       -> txns_clean          (partición mis_date=vintage, process_date=hoy)
  -> ExampleGroupByStep
       -> txns_monthly        parciales por arista-mes (SOLO meses ausentes)
       -> txns_grouped        agregado de la ventana combinando parciales
```

## Cómo ejecutarlo

```bash
# Sesión local (sin cluster): las rutas table_or_hdfs pueden apuntar a /tmp
python examples/main.py --local --build-only   # smoke test
python examples/main.py --local                # ejecuta (necesita raw_txns)

# En cluster: carga primero las variables de entorno
source opt/environment_vars.sh
python examples/main.py
```

Para generar un `raw_txns` de juguete en local:

```python
spark.createDataFrame(
    [("a", "b", 100.0, "2025-07-05", "spei"),
     ("a", "c",  50.0, "2025-08-12", "spei"),
     ("b", "c",  10.0, "2025-08-20", "atm")],
    ["src", "dst", "amount", "event_date", "channel"],
).withColumn("information_date", col("event_date")) \
 .withColumn("process_date", lit("2025-09-01")) \
 .write.partitionBy("information_date", "process_date") \
 .parquet("/tmp/framework_example/<vintage>/raw_txns")
```

## Qué hay que tocar para adaptarlo

1. **`config/job.py`** — `date_treatment` (claves que consume
   `build_step_init_kwargs`), `COHORT`, `IS_DYNAMIC`, raíz de salida.
2. **`config/transactions_etl.py`** — dicts `input`/`output`: rutas, columnas
   de partición, `lag`/`history` (ventana), `select`, `keep_or_delete`.
3. **`pipelines/`** — subclases de las `Standard*`: `step_action` +
   `@cached_property` por cada DataFrame materializable. Los decoradores
   `dynamic_*` dan la reanudabilidad; `ensure_monthly_partitions` da la
   incrementalidad mensual.
4. **`main.py`** — encadena los steps con `previous_step=[...]` y ejecuta el
   último.
5. **Producción** — lanzar bajo `supervisor.py` (reinicio ante OOM/zombie):
   `SUPERVISOR_COMMAND="python examples/main.py" ./opt/run-supervised.sh`.
