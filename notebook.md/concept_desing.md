# Minerva

Este es un proyecto para un banco que consiste en crear un Pieline DataPull para generar features de grafos basado en Pyspark lo más importante es el uso de la librería graphframes, sacar todo lo posible de esta librería. Las features finales deben estar a nivel numcliente y estar almacenadas en un VectorAssebler para los modelos

El proyecto ya está pparcialmente programado en python. escanea todas las capturas y copia la distribución actual.


## Concepto

Todo el proceso se ejecuta en una premisa dinámica, cada paso es guardado como un parquet o en una partición de una tabla. Es muy importante que si el proceso es interrumpido, se pueda reanudar fácilmente porque dentro del Clúster del banco, se interruumpe continuamente para dar a procesos de mayor prioridad, por lo que cada ETL debe ser lo más independiente posbile. (rutas almacenadas, etc).

El proceso debe ser lo más universal posible, altamente parámétrico y extendible.

La idea es que se calculen los features una vez por mes, con una cantidad limitad de historia (6 meses, 12 meses etc.)

### CEPS

Es una base de datos temporal que está incompleta y sucia, trabaja mejor a nivel RFC (Registro Federal de Contribuyentes). El RFC es universal en México para Personas y Negocios, tanto si son clientes del banco como sino, preferimos utilizar este dato para los grafos justo por esta unicidad y universalidad. También está la cuenta, que es un número indpendiente del banco y finalmente, el numcliente, que sólo es para los cliente internos del banco.

La tabla de CEPS se actualiza por un proceso externo tiene:
- cve_tipo_orden (str): E si nuestro banco es el emisor (R) si es el receptor.
- customer_id (str): es el numcliente, siempre es el del cliente emisor o receptor, cambia según el caso de cve_tipo_orden
- opa_cve (int): es una clave bancaria de banxico
- fecha_oper (str) la fecha de operación en formato estandard yyyy-mm-dd
- hora_oper (int) hora de 24 horas en formato hhmmss (la hora si es empieza en 0, los quita)
- oper_mto (str) monto de la tranferencia
- id_ban_ben (int) clave fija del banco beneficiario
- id_ban_ord (int) clave fija del banco ordenante.
- nom_ord (str) nombre de ordenante
- nom_ben (str) nombre de beneficiario
- cta_ben (str) cuenta del beneficiario
- cta_ord (str) cuenta del ordenantne
- rfc_curp_ord (str) rfc o curp del ordenante 
- rfc_curp_ben (str) rfc o curp del beneficiario
- fec_información (str) fecha de partición diiaria en yy-mm-dd
- ..
Es importante que todo sea nullable.

Se sigue un criterio cuidadoso para elgir el rfc o curp correctos.

### Lovelace

Es un modelo de predicción de ejemplo, la base de datos está a nivel txn, por lo que se agrega a num_cliente, cuenta antes de hacer el join. Podría haber casos que simpleemnte estén ya num_cliente.

- num_cliente (str): numero interno del cliente
- dispute_amt (float): cantidad de la txn
- beneficiaryaccountnumber (str): cta de destino
- num_oper (int) numero de la opreción
- ft_tmx_proba (float) TTarget a propagar.
- fecha_s015 (str) fecha de txn oficial en formato yyyy-mm-dd
- mis_date (int) mes de txn en formato yyyymm
- ...


### Partes

1. Extracción especial dependiendo de los datos RAW
    - Ejemplo y caso de uso: CEPS pero en teoría podria ser cualqueir base de datos a nivel TXN
2. Special Treatment:
    - Primer paso real del proceso de Minerva, consiste en el aplanamiento de las transacciones.
        - Todo el proceso debe trabajar con tfrom_months, tfrom_days que son meses desde la fecha actual (0 si es el mismo mes día)
    - Missing Treatment.
        - Se quitan los nulos
3. GroupBy:
    - En esta parte se agrupan los datos por txn (src-dst) y por llave (src)
        - Se separan los ids de origen y de destino.
        - Se juntan y se agrupan por id, se sacan métricas utiles, (como la oper_mto, total)
            - Conservamos también datos que podrían permitir la identificación del cliente como ArrayType
                - numcliete, nom, cta, id_ban, mis_date 
        - Se agrupan también por txn.
            - Se calculan features a nivel TXN que al finan se usarán para calcular más features a nivel numcliente, pero más importante, para definir los pesos que se usan en la función de pesos.

4. EdgesAndNodes
    - Se calculan los nodos y las aristas del grafo.
    - Aristas:
        - Incluye todos los datos a nivel txn txn, ppero también tiene un array de los diferentes tipos de pesos que se ppueden utilizar ppara construir el grafo.
    - Nodos:
        Por el momento simplemente replica el resultado final 
5. GraphFeatures:
    - Son las Features principales del grafo, son con y sin pesos.
6. Propagation Step:
    - se usan diferentes targets con valor de 0 a 1. La source actual es lovelace, pero podría haber múltiples targets.
    - la idea es que cada combinación de raw con target tenga sus features. en este caso es:
        - Lovelace (un modelo) x CEPS (una soruce)
        - Esta parte ayudame a simplificar.
    - Special Treatment
        - Cada target sigue su proceso de Special Treatment y Missing Treatment.
        - La target se puede proppagar a nivel cta o nivel cliente
    - Join Target
        La target se une a los nodos del grafo. siguiendo criterios. También se hace un missing treatment que debe ser parametrizable.
7. PropagationFeatures en proceso
    - La target se normaliza para propagarla.
    - Actualmente hay ya una proppuesta de contagio, pero hay que añadir todas las fetures posibles con contagio.
    - Hay que definir que Features se crearan en el proceso.
8. Feautures de Agrupación
    - no lo desarrollemos aún, pero deja el espacio.
8. Vector Assembler pendiente
    - Dado un pivote externo (lista de numclientes) se tiene que crear vectores con todas las features para entrenar modelos.
    - Como la base de datos no está nivel nucliente, se busca en el array de Nodes que numcliente son, se hace un explode y se utiliza una estadistica final media ponderarada por txn_monto_antiguedad_en_días o algo así (parametrizable), para obtener una features finales a nivel num_cliente.



## Ambiente virtual

Uso de los jars:

```text
graphframes-0.81-spark3.0-s2.12.jar
```

```yaml
name:
  minerva_devin
dependencies:
  - python=3.10.18
  - packaging=24.2  
  - spark=3.3.2.3
  - pip
  - cxx-compiler
  - abseil-cpp
  - google-re2
  - protobuf=4.23.4
  - postgresql=15
  - conda-pack
  - pytest=8.3.*
  - sqlalcehmy>=1.4.36,<2.0
  - pandas>=1.5.3,<2.0
  - plotly
  - pyarrow>=10.0.1,<11.0
  - regex
  - graphframes=0.81
  - pip:
    - graphframes
```

Se pueden agregar otras librerías requeridas, pero estas versiones deben ser preservadas.

