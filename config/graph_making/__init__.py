# Nº de meses de historia transaccional que conforma la ventana del grafo: se usa
# como "history" en la carga de insumos y en la relectura de las salidas de cada
# step (special_treatment, group_by, edges_and_nodes).
GRAPH_TOTAL_HISTORY_IN_MONTHS = 12
# Lag global (en meses) aplicado a la ventana del grafo respecto a `vintage_date`:
# 0 = la ventana termina en el mes de la vintage; >0 la desplaza hacia atrás.
GRAPH_GLOBAL_LAG = 0
