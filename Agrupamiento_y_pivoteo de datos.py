"""
AE6 - Agrupamiento y pivoteo de datos (Pandas)
Dataset: e-commerce (Online Retail-like)
Columnas esperadas:
InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country

1) Carga y exploración del dataset
2) Agrupamiento (groupby) con métricas y .agg()
3) Pivotado (pivot_table) y despivotado (melt)
4) Combinación (concat) y fusión (merge) con distintos joins
5) Exportación a CSV y Excel
"""

import pandas as pd


# -----------------------------
# 1) CARGA Y EXPLORACIÓN
# -----------------------------
RUTA_CSV = "data.csv"          # cambia si tu archivo tiene otro nombre/ruta
ENCODING = "latin1"            # útil si tu CSV no está en utf-8 (como te pasó)

df = pd.read_csv(RUTA_CSV, encoding=ENCODING)

print("Dataset cargado:", df.shape)
print("\nPrimeras 5 filas:")
print(df.head())

print("\nInfo general:")
print(df.info())

print("\nValores nulos por columna:")
print(df.isna().sum().sort_values(ascending=False))


# Limpieza mínima para trabajar mejor en el ejercicio
# (en la guía piden explorar, pero para agrupar/pivotear conviene tener esto)
df = df.drop_duplicates()
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
df["CustomerID"] = df["CustomerID"].astype("Int64")  # Int con nulos
df["TotalLine"] = df["Quantity"] * df["UnitPrice"]   # métrica útil para análisis

print("\nDespués de drop_duplicates + conversiones básicas:", df.shape)


# -----------------------------
# 2) AGRUPAMIENTO DE DATOS (groupby)
# -----------------------------
# Ejemplo 1: agrupar por Country y calcular promedio, suma y conteo
group_country = df.groupby("Country").agg(
    avg_unit_price=("UnitPrice", "mean"),
    sum_quantity=("Quantity", "sum"),
    count_rows=("InvoiceNo", "count"),
    sum_total=("TotalLine", "sum")
).sort_values("sum_total", ascending=False)

print("\nAgrupamiento por Country (avg, sum, count, sum_total):")
print(group_country.head(10))

# Ejemplo 2: función personalizada con .agg()
# cantidad de facturas únicas por país (InvoiceNo únicos)
group_country_custom = df.groupby("Country").agg(
    invoices_unique=("InvoiceNo", "nunique"),
    customers_unique=("CustomerID", "nunique")
).sort_values("invoices_unique", ascending=False)

print("\nAgrupamiento por Country (función personalizada nunique):")
print(group_country_custom.head(10))

# Guardamos estos resúmenes como DataFrames con columna Country
group_country_reset = group_country.reset_index()
group_country_custom_reset = group_country_custom.reset_index()


# -----------------------------
# 3) PIVOTADO Y DESPIVOTADO
# -----------------------------
# Pivot table: ejemplo ventas totales por país y mes
# (si InvoiceDate tiene nulos, se excluyen en esta tabla)
df_pivot_base = df.dropna(subset=["InvoiceDate"]).copy()
df_pivot_base["YearMonth"] = df_pivot_base["InvoiceDate"].dt.to_period("M").astype(str)

pivot_sales = df_pivot_base.pivot_table(
    index="Country",
    columns="YearMonth",
    values="TotalLine",
    aggfunc="sum",
    fill_value=0
)

print("\nPivot table (TotalLine por Country y YearMonth) - muestra:")
print(pivot_sales.head())

# Despivotado con melt: convertir la pivot table a formato largo
pivot_sales_reset = pivot_sales.reset_index()
melt_sales = pivot_sales_reset.melt(
    id_vars=["Country"],
    var_name="YearMonth",
    value_name="TotalLine_sum"
)

print("\nMelt del pivot (formato largo) - muestra:")
print(melt_sales.head())


# -----------------------------
# 4) COMBINACIÓN Y FUSIÓN DE DATOS
# -----------------------------
# 4.1 Concat: unir dos DataFrames (ejemplo, top 100 registros de 2 países si existen)
countries = df["Country"].dropna().unique().tolist()

df_a = df[df["Country"] == countries[0]].head(100).copy()
df_b = df[df["Country"] == countries[1]].head(100).copy() if len(countries) > 1 else df.head(100).copy()

df_concat = pd.concat([df_a, df_b], axis=0, ignore_index=True)

print("\nConcat (dos subconjuntos por país):", df_concat.shape)
print(df_concat[["InvoiceNo", "Country", "Quantity", "UnitPrice", "TotalLine"]].head())

# 4.2 Merge: unir DataFrames por una clave común con distintos joins

# Merge LEFT: pegar el resumen por país a cada fila del dataset (enriquecimiento)
df_merge_left = df.merge(
    group_country_reset[["Country", "sum_total", "count_rows"]],
    on="Country",
    how="left"
)

print("\nMerge LEFT (dataset + métricas por Country) - columnas nuevas:")
print(df_merge_left[["Country", "TotalLine", "sum_total", "count_rows"]].head())

# Preparar un segundo resumen, por ejemplo: total por CustomerID (solo clientes con ID)
cust_summary = df.dropna(subset=["CustomerID"]).groupby("CustomerID").agg(
    cust_total=("TotalLine", "sum"),
    cust_invoices=("InvoiceNo", "nunique")
).reset_index()

# Merge INNER: quedarnos solo con filas que tienen CustomerID y resumen
df_with_customer = df.dropna(subset=["CustomerID"]).copy()

df_merge_inner = df_with_customer.merge(
    cust_summary,
    on="CustomerID",
    how="inner"
)

print("\nMerge INNER (solo filas con CustomerID + resumen por cliente) - muestra:")
print(df_merge_inner[["CustomerID", "TotalLine", "cust_total", "cust_invoices"]].head())

# Merge OUTER (demostración): unir dos resúmenes por Country, mostrando coincidencias y no coincidencias
# (normalmente coinciden todos, pero sirve para mostrar el how='outer')
df_merge_outer = group_country_reset.merge(
    group_country_custom_reset,
    on="Country",
    how="outer"
)

print("\nMerge OUTER (dos resúmenes por Country):")
print(df_merge_outer.head())


# -----------------------------
# 5) EXPORTACIÓN
# -----------------------------
# Exportar algunos resultados clave (puedes elegir lo que quieras entregar)
SALIDA_CSV = "ae6_resultados.csv"
SALIDA_XLSX = "ae6_resultados.xlsx"

# Guardamos un dataset transformado y algunos resúmenes
df_export = df_merge_left.copy()

df_export.to_csv(SALIDA_CSV, index=False)

with pd.ExcelWriter(SALIDA_XLSX, engine="openpyxl") as writer:
    df_export.to_excel(writer, sheet_name="dataset_enriquecido", index=False)
    group_country_reset.to_excel(writer, sheet_name="group_country", index=False)
    group_country_custom_reset.to_excel(writer, sheet_name="group_country_custom", index=False)
    pivot_sales_reset.to_excel(writer, sheet_name="pivot_country_month", index=False)
    melt_sales.to_excel(writer, sheet_name="melt_pivot", index=False)
    df_concat.to_excel(writer, sheet_name="concat_demo", index=False)
    df_merge_outer.to_excel(writer, sheet_name="merge_outer_demo", index=False)

print("\nArchivos exportados:")
print(SALIDA_CSV)
print(SALIDA_XLSX)
