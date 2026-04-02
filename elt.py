import pandas as pd
import os
from datetime import datetime
import pyarrow as pa

pd.set_option('display.max_columns', None)

# Path a la carpeta con la data
path = "YOURPATH/mi-primer-elt/data/"


"""FUNCIONES PARA AUTOMATIZAR EL PROCESO"""

def check_nulls(df, file):
    if df.isnull().values.any():
        for column in df.columns:
            if df[column].isna().sum() > 0:
                print(f'File: {file}, Columna: {column}, cantidad de nulos: {df[column].isna().sum()}')
                # Cambiar los nulos por 0
                df[column] = df[column].fillna(0)
            # Al tener nulls, el type de la columna puede cambiar
            # Por prueba y error se conocen las columnas a cambiar y se aplica el cambio de type
            if column == 'promotion_id' or column == 'parent_category_id':
                df[column] = df[column].astype('int64')
            elif df[column].dtype == 'object':
                df[column] = df[column].astype(str)
    return df


def is_date(value: str) -> bool:
    # Intenta procesar un dato como date type
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except (ValueError, TypeError):
        return False


def column_looks_like_dates(series: pd.Series, sample_size: int = 10) -> bool:
    # Dropea nulos y toma un sample
    sample = series.dropna().head(sample_size)

    # Edge case: si la columna esta vacia, la salta
    if sample.empty:
        return False
    # Devuelve si y solo si todo los valores de la muestra son True
    return all(is_date(value) for value in sample)


def check_datetypes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()  # Para no mutar el original

    # Chequea columna por columna si parece un date type
    for col in df.columns:
        # Pasa primero a tomar una muestra
        if column_looks_like_dates(df[col]):
            # Si la muestra pasa la prueba, se convierte a datetime type
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d")
            print(f"✅ Converted '{col}' to datetime")

    return df


os.makedirs('output', exist_ok=True)

# Chequeo si existe el path
if os.path.exists(path):
    # Loop para revisar archivo por archivo
    for file in os.listdir(path):
        # Solo archivos que terminan en .csv
        if file.endswith('.csv'):
            # Crea un dataframe (df) con la informacion del archivo
            df = pd.read_csv(path + file)

            print(f"/n📈 Resumen:")
            print(f"{file}: {len(df)} filas, {len(df.columns)} columnas")
            print("/n🔍 Primeras filas de la tabla:")
            print(df.head())
            print("/n📋 Info de la tabla:")
            print(df.info())

            # Verifica si hay nulls y realiza los cambios necesarios de datatypes
            df_processed = check_nulls(df, file)

            # Verifico si tiene duplicados, si si, me indica la cantidad y luego los dropea
            if df_processed.duplicated().any():
                print(f'Duplicados: {df_processed.duplicated().sum()}')
                df_processed = df_processed.drop_duplicates()

            # Verifica si la columna es una fecha y le asigna datetype
            df_processed = check_datetypes(df_processed)

            # Verifica si es el archivo con la data necesaria para procesar las preguntas #1 y #3
            if file == 'ecommerce_orders.csv':
                # Agrupa por customer_id, suma el monto total gastado y cuenta la cantidad de ordenes
                top_clientes = df_processed.groupby('customer_id').agg({
                    'total_amount': 'sum',
                    'order_id': 'count'
                }).rename(columns={'total_amount': 'total_gastado', 'order_id': 'cantidad_ordenes'})
                top_clientes = top_clientes.sort_values(by=['total_gastado'], ascending=False)
                print(f"🏆 Top 5 clientes: {top_clientes.head()}")

                # Crea nueva columna que asigna el mes
                df_processed['mes'] = df_processed['order_date'].dt.to_period('M')
                # Agrupa por mes, y suma total_amount. Resetea el index.
                ventas_mes = df_processed.groupby('mes')['total_amount'].sum().reset_index()
                ventas_mes.columns = ['mes', 'total_ventas']
                print(f"📈 Ventas por mes: {ventas_mes}")
                
                top_clientes.to_csv('output/ventas_por_cliente.csv', index=False)
                ventas_mes.to_csv('output/ventas_por_mes.csv', index=False)

            # Verifica si es el archivo con la data necesaria para procesar la pregunta #2
            if file == 'ecommerce_order_items.csv':
                # Agrupa por product_id, suma valores de la columna quantity y ordena de mayor a menor
                mas_vendido = df_processed.groupby('product_id')['quantity'].sum().sort_values(ascending=False)
                print(f"\n📦 Producto más vendido: ID {mas_vendido.idxmax()} ({mas_vendido.max()} unidades)")
                mas_vendido.to_csv('output/productos_mas_vendidos.csv', index=False)

            # Guardar df procesados en /output basado en su nombre real
            df_processed.to_csv(f'output/processed_{file}', index=False)
            df_processed.to_parquet(f'output/processed_{file.split('.')[0]}.parquet', index=False)

# En caso de no existir el path
else:
    print('Path no encontrado')


# Comparar tamaños
csv_size = os.path.getsize('output/processed_ecommerce_orders.csv') / 1024
parquet_size = os.path.getsize('output/processed_ecommerce_orders.parquet') / 1024

print(f"Tamaño CSV: {csv_size:.1f} KB")
print(f"Tamaño Parquet: {parquet_size:.1f} KB")
print(f"Parquet es {csv_size/parquet_size:.1f}x más chico")
