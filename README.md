# Mi Primer ETL con Python

## Descripción
Pipeline ETL que procesa datos de e-commerce para generar métricas de ventas.

## Cómo correr
```bash
pip install pandas pyarrow
python etl.py
```

## Decisiones de limpieza
- **Nulos**: Eliminé filas sin customer_id, product_id o total (campos críticos)
- **Duplicados**: Eliminé duplicados por order_id, quedándome con el más reciente
- **Tipos**: Convertí order_date a datetime, total y quantity a numérico

## Output
- `ventas_por_cliente.csv`: Total gastado y cantidad de órdenes por cliente
- `ventas_por_mes.csv`: Ventas totales por mes
- `orders_clean.parquet`: Dataset limpio en formato optimizado

## Autor
Reinaldo Franco - 4/2/2026
