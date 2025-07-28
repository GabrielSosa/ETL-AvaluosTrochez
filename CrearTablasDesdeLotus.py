import pandas as pd
from dbfread import DBF
from database_connection import DatabaseConnection

# Crear instancia de conexión a la base de datos
db = DatabaseConnection()

# Leer el archivo DBF
tabla = DBF('BaseDatosDBF/avaluos2.dbf', encoding='latin-1')
df = pd.DataFrame(iter(tabla))

# Agregar campo id_unico autoincremental y único
# Usar range para generar IDs únicos desde 1 hasta el número de filas
df['id_unico'] = range(1, len(df) + 1)

print(f"✅ Agregado campo id_unico con {len(df)} registros únicos")
print(f"📊 Rango de IDs: {df['id_unico'].min()} - {df['id_unico'].max()}")

# Verificar que no hay duplicados
if df['id_unico'].duplicated().any():
    print("⚠️ ADVERTENCIA: Se encontraron IDs duplicados")
else:
    print("✅ Verificación: No hay IDs duplicados")

# Convertir tipos de datos si es necesario
# Por ejemplo, convertir fechas:
# df['fecha'] = pd.to_datetime(df['fecha'])

# Guardar en PostgreSQL usando la conexión de la clase
df.to_sql('mi_tabla', db.get_engine(), if_exists='replace', index=False)

print("✅ Conversión completada con éxito")
print(f"📊 Total de registros insertados: {len(df)}")

# Cerrar la conexión
db.close_connection()