import pandas as pd
from dbfread import DBF
from database_connection import DatabaseConnection

# Crear instancia de conexión a la base de datos
db = DatabaseConnection()

# Leer el archivo DBF
tabla = DBF('BaseDatos 15-07-2025/avaluos2.dbf', encoding='latin-1')
df = pd.DataFrame(iter(tabla))

# Convertir tipos de datos si es necesario
# Por ejemplo, convertir fechas:
# df['fecha'] = pd.to_datetime(df['fecha'])

# Guardar en PostgreSQL usando la conexión de la clase
df.to_sql('mi_tabla', db.get_engine(), if_exists='replace', index=False)

print("Conversión completada con éxito")

# Cerrar la conexión
db.close_connection()