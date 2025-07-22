# ETL de Avalúos - mi_tabla a vehicle_appraisal

Este proceso ETL extrae datos de `mi_tabla`, los transforma y limpia según las reglas de negocio, y los carga en `vehicle_appraisal`.

## Configuración

### 1. Variables de entorno
Crea un archivo `.env` en la raíz del proyecto con las siguientes variables:

```env
DB_USER=tu_usuario
DB_PASSWORD=tu_contraseña
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nombre_de_tu_base_de_datos
```

### 2. Instalación de dependencias
```bash
pip install -r requirements.txt
```

## Mapeo de campos

| Campo Origen (mi_tabla) | Campo Destino (vehicle_appraisal) | Tipo de transformación |
|------------------------|-----------------------------------|----------------------|
| CILINDRADA | engine_size | Numérico (decimal) |
| COMBUSTIBL | fuel_type | Texto limpio |
| NUMERO_CER | - | Referencia (no se mapea a ID auto-incremental) |
| SOLICITANT | applicant | Texto limpio |
| PROPIETARI | owner | Texto limpio |
| MARCA | brand | Texto limpio |
| MODELO | vehicle_description | Texto limpio |
| A_O | model_year | Entero |
| KMS | mileage | Entero |
| COLOR | color | Texto limpio |
| PLACAS | plate_number | Texto limpio |
| NOTA | notes | Texto limpio |
| ACCESORIOS | extras | Texto limpio |
| VIN_CHASIS | vin | Texto limpio |
| __VIN_DE_C | vin_card | Texto limpio |
| __VIN_DE_M | engine_number | Texto limpio |
| VIN_DE_MOT | engine_number_card | Texto limpio |
| AVALUO_BAN | apprasail_value_bank | Decimal |
| _FECHAS_1 | appraisal_date | Fecha |
| AVALUO_DIS | apprasail_value_lower_cost | Decimal |
| VALOR_GIBS | appraisal_value_trochez | Decimal |
| AV_DIST_NU | appraisal_value_usd | Decimal |
| - | validity_days | Valor fijo: 30 |
| - | validity_kms | Valor fijo: 1000 |
| - | apprasail_value_lower_bank | Calculado: apprasail_value_bank * 0.9 |

## Reglas de limpieza

### Texto
- Elimina espacios al inicio y final
- Normaliza espacios múltiples a uno solo
- Remueve caracteres especiales no válidos
- Convierte valores NULL, "NULL", "NONE", "N/A" a NULL
- Limita longitud a 100 caracteres para campos de texto

### Números
- Remueve caracteres no numéricos
- Convierte comas a puntos para decimales
- Maneja valores NULL/vacíos

### Fechas
- Soporta múltiples formatos: YYYY-MM-DD, DD/MM/YYYY, MM/DD/YYYY, YYYY/MM/DD
- Convierte a formato DATE estándar

## Uso

### Ejecución básica
```bash
python etl_avaluos.py
```

### Desde otro script
```python
from etl_avaluos import ETLAvaluos

etl = ETLAvaluos()
resultado = etl.ejecutar_etl()

if resultado:
    print("ETL exitoso")
else:
    print("ETL falló")
```

## Proceso ETL

1. **Extracción**: Lee todos los registros de `mi_tabla` donde `NUMERO_CER` no es NULL
2. **Transformación**: 
   - Aplica reglas de limpieza a cada campo
   - Convierte tipos de datos
   - Realiza cálculos (ej: descuento del 10%)
   - Filtra registros con datos mínimos válidos
3. **Carga**: Inserta registros uno por uno en `vehicle_appraisal`
4. **Verificación**: Cuenta total de registros insertados

## Logs

El proceso genera logs detallados:
- ✅ Operaciones exitosas
- ⚠️ Advertencias (registros problemáticos)
- ❌ Errores críticos
- 📊 Estadísticas finales

## Manejo de errores

- Errores de conexión: El proceso se detiene
- Errores de registro individual: Se registra el error y continúa
- Transacciones: Se confirman solo si toda la carga es exitosa

## Consideraciones importantes

1. **NUMERO_CER no se mapea al ID**: El campo `vehicle_appraisal_id` es auto-incremental
2. **Campos no mapeados**: Algunos campos de origen no tienen equivalente en destino
3. **Duplicados**: El proceso no verifica duplicados - ejecutar solo con datos limpios
4. **Rendimiento**: Procesa registro por registro para mejor control de errores

## Estructura de archivos

```
ETL/
├── database_connection.py    # Clase de conexión a BD
├── etl_avaluos.py           # Proceso ETL principal
├── requirements.txt         # Dependencias
├── .env                     # Credenciales (crear manualmente)
└── README_ETL.md           # Esta documentación
``` 