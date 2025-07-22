# ETL de Avalúos - mi_tabla a vehicle_appraisal

Este proceso ETL extrae datos de `mi_tabla`, los transforma y limpia según las reglas de negocio, y los carga en `vehicle_appraisal` en PostgreSQL. Incluye manejo de deducciones y validaciones adicionales.

## Configuración

### 1. Variables de entorno
Crea un archivo `.env` en la raíz del proyecto con las siguientes variables (no subas este archivo a git):

```env
DB_USER=tu_usuario
DB_PASSWORD=tu_contraseña
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nombre_de_tu_base_de_datos
```

> **Nota:** El archivo `.env` está incluido en `.gitignore` para evitar exponer credenciales.

### 2. Instalación de dependencias
```bash
pip install -r requirements.txt
```

## Estructura de archivos

```
ETL-AvaluosTrochez/
├── database_connection.py      # Clase de conexión a BD (usa variables de entorno)
├── etl_avaluos.py             # Proceso ETL principal
├── CrearTablasDesdeLotus.py   # Conversión de DBF a tabla temporal
├── requirements.txt           # Dependencias
├── .env                       # Credenciales (NO subir a git)
├── tests/                     # Pruebas y utilidades
├── BaseDatos 15-07-2025/      # Archivos fuente DBF, ADX, APR (NO subir a git)
└── README.md                  # Esta documentación
```

## Seguridad y buenas prácticas
- **Nunca subas archivos `.env`, `.dbf`, `.adx`, `.apr` ni datos sensibles al repositorio.**
- El archivo `.gitignore` ya está configurado para ignorar estos archivos y carpetas.
- Ningún script ni test contiene credenciales hardcodeadas; todo se maneja por variables de entorno.

## Mapeo de campos

| Campo Origen (mi_tabla) | Campo Destino (vehicle_appraisal) | Tipo de transformación |
|------------------------|-----------------------------------|----------------------|
| CILINDRADA | engine_size | Numérico (decimal, conversión cc→L si aplica) |
| COMBUSTIBL | fuel_type | Texto limpio |
| NUMERO_CER | cert | Numérico |
| SOLICITANT | applicant | Texto limpio |
| PROPIETARI | owner | Texto limpio |
| MARCA | brand | Texto limpio |
| MODELO | vehicle_description | Texto limpio |
| A_O | model_year | Entero (validación de rango) |
| KMS | mileage | Entero (limpieza y validación) |
| COLOR | color | Texto limpio |
| PLACAS | plate_number | Texto limpio |
| NOTA | notes | Texto limpio |
| ACCESORIOS | extras | Texto limpio |
| VIN_CHASIS | vin | Texto limpio |
| __VIN_DE_C | vin_card | Texto limpio |
| __VIN_DE_M | engine_number | Texto limpio |
| VIN_DE_MOT | engine_number_card | Texto limpio |
| TOTAL_DE_R | total_deductions | Decimal |
| MODIF_KM | modified_km | Entero |
| VALOR_EXTR | extra_value | Decimal |
| DESCUENTOS | discounts | Decimal |
| AV_BANC_NU | bank_value_in_dollars | Decimal |
| AVALUO_BAN | apprasail_value_bank | Decimal |
| _FECHAS_1 | appraisal_date | Fecha |
| AVALUO_DIS | apprasail_value_lower_cost | Decimal |
| VALOR_GIBS | appraisal_value_trochez | Decimal |
| AV_DIST_NU | appraisal_value_usd | Decimal |
| - | validity_days | Valor fijo: 30 |
| - | validity_kms | Valor fijo: 1000 |
| - | apprasail_value_lower_bank | Calculado: apprasail_value_bank * 0.9 |
| id_unico | referencia_original | Entero (clave de mapeo) |

## Reglas de limpieza

### Texto
- Elimina espacios al inicio y final
- Normaliza espacios múltiples a uno solo
- Remueve caracteres especiales no válidos
- Convierte valores NULL, "NULL", "NONE", "N/A" a vacío
- Limita longitud a 100 caracteres para campos de texto

### Números
- Remueve caracteres no numéricos
- Convierte comas a puntos para decimales
- Maneja valores NULL/vacíos
- Convierte a 0 si el valor es inválido

### Fechas
- Soporta múltiples formatos: YYYY-MM-DD, DD/MM/YYYY, MM/DD/YYYY, YYYY/MM/DD
- Convierte a formato DATE estándar

## Proceso ETL

1. **Extracción**: Lee todos los registros de `mi_tabla` donde `id_unico` no es NULL
2. **Transformación**: 
   - Aplica reglas de limpieza a cada campo
   - Convierte tipos de datos y valida rangos
   - Realiza cálculos (ej: descuento del 10%)
   - Filtra registros con datos mínimos válidos
3. **Carga**: Inserta registros en bloque en `vehicle_appraisal` (carga masiva)
4. **Deducciones**: Procesa y carga deducciones en `appraisal_deductions` usando los IDs generados
5. **Verificación**: Cuenta total de registros insertados

## Ejecución

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

## Logs y manejo de errores

- El proceso genera logs detallados:
  - ✅ Operaciones exitosas
  - ⚠️ Advertencias (registros problemáticos)
  - ❌ Errores críticos
  - 📊 Estadísticas finales
- Errores de conexión: El proceso se detiene
- Errores de registro individual: Se registra el error y continúa
- Transacciones: Se confirman solo si toda la carga es exitosa

## Consideraciones importantes

1. **No se suben datos sensibles**: `.env`, archivos de base de datos y temporales están en `.gitignore`.
2. **Ningún script contiene credenciales hardcodeadas**: Todo se maneja por variables de entorno.
3. **Duplicados**: El proceso no verifica duplicados - ejecutar solo con datos limpios.
4. **Rendimiento**: Procesa en bloques para mejor rendimiento y control de errores.
5. **Pruebas**: Los archivos en `tests/` no contienen datos sensibles ni credenciales.

---

¿Dudas o sugerencias? ¡Abre un issue o contacta al responsable del ETL! 