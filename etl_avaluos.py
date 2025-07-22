import pandas as pd
import numpy as np
from datetime import datetime
import re
from database_connection import DatabaseConnection
from sqlalchemy import text
import logging
import psycopg2
from psycopg2.errors import InFailedSqlTransaction, DataError, IntegrityError, OperationalError
import time

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ETLAvaluos:
    """
    Clase para realizar ETL desde mi_tabla hacia vehicle_appraisal
    """
    
    def __init__(self):
        self.db_connection = None
        
    def conectar_base_datos(self):
        """Establecer conexión con la base de datos"""
        try:
            self.db_connection = DatabaseConnection()
            logger.info("✅ Conexión establecida correctamente")
            return True
        except Exception as e:
            logger.error(f"❌ Error al conectar: {e}")
            return False
    
    def extraer_datos(self):
        """Extraer datos de mi_tabla"""
        try:
            query = """
            SELECT 
                "id_unico",
                "CILINDRADA",
                "COMBUSTIBL", 
                "NUMERO_CER",
                "SOLICITANT",
                "PROPIETARI",
                "MARCA",
                "MODELO", 
                "A_O",
                "KMS",
                "ORIGEN",
                "COLOR",
                "PLACAS",
                "NOTA",
                "ACCESORIOS",
                "VIN_CHASIS",
                "__VIN_DE_C",
                "__VIN_DE_M", 
                "VIN_DE_MOT",
                "TOTAL_DE_R",
                "MODIF_KM",
                "VALOR_EXTR",
                "DESCUENTOS",
                "AV_BANC_NU",
                "AVALUO_BAN",
                "_FECHAS_1",
                "AVALUO_DIS",
                "VALOR_GIBS",
                "AV_DIST_NU",
                "MOTOR1",
                "MOTOR2",
                "TRANSMISIO",
                "TRANSMICIO",
                "SUSPENSION",
                "SUSPENSIO2",
                "DIRECCION",
                "DIRECCION2",
                "FRENOS",
                "FRENOS2",
                "LLANTAS",
                "RUEDAS",
                "SIST_ELECT",
                "SISTELEC2",
                "INTYACC2"
            FROM public.mi_tabla
            WHERE "id_unico" IS NOT NULL
            """
            
            df = pd.read_sql_query(query, self.db_connection.get_engine())
            logger.info(f"✅ Extraídos {len(df)} registros de mi_tabla")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error al extraer datos: {e}")
            return None
    
    def limpiar_texto(self, texto):
        """Limpiar y normalizar texto"""
        if pd.isna(texto) or texto is None:
            return ''
        
        texto = str(texto).strip()
        if texto == '' or texto.upper() in ['NULL', 'NONE', 'N/A']:
            return ''
            
        # Remover caracteres especiales extraños y normalizar espacios
        texto = re.sub(r'\s+', ' ', texto)
        texto = re.sub(r'[^\w\s\-\.\/]', '', texto, flags=re.UNICODE)
        
        return texto[:100] if len(texto) > 100 else texto
    
    def limpiar_numero(self, numero, tipo='float'):
        """Limpiar y convertir números"""
        if pd.isna(numero) or numero is None:
            return None
            
        try:
            # Convertir a string y limpiar
            numero_str = str(numero).strip()
            if numero_str == '' or numero_str.upper() in ['NULL', 'NONE', 'N/A']:
                return None
                
            # Remover caracteres no numéricos excepto punto y coma
            numero_str = re.sub(r'[^\d\.\,\-]', '', numero_str)
            
            # Reemplazar coma por punto para decimales
            numero_str = numero_str.replace(',', '.')
            
            valor = float(numero_str)
            
            if tipo == 'int':
                return int(valor)
            else:
                return valor
                
        except (ValueError, TypeError):
            return None
    
    def limpiar_fecha(self, fecha):
        """Limpiar y convertir fechas"""
        if pd.isna(fecha) or fecha is None:
            return None
            
        try:
            if isinstance(fecha, str):
                # Intentar diferentes formatos de fecha
                formatos = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d']
                for formato in formatos:
                    try:
                        return datetime.strptime(fecha, formato).date()
                    except ValueError:
                        continue
                return None
            else:
                return fecha
        except:
            return None
    
    def validar_datos_insercion(self, datos):
        """Validar datos antes de la inserción"""
        try:
            # Validar tipos de datos
            if datos['model_year'] is not None:
                if not isinstance(datos['model_year'], int) or datos['model_year'] < 1900 or datos['model_year'] > 2030:
                    datos['model_year'] = None
            
            if datos['mileage'] is not None:
                if not isinstance(datos['mileage'], int) or datos['mileage'] < 0:
                    datos['mileage'] = None
            
            if datos['modified_km'] is not None:
                if not isinstance(datos['modified_km'], int) or datos['modified_km'] < 0:
                    datos['modified_km'] = None
            
            # Validar engine_size según restricción numeric(3,1) - máximo 99.9
            if datos['engine_size'] is not None:
                if not isinstance(datos['engine_size'], (int, float)) or datos['engine_size'] <= 0:
                    datos['engine_size'] = None
                elif datos['engine_size'] > 99.9:
                    # Si excede el límite, convertir a litros (dividir por 1000 si está en cc)
                    if datos['engine_size'] > 999:
                        datos['engine_size'] = round(datos['engine_size'] / 1000, 1)
                    else:
                        datos['engine_size'] = None
            
            # Validar longitudes de texto
            campos_texto = ['vehicle_description', 'brand', 'color', 'plate_number', 'applicant', 'owner', 'fuel_type', 'vin', 'engine_number', 'vin_card', 'engine_number_card']
            for campo in campos_texto:
                if datos[campo] is not None and datos[campo] != '' and len(str(datos[campo])) > 100:
                    datos[campo] = str(datos[campo])[:100]
            
            # Validar valores monetarios
            campos_monetarios = ['appraisal_value_usd', 'appraisal_value_trochez', 'apprasail_value_lower_cost', 'apprasail_value_bank', 'apprasail_value_lower_bank', 'total_deductions', 'extra_value', 'discounts', 'bank_value_in_dollars']
            for campo in campos_monetarios:
                if datos[campo] is not None:
                    if not isinstance(datos[campo], (int, float)) or datos[campo] < 0:
                        datos[campo] = None
            
            return True
            
        except Exception as e:
            logger.warning(f"⚠️ Error en validación de datos: {e}")
            return False
    
    def procesar_cilindrada(self, cilindrada):
        """Procesar cilindrada y convertir a formato compatible con numeric(3,1)"""
        try:
            # Limpiar el valor
            valor_limpio = self.limpiar_numero(cilindrada, 'float')
            if valor_limpio is None:
                return None
            
            # Si el valor es mayor a 99.9, probablemente está en cc, convertir a litros
            if valor_limpio > 99.9:
                # Convertir cc a litros (dividir por 1000)
                valor_litros = round(valor_limpio / 1000, 1)
                logger.info(f"🔄 Convertido cilindrada de {valor_limpio}cc a {valor_litros}L")
                return valor_litros
            
            # Si está en el rango válido (0-99.9), usar directamente
            return round(valor_limpio, 1)
            
        except Exception as e:
            logger.warning(f"⚠️ Error procesando cilindrada {cilindrada}: {e}")
            return None
    
    def procesar_deducciones(self, df_origen, vehicle_appraisal_ids):
        """Procesar deducciones y crear filas para appraisal_deductions"""
        try:
            deducciones = []
            
            logger.info(f"🔍 Procesando deducciones para {len(df_origen)} registros")
            logger.info(f"🔍 Vehicle appraisal IDs disponibles: {len(vehicle_appraisal_ids)}")
            
            # Log para ver qué datos están llegando de los campos de deducciones
            logger.info(f"📊 Muestra de MOTOR1: {df_origen['MOTOR1'].head(3).tolist()}")
            logger.info(f"📊 Muestra de MOTOR2: {df_origen['MOTOR2'].head(3).tolist()}")
            logger.info(f"📊 Muestra de TRANSMISIO: {df_origen['TRANSMISIO'].head(3).tolist()}")
            logger.info(f"📊 Muestra de TRANSMICIO: {df_origen['TRANSMICIO'].head(3).tolist()}")
            logger.info(f"📊 Campos no nulos MOTOR1: {df_origen['MOTOR1'].notna().sum()}")
            logger.info(f"📊 Campos no nulos MOTOR2: {df_origen['MOTOR2'].notna().sum()}")
            
            # Buscar registros con deducciones válidas
            registros_con_deducciones = df_origen[
                (df_origen['MOTOR1'].notna() & (df_origen['MOTOR1'] != 0)) |
                (df_origen['TRANSMISIO'].notna() & (df_origen['TRANSMISIO'] != 0))
            ]
            logger.info(f"🔍 Registros con deducciones válidas: {len(registros_con_deducciones)}")
            
            if len(registros_con_deducciones) > 0:
                logger.info(f"📋 Ejemplos de deducciones:")
                for idx, row in registros_con_deducciones.head(3).iterrows():
                    if pd.notna(row['MOTOR1']) and row['MOTOR1'] != 0:
                        logger.info(f"  - MOTOR1: {row['MOTOR1']} -> {row['MOTOR2']}")
                    if pd.notna(row['TRANSMISIO']) and row['TRANSMISIO'] != 0:
                        logger.info(f"  - TRANSMISIO: {row['TRANSMISIO']} -> {row['TRANSMICIO']}")
            
            # Mapeo de campos de deducciones
            mapeo_deducciones = [
                ('MOTOR1', 'MOTOR2', 'Motor'),
                ('TRANSMISIO', 'TRANSMICIO', 'Transmisión'),
                ('SUSPENSION', 'SUSPENSIO2', 'Suspensión'),
                ('DIRECCION', 'DIRECCION2', 'Dirección'),
                ('FRENOS', 'FRENOS2', 'Frenos'),
                ('LLANTAS', 'RUEDAS', 'Llantas'),
                ('SIST_ELECT', 'SISTELEC2', 'Sistema Eléctrico'),
                (None, 'INTYACC2', 'Interior y Accesorios')  # Solo descripción
            ]
            
            for index, row in df_origen.iterrows():
                id_unico = row['id_unico']
                vehicle_appraisal_id = vehicle_appraisal_ids.get(id_unico)
                if vehicle_appraisal_id is None:
                    logger.debug(f"⚠️ No se encontró vehicle_appraisal_id para id_unico: {id_unico}")
                    continue
                
                for campo_amount, campo_desc, descripcion_base in mapeo_deducciones:
                    amount = None
                    description = None
                    
                    # Procesar campo de monto (float)
                    if campo_amount:
                        amount = self.limpiar_numero(row[campo_amount], 'float')
                    
                    # Procesar campo de descripción (text)
                    if campo_desc:
                        description = self.limpiar_texto(row[campo_desc])
                    
                    # Solo crear fila si hay al menos un valor no vacío
                    if amount is not None or (description and description != ''):
                        # Si amount es None o NaN, poner 0
                        if amount is None or (isinstance(amount, float) and pd.isna(amount)):
                            amount = 0
                        deduccion = {
                            'vehicle_appraisal_id': vehicle_appraisal_id,
                            'amount': amount,
                            'description': description if description and description != '' else descripcion_base
                        }
                        deducciones.append(deduccion)
                        logger.debug(f"➕ Deducción agregada: {descripcion_base} - Amount: {amount}, Description: {description}")
            
            logger.info(f"✅ Procesadas {len(deducciones)} deducciones válidas")
            logger.info(f"📊 Total de registros procesados: {len(df_origen)}")
            logger.info(f"📊 Registros con vehicle_appraisal_id encontrado: {len([r for r in df_origen.iterrows() if vehicle_appraisal_ids.get(r[1]['id_unico']) is not None])}")
            return deducciones
            
        except Exception as e:
            logger.error(f"❌ Error procesando deducciones: {e}")
            return []
    
    def transformar_datos(self, df_origen):
        """Transformar datos según el mapeo especificado"""
        try:
            df_transformado = pd.DataFrame()
            
            # Mapeo completo de campos según la especificación
            df_transformado['engine_size'] = df_origen['CILINDRADA'].apply(
                lambda x: self.procesar_cilindrada(x)
            )
            
            df_transformado['fuel_type'] = df_origen['COMBUSTIBL'].apply(self.limpiar_texto)
            
            # id_unico se usa como la nueva llave única para el mapeo con deducciones
            df_transformado['referencia_original'] = df_origen['id_unico'].apply(
                lambda x: self.limpiar_numero(x, 'int')
            )
            
            # NUMERO_CER se mapea al campo cert
            df_transformado['cert'] = df_origen['NUMERO_CER'].apply(
                lambda x: self.limpiar_numero(x, 'float')
            )
            
            df_transformado['applicant'] = df_origen['SOLICITANT'].apply(self.limpiar_texto)
            df_transformado['owner'] = df_origen['PROPIETARI'].apply(self.limpiar_texto)
            df_transformado['brand'] = df_origen['MARCA'].apply(self.limpiar_texto)
            
            # Log para diagnosticar campos problemáticos
            logger.info(f"📊 Muestra de datos SOLICITANT: {df_origen['SOLICITANT'].head().tolist()}")
            logger.info(f"📊 Muestra de datos PROPIETARI: {df_origen['PROPIETARI'].head().tolist()}")
            logger.info(f"📊 Muestra de datos MARCA: {df_origen['MARCA'].head().tolist()}")
            logger.info(f"📊 Campos no nulos SOLICITANT: {df_origen['SOLICITANT'].notna().sum()}")
            logger.info(f"📊 Campos no nulos PROPIETARI: {df_origen['PROPIETARI'].notna().sum()}")
            logger.info(f"📊 Campos no nulos MARCA: {df_origen['MARCA'].notna().sum()}")
            
            # Log de ejemplos después de la limpieza
            ejemplos_applicant = df_origen['SOLICITANT'].head(3).apply(self.limpiar_texto).tolist()
            ejemplos_owner = df_origen['PROPIETARI'].head(3).apply(self.limpiar_texto).tolist()
            ejemplos_brand = df_origen['MARCA'].head(3).apply(self.limpiar_texto).tolist()
            logger.info(f"📊 Ejemplos applicant después de limpieza: {ejemplos_applicant}")
            logger.info(f"📊 Ejemplos owner después de limpieza: {ejemplos_owner}")
            logger.info(f"📊 Ejemplos brand después de limpieza: {ejemplos_brand}")
            
            df_transformado['vehicle_description'] = df_origen['MODELO'].apply(self.limpiar_texto)
            
            # Log temporal para ver cómo se mapea A_O a model_year
            logger.info(f"Ejemplo A_O original: {df_origen['A_O'].head(10).tolist()}")
            def limpiar_model_year(x):
                if isinstance(x, int):
                    val = x
                elif isinstance(x, str) and x.isdigit():
                    val = int(x)
                else:
                    return None
                if val < 1900 or val > 2030:
                    return None
                return val
            df_transformado['model_year'] = df_origen['A_O'].apply(limpiar_model_year)
            logger.info(f"Ejemplo model_year transformado: {df_transformado['model_year'].head(10).tolist()}")
            
            # Log temporal para ver cómo se mapea KMS a mileage
            logger.info(f"Ejemplo KMS original: {df_origen['KMS'].head(10).tolist()}")
            def limpiar_mileage(x):
                original = x
                # Limpiar espacios y caracteres comunes
                if isinstance(x, str):
                    x = x.strip().replace(',', '').replace('.', '')
                # Permitir enteros puros
                if isinstance(x, int) and x >= 0:
                    logger.info(f"[mileage] Entrada: {original} -> Salida: {x}")
                    return x
                elif isinstance(x, float) and x.is_integer() and x >= 0:
                    logger.info(f"[mileage] Entrada: {original} -> Salida: {int(x)}")
                    return int(x)
                elif isinstance(x, str) and x.isdigit():
                    val = int(x)
                    logger.info(f"[mileage] Entrada: {original} -> Salida: {val}")
                    return val if val >= 0 else None
                else:
                    logger.info(f"[mileage] Entrada: {original} -> Salida: None")
                    return None
            df_transformado['mileage'] = df_origen['KMS'].apply(limpiar_mileage)
            logger.info(f"Ejemplo mileage transformado: {df_transformado['mileage'].head(10).tolist()}")
            
            # ORIGEN no se mapea según la especificación
            
            df_transformado['color'] = df_origen['COLOR'].apply(self.limpiar_texto)
            df_transformado['plate_number'] = df_origen['PLACAS'].apply(self.limpiar_texto)
            df_transformado['notes'] = df_origen['NOTA'].apply(self.limpiar_texto)
            df_transformado['extras'] = df_origen['ACCESORIOS'].apply(self.limpiar_texto)
            df_transformado['vin'] = df_origen['VIN_CHASIS'].apply(self.limpiar_texto)
            df_transformado['vin_card'] = df_origen['__VIN_DE_C'].apply(self.limpiar_texto)
            df_transformado['engine_number'] = df_origen['__VIN_DE_M'].apply(self.limpiar_texto)
            df_transformado['engine_number_card'] = df_origen['VIN_DE_MOT'].apply(self.limpiar_texto)
            
            # Nuevos campos del mapeo
            df_transformado['total_deductions'] = df_origen['TOTAL_DE_R'].apply(
                lambda x: self.limpiar_numero(x, 'float')
            )
            
            df_transformado['modified_km'] = df_origen['MODIF_KM'].apply(
                lambda x: self.limpiar_numero(x, 'int')
            )
            
            df_transformado['extra_value'] = df_origen['VALOR_EXTR'].apply(
                lambda x: self.limpiar_numero(x, 'float')
            )
            
            df_transformado['discounts'] = df_origen['DESCUENTOS'].apply(
                lambda x: self.limpiar_numero(x, 'float')
            )
            
            df_transformado['bank_value_in_dollars'] = df_origen['AV_BANC_NU'].apply(
                lambda x: self.limpiar_numero(x, 'float')
            )
            
            df_transformado['apprasail_value_bank'] = df_origen['AVALUO_BAN'].apply(
                lambda x: self.limpiar_numero(x, 'float')
            )
            
            df_transformado['appraisal_date'] = df_origen['_FECHAS_1'].apply(self.limpiar_fecha)
            
            df_transformado['apprasail_value_lower_cost'] = df_origen['AVALUO_DIS'].apply(
                lambda x: self.limpiar_numero(x, 'float')
            )
            
            df_transformado['appraisal_value_trochez'] = df_origen['VALOR_GIBS'].apply(
                lambda x: self.limpiar_numero(x, 'float')
            )
            
            df_transformado['appraisal_value_usd'] = df_origen['AV_DIST_NU'].apply(
                lambda x: self.limpiar_numero(x, 'float')
            )
            
            # Valores fijos
            df_transformado['validity_days'] = 30
            df_transformado['validity_kms'] = 1000
            
            # Cálculo: apprasail_value_lower_bank = apprasail_value_bank - 10%
            df_transformado['apprasail_value_lower_bank'] = df_transformado['apprasail_value_bank'].apply(
                lambda x: x * 0.9 if x is not None and not pd.isna(x) else None
            )
            
            # Filtrar registros con datos mínimos requeridos
            df_limpio = df_transformado.dropna(subset=['referencia_original'])
            
            # Log para ver cuántos registros tienen los campos después de la limpieza
            logger.info(f"📊 Registros con applicant no vacío: {(df_limpio['applicant'] != '').sum()}")
            logger.info(f"📊 Registros con owner no vacío: {(df_limpio['owner'] != '').sum()}")
            logger.info(f"📊 Registros con brand no vacío: {(df_limpio['brand'] != '').sum()}")
            
            logger.info(f"✅ Transformados {len(df_limpio)} registros válidos")

            # Limpiar todos los campos numéricos: None, NaN, 'NULL', 'NONE', 'N/A' o vacíos -> 0
            campos_numericos = [
                'engine_size', 'model_year', 'mileage', 'total_deductions', 'modified_km',
                'extra_value', 'discounts', 'bank_value_in_dollars', 'apprasail_value_bank',
                'apprasail_value_lower_cost', 'appraisal_value_trochez', 'appraisal_value_usd',
                'apprasail_value_lower_bank', 'cert', 'referencia_original'
            ]
            for campo in campos_numericos:
                if campo in df_limpio.columns:
                    df_limpio[campo] = df_limpio[campo].apply(lambda x: 0 if pd.isna(x) or x is None else x)

            # Asegurar que los campos de valores de avalúo no sean negativos
            for campo in ['apprasail_value_lower_cost', 'apprasail_value_bank', 'apprasail_value_lower_bank']:
                if campo in df_limpio.columns:
                    df_limpio[campo] = df_limpio[campo].apply(lambda x: 0 if x < 0 else x)

            # Asegurar que modified_km y extra_value no sean negativos
            for campo in ['modified_km', 'extra_value']:
                if campo in df_limpio.columns:
                    df_limpio[campo] = df_limpio[campo].apply(lambda x: 0 if x < 0 else x)

            # Para model_year, si el valor es 0, dejarlo en 1900
            if 'model_year' in df_limpio.columns:
                df_limpio['model_year'] = df_limpio['model_year'].apply(lambda x: 1900 if x == 0 else x)

            return df_limpio
            
        except Exception as e:
            logger.error(f"❌ Error en transformación: {e}")
            return None
    
    def cargar_datos(self, df_transformado):
        """Cargar datos en vehicle_appraisal usando inserción masiva"""
        try:
            # Inserción masiva con pandas
            engine = self.db_connection.get_engine()
            # Selecciona solo las columnas que existen en la tabla destino
            columnas_destino = [
                'appraisal_date', 'vehicle_description', 'brand', 'model_year', 'color',
                'mileage', 'fuel_type', 'engine_size', 'plate_number', 'applicant',
                'owner', 'appraisal_value_usd', 'appraisal_value_trochez', 'vin',
                'engine_number', 'notes', 'validity_days', 'validity_kms',
                'apprasail_value_lower_cost', 'apprasail_value_bank',
                'apprasail_value_lower_bank', 'extras', 'vin_card', 'engine_number_card',
                'total_deductions', 'modified_km', 'extra_value', 'discounts', 'bank_value_in_dollars',
                'referencia_original', 'cert'
            ]
            df_insert = df_transformado[columnas_destino]
            df_insert.to_sql('vehicle_appraisal', engine, schema='public', if_exists='append', index=False, chunksize=2000, method='multi')
            logger.info(f"✅ Inserción masiva completada: {len(df_insert)} registros en vehicle_appraisal")
            return True
        except Exception as e:
            logger.error(f"❌ Error al cargar datos masivos: {e}")
            return False
    
    def obtener_vehicle_appraisal_ids(self, df_origen):
        """Obtener los IDs de vehicle_appraisal basados en id_unico"""
        try:
            vehicle_appraisal_ids = {}
            
            # Obtener los id_unico únicos que se procesaron
            ids_unicos = df_origen['id_unico'].dropna().unique()
            
            if len(ids_unicos) == 0:
                logger.warning("⚠️ No hay id_unico para mapear")
                return vehicle_appraisal_ids
            
            logger.info(f"🔍 Valores a buscar: {len(ids_unicos)} id_unico únicos")
            logger.info(f"🔍 Ejemplos: {ids_unicos[:5].tolist()}")
            
            # Buscar los vehicle_appraisal_id por id_unico (que se guarda como referencia_original)
            session = self.db_connection.get_engine().connect()
            try:
                # Crear consulta con placeholders limitados para evitar errores
                # Procesar en lotes de 100 para evitar demasiados parámetros
                batch_size = 100
                total_mapped = 0
                
                for i in range(0, len(ids_unicos), batch_size):
                    batch = ids_unicos[i:i+batch_size]
                    placeholders = ','.join([':val' + str(j) for j in range(len(batch))])
                    query = f"""
                        SELECT vehicle_appraisal_id, referencia_original 
                        FROM public.vehicle_appraisal 
                        WHERE referencia_original IN ({placeholders})
                    """
                    
                    # Crear diccionario de parámetros para este lote
                    params = {f'val{j}': valor for j, valor in enumerate(batch.tolist())}
                    
                    result = session.execute(text(query), params)
                    batch_found = 0
                    for row in result:
                        vehicle_appraisal_ids[row.referencia_original] = row.vehicle_appraisal_id
                        batch_found += 1
                        total_mapped += 1
                    
                    logger.info(f"🔍 Lote {i//batch_size + 1}: {batch_found} IDs encontrados")
                
                logger.info(f"✅ Total mapeados: {total_mapped} IDs de vehicle_appraisal")
                if len(vehicle_appraisal_ids) > 0:
                    logger.info(f"📊 Ejemplos de mapeo: {list(vehicle_appraisal_ids.items())[:3]}")
                return vehicle_appraisal_ids
                
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"❌ Error obteniendo vehicle_appraisal_ids: {e}")
            return {}
    
    def obtener_ultimos_ids_insertados(self, limite=50):
        """Obtener los últimos IDs de vehicle_appraisal insertados"""
        try:
            vehicle_appraisal_ids = {}
            
            session = self.db_connection.get_engine().connect()
            try:
                query = f"""
                    SELECT vehicle_appraisal_id, referencia_original 
                    FROM public.vehicle_appraisal 
                    WHERE referencia_original IS NOT NULL
                    ORDER BY vehicle_appraisal_id DESC
                    LIMIT {limite}
                """
                
                result = session.execute(text(query))
                for row in result:
                    vehicle_appraisal_ids[row.referencia_original] = row.vehicle_appraisal_id
                
                logger.info(f"✅ Obtenidos {len(vehicle_appraisal_ids)} IDs de los últimos registros insertados")
                if len(vehicle_appraisal_ids) > 0:
                    logger.info(f"📊 Ejemplos de mapeo: {list(vehicle_appraisal_ids.items())[:3]}")
                return vehicle_appraisal_ids
                
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"❌ Error obteniendo últimos IDs: {e}")
            return {}
    
    def cargar_deducciones(self, deducciones):
        """Cargar deducciones en appraisal_deductions usando inserción masiva"""
        try:
            if not deducciones:
                logger.info("📝 No hay deducciones para cargar")
                return True
            engine = self.db_connection.get_engine()
            df_deducciones = pd.DataFrame(deducciones)
            columnas_destino = ['vehicle_appraisal_id', 'amount', 'description']
            df_insert = df_deducciones[columnas_destino]
            df_insert.to_sql('appraisal_deductions', engine, schema='public', if_exists='append', index=False, chunksize=2000, method='multi')
            logger.info(f"✅ Inserción masiva completada: {len(df_insert)} registros en appraisal_deductions")
            return True
        except Exception as e:
            logger.error(f"❌ Error al cargar deducciones masivas: {e}")
            return False
    
    def verificar_carga(self):
        """Verificar que los datos se cargaron correctamente"""
        try:
            query = "SELECT COUNT(*) as total FROM public.vehicle_appraisal"
            resultado = pd.read_sql_query(query, self.db_connection.get_engine())
            total = resultado['total'].iloc[0]
            logger.info(f"📊 Total de registros en vehicle_appraisal: {total}")
            return total
        except Exception as e:
            logger.error(f"❌ Error al verificar carga: {e}")
            return 0
    
    def ejecutar_etl(self):
        """Ejecutar el proceso ETL completo"""
        logger.info("🚀 Iniciando proceso ETL...")
        
        try:
            # 1. Conectar a la base de datos
            if not self.conectar_base_datos():
                return False
            
            # 2. Extraer datos
            df_origen = self.extraer_datos()
            if df_origen is None or len(df_origen) == 0:
                logger.warning("⚠️ No se encontraron datos para procesar")
                return False
            
            # SOLO PARA PRUEBA: procesar solo los primeros 5 registros
            #df_origen = df_origen.head(5)
            #logger.info(f"🧪 Modo prueba: procesando solo {len(df_origen)} registros")
            # Para procesar todo, comenta la línea anterior y descomenta la siguiente:
            logger.info(f"📊 Procesando {len(df_origen)} registros completos")
            
            # 3. Transformar datos
            df_transformado = self.transformar_datos(df_origen)
            if df_transformado is None or len(df_transformado) == 0:
                logger.warning("⚠️ No se pudieron transformar los datos")
                return False
            
            # 4. Cargar datos de vehicle_appraisal
            if not self.cargar_datos(df_transformado):
                return False
            
            # 5. Obtener los IDs de vehicle_appraisal para las deducciones
            vehicle_appraisal_ids = self.obtener_vehicle_appraisal_ids(df_origen)
            
            # Si no encontramos IDs, intentar con los últimos registros insertados
            if len(vehicle_appraisal_ids) == 0:
                logger.warning("⚠️ No se encontraron IDs específicos, buscando últimos registros insertados...")
                vehicle_appraisal_ids = self.obtener_ultimos_ids_insertados(1000)
            
            # 6. Procesar y cargar deducciones
            deducciones = self.procesar_deducciones(df_origen, vehicle_appraisal_ids)
            logger.info(f"🔍 Deducciones procesadas: {len(deducciones) if deducciones else 0}")
            if deducciones:
                logger.info(f"📋 Ejemplos de deducciones a insertar: {deducciones[:3]}")
                if not self.cargar_deducciones(deducciones):
                    logger.warning("⚠️ Error al cargar deducciones, pero el ETL principal se completó")
            else:
                logger.warning("⚠️ No se generaron deducciones para insertar")
            
            # 7. Verificar carga
            self.verificar_carga()
            
            logger.info("🎉 Proceso ETL completado exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error general en ETL: {e}")
            return False
        
        finally:
            if self.db_connection:
                self.db_connection.close_connection()

def main():
    """Función principal"""
    etl = ETLAvaluos()
    exito = etl.ejecutar_etl()
    
    if exito:
        print("✅ ETL ejecutado correctamente")
    else:
        print("❌ ETL falló")

if __name__ == "__main__":
    main() 