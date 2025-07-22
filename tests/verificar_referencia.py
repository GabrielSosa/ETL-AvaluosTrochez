#!/usr/bin/env python3
"""
Script para verificar si hay alguna columna que contenga el NUMERO_CER en vehicle_appraisal
"""

import pandas as pd
import logging
from sqlalchemy import text
from database_connection import DatabaseConnection

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verificar_referencia():
    """Verificar si hay alguna columna que contenga el NUMERO_CER en vehicle_appraisal"""
    try:
        # Conectar a la base de datos
        db_connection = DatabaseConnection()
        if not db_connection.test_connection():
            logger.error("❌ No se pudo conectar a la base de datos")
            return
        
        logger.info("🔍 Verificando si hay referencia al NUMERO_CER en vehicle_appraisal...")
        
        # Obtener algunos NUMERO_CER de mi_tabla
        query_mi_tabla = "SELECT \"NUMERO_CER\" FROM mi_tabla LIMIT 5"
        df_mi_tabla = pd.read_sql_query(query_mi_tabla, db_connection.get_engine())
        numeros_cer = df_mi_tabla['NUMERO_CER'].dropna().unique()
        
        logger.info(f"📊 NUMERO_CER de mi_tabla: {numeros_cer.tolist()}")
        
        # Buscar estos valores en vehicle_appraisal
        for numero_cer in numeros_cer:
            logger.info(f"🔍 Buscando NUMERO_CER {numero_cer} en vehicle_appraisal...")
            
            # Buscar en todas las columnas numéricas
            query_buscar = f"""
                SELECT vehicle_appraisal_id, appraisal_date, brand, applicant, owner
                FROM vehicle_appraisal 
                WHERE vehicle_appraisal_id = {numero_cer}
                LIMIT 1
            """
            
            try:
                df_resultado = pd.read_sql_query(query_buscar, db_connection.get_engine())
                if len(df_resultado) > 0:
                    logger.info(f"✅ Encontrado vehicle_appraisal_id {numero_cer}: {df_resultado.iloc[0].to_dict()}")
                else:
                    logger.info(f"❌ No se encontró vehicle_appraisal_id {numero_cer}")
            except Exception as e:
                logger.info(f"❌ Error buscando {numero_cer}: {e}")
        
        # Verificar si hay alguna columna que pueda contener el NUMERO_CER
        query_columnas = """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'vehicle_appraisal' 
            AND data_type IN ('integer', 'bigint', 'numeric', 'double precision')
            ORDER BY column_name
        """
        df_columnas = pd.read_sql_query(query_columnas, db_connection.get_engine())
        
        logger.info(f"📊 Columnas numéricas en vehicle_appraisal:")
        for _, row in df_columnas.iterrows():
            logger.info(f"   - {row['column_name']} ({row['data_type']})")
        
        # Verificar si vehicle_appraisal_id coincide con algún NUMERO_CER
        coincidencias = []
        for numero_cer in numeros_cer:
            if numero_cer in [72625, 72626, 72627]:  # Los IDs que vimos en la muestra
                coincidencias.append(numero_cer)
        
        if coincidencias:
            logger.info(f"✅ Coincidencias encontradas: {coincidencias}")
            logger.info("🎉 Los vehicle_appraisal_id coinciden con NUMERO_CER")
        else:
            logger.warning("⚠️ NO hay coincidencias entre vehicle_appraisal_id y NUMERO_CER")
            logger.info("💡 Necesitamos agregar una columna referencia_original a vehicle_appraisal")
        
        db_connection.close_connection()
        logger.info("✅ Verificación completada")
        
    except Exception as e:
        logger.error(f"❌ Error en verificación: {e}")

if __name__ == "__main__":
    verificar_referencia() 