#!/usr/bin/env python3
"""
Script de diagnóstico para verificar el mapeo entre NUMERO_CER y vehicle_appraisal_id
"""

import pandas as pd
import logging
from sqlalchemy import text
from database_connection import DatabaseConnection

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def diagnosticar_mapeo():
    """Diagnosticar el mapeo entre NUMERO_CER y vehicle_appraisal_id"""
    try:
        # Conectar a la base de datos
        db_connection = DatabaseConnection()
        if not db_connection.test_connection():
            logger.error("❌ No se pudo conectar a la base de datos")
            return
        
        logger.info("🔍 Iniciando diagnóstico de mapeo...")
        
        # 1. Verificar datos en mi_tabla
        query_mi_tabla = "SELECT \"NUMERO_CER\", \"MOTOR1\", \"MOTOR2\", \"TRANSMISIO\", \"TRANSMICIO\" FROM mi_tabla LIMIT 10"
        df_mi_tabla = pd.read_sql_query(query_mi_tabla, db_connection.get_engine())
        logger.info(f"📊 Datos en mi_tabla (primeros 10):")
        logger.info(f"   NUMERO_CER únicos: {df_mi_tabla['NUMERO_CER'].unique()}")
        logger.info(f"   Ejemplos de NUMERO_CER: {df_mi_tabla['NUMERO_CER'].head().tolist()}")
        
        # 2. Verificar datos en vehicle_appraisal
        query_vehicle_appraisal = "SELECT vehicle_appraisal_id, referencia_original FROM vehicle_appraisal ORDER BY vehicle_appraisal_id DESC LIMIT 10"
        df_vehicle_appraisal = pd.read_sql_query(query_vehicle_appraisal, db_connection.get_engine())
        logger.info(f"📊 Datos en vehicle_appraisal (últimos 10):")
        logger.info(f"   referencia_original únicos: {df_vehicle_appraisal['referencia_original'].unique()}")
        logger.info(f"   Ejemplos de referencia_original: {df_vehicle_appraisal['referencia_original'].head().tolist()}")
        
        # 3. Verificar si hay coincidencias
        numeros_cer_mi_tabla = set(df_mi_tabla['NUMERO_CER'].dropna().unique())
        referencias_vehicle = set(df_vehicle_appraisal['referencia_original'].dropna().unique())
        
        coincidencias = numeros_cer_mi_tabla.intersection(referencias_vehicle)
        logger.info(f"🔍 Análisis de coincidencias:")
        logger.info(f"   NUMERO_CER en mi_tabla: {len(numeros_cer_mi_tabla)}")
        logger.info(f"   referencia_original en vehicle_appraisal: {len(referencias_vehicle)}")
        logger.info(f"   Coincidencias encontradas: {len(coincidencias)}")
        
        if len(coincidencias) > 0:
            logger.info(f"   Ejemplos de coincidencias: {list(coincidencias)[:5]}")
        else:
            logger.warning("⚠️ NO HAY COINCIDENCIAS entre NUMERO_CER y referencia_original")
            
            # Verificar tipos de datos
            logger.info(f"📊 Tipos de datos:")
            logger.info(f"   NUMERO_CER tipo: {df_mi_tabla['NUMERO_CER'].dtype}")
            logger.info(f"   referencia_original tipo: {df_vehicle_appraisal['referencia_original'].dtype}")
            
            # Mostrar ejemplos de ambos
            logger.info(f"📊 Ejemplos de NUMERO_CER: {df_mi_tabla['NUMERO_CER'].head().tolist()}")
            logger.info(f"📊 Ejemplos de referencia_original: {df_vehicle_appraisal['referencia_original'].head().tolist()}")
        
        # 4. Verificar si hay deducciones en appraisal_deductions
        query_deducciones = "SELECT COUNT(*) as total FROM appraisal_deductions"
        resultado_deducciones = pd.read_sql_query(query_deducciones, db_connection.get_engine())
        total_deducciones = resultado_deducciones['total'].iloc[0]
        logger.info(f"📊 Total de deducciones en appraisal_deductions: {total_deducciones}")
        
        if total_deducciones > 0:
            query_ejemplos = "SELECT vehicle_appraisal_id, amount, description FROM appraisal_deductions LIMIT 5"
            df_ejemplos = pd.read_sql_query(query_ejemplos, db_connection.get_engine())
            logger.info(f"📊 Ejemplos de deducciones:")
            for _, row in df_ejemplos.iterrows():
                logger.info(f"   - ID: {row['vehicle_appraisal_id']}, Amount: {row['amount']}, Description: {row['description']}")
        
        db_connection.close_connection()
        logger.info("✅ Diagnóstico completado")
        
    except Exception as e:
        logger.error(f"❌ Error en diagnóstico: {e}")

if __name__ == "__main__":
    diagnosticar_mapeo() 