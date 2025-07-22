#!/usr/bin/env python3
"""
Script para verificar si los datos se están guardando correctamente en referencia_original
"""

import pandas as pd
import logging
from sqlalchemy import text
from database_connection import DatabaseConnection

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verificar_referencia_guardada():
    """Verificar si los datos se están guardando correctamente en referencia_original"""
    try:
        # Conectar a la base de datos
        db_connection = DatabaseConnection()
        if not db_connection.test_connection():
            logger.error("❌ No se pudo conectar a la base de datos")
            return
        
        logger.info("🔍 Verificando datos en referencia_original...")
        
        # Verificar los últimos registros insertados
        query_ultimos = """
            SELECT vehicle_appraisal_id, referencia_original, brand, applicant, owner
            FROM vehicle_appraisal 
            ORDER BY vehicle_appraisal_id DESC 
            LIMIT 10
        """
        df_ultimos = pd.read_sql_query(query_ultimos, db_connection.get_engine())
        
        logger.info(f"📊 Últimos registros insertados:")
        for _, row in df_ultimos.iterrows():
            logger.info(f"   - ID: {row['vehicle_appraisal_id']}, Referencia: {row['referencia_original']}, Brand: {row['brand']}")
        
        # Verificar si hay valores en referencia_original
        query_conteo = """
            SELECT COUNT(*) as total, 
                   COUNT(referencia_original) as con_referencia,
                   COUNT(CASE WHEN referencia_original IS NOT NULL THEN 1 END) as no_nulos
            FROM vehicle_appraisal
        """
        df_conteo = pd.read_sql_query(query_conteo, db_connection.get_engine())
        
        total = df_conteo['total'].iloc[0]
        con_referencia = df_conteo['con_referencia'].iloc[0]
        no_nulos = df_conteo['no_nulos'].iloc[0]
        
        logger.info(f"📊 Estadísticas de referencia_original:")
        logger.info(f"   - Total registros: {total}")
        logger.info(f"   - Con referencia: {con_referencia}")
        logger.info(f"   - No nulos: {no_nulos}")
        
        if con_referencia == 0:
            logger.warning("⚠️ La columna referencia_original está vacía")
            logger.info("💡 Los datos no se están guardando en referencia_original")
        else:
            logger.info("✅ Los datos se están guardando en referencia_original")
            
            # Mostrar algunos ejemplos de referencia_original
            query_ejemplos = """
                SELECT referencia_original, brand, applicant
                FROM vehicle_appraisal 
                WHERE referencia_original IS NOT NULL
                LIMIT 5
            """
            df_ejemplos = pd.read_sql_query(query_ejemplos, db_connection.get_engine())
            
            logger.info(f"📊 Ejemplos de referencia_original:")
            for _, row in df_ejemplos.iterrows():
                logger.info(f"   - Referencia: {row['referencia_original']}, Brand: {row['brand']}")
        
        db_connection.close_connection()
        logger.info("✅ Verificación completada")
        
    except Exception as e:
        logger.error(f"❌ Error en verificación: {e}")

if __name__ == "__main__":
    verificar_referencia_guardada() 