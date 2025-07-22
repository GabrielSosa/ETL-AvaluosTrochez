#!/usr/bin/env python3
"""
Script para verificar si existe la columna cert en vehicle_appraisal
"""

import pandas as pd
import logging
from database_connection import DatabaseConnection

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verificar_columna_cert():
    """Verificar si existe la columna cert en vehicle_appraisal"""
    try:
        # Conectar a la base de datos
        db_connection = DatabaseConnection()
        if not db_connection.test_connection():
            logger.error("❌ No se pudo conectar a la base de datos")
            return
        
        logger.info("🔍 Verificando columna cert en vehicle_appraisal...")
        
        # Verificar si existe la columna cert
        query_verificar = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'vehicle_appraisal' 
            AND column_name = 'cert'
        """
        df_verificar = pd.read_sql_query(query_verificar, db_connection.get_engine())
        
        if len(df_verificar) > 0:
            logger.info(f"✅ La columna 'cert' existe en vehicle_appraisal")
            logger.info(f"   - Tipo de datos: {df_verificar['data_type'].iloc[0]}")
            logger.info(f"   - Permite NULL: {df_verificar['is_nullable'].iloc[0]}")
        else:
            logger.warning("⚠️ La columna 'cert' NO existe en vehicle_appraisal")
            logger.info("📝 Se necesita agregar la columna cert")
        
        # Mostrar todas las columnas de vehicle_appraisal para referencia
        query_columnas = """
            SELECT column_name, data_type
            FROM information_schema.columns 
            WHERE table_name = 'vehicle_appraisal' 
            ORDER BY column_name
        """
        df_columnas = pd.read_sql_query(query_columnas, db_connection.get_engine())
        
        logger.info(f"📊 Total de columnas en vehicle_appraisal: {len(df_columnas)}")
        logger.info("📊 Columnas de vehicle_appraisal:")
        for _, row in df_columnas.iterrows():
            logger.info(f"   - {row['column_name']} ({row['data_type']})")
        
        db_connection.close_connection()
        logger.info("✅ Verificación completada")
        
    except Exception as e:
        logger.error(f"❌ Error en verificación: {e}")

if __name__ == "__main__":
    verificar_columna_cert() 