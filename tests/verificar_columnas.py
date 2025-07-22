#!/usr/bin/env python3
"""
Script para verificar las columnas de la tabla mi_tabla
"""

import pandas as pd
import logging
from sqlalchemy import text
from database_connection import DatabaseConnection

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verificar_columnas():
    """Verificar las columnas de la tabla mi_tabla"""
    try:
        # Conectar a la base de datos
        db_connection = DatabaseConnection()
        if not db_connection.test_connection():
            logger.error("❌ No se pudo conectar a la base de datos")
            return
        
        logger.info("🔍 Verificando columnas de mi_tabla...")
        
        # Obtener información de las columnas
        query_info = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'mi_tabla'
            ORDER BY ordinal_position
        """
        df_info = pd.read_sql_query(query_info, db_connection.get_engine())
        
        logger.info(f"📊 Columnas en mi_tabla:")
        for _, row in df_info.iterrows():
            logger.info(f"   - {row['column_name']} ({row['data_type']}, nullable: {row['is_nullable']})")
        
        # Buscar columnas que puedan ser NUMERO_CER
        columnas_posibles = df_info[df_info['column_name'].str.contains('NUMERO|CER|ID|REFERENCIA', case=False, na=False)]
        if len(columnas_posibles) > 0:
            logger.info(f"🔍 Columnas que podrían ser NUMERO_CER:")
            for _, row in columnas_posibles.iterrows():
                logger.info(f"   - {row['column_name']}")
        
        # Obtener una muestra de datos
        query_muestra = "SELECT * FROM mi_tabla LIMIT 3"
        df_muestra = pd.read_sql_query(query_muestra, db_connection.get_engine())
        
        logger.info(f"📊 Muestra de datos (primeras 3 filas):")
        for col in df_muestra.columns:
            logger.info(f"   {col}: {df_muestra[col].tolist()}")
        
        db_connection.close_connection()
        logger.info("✅ Verificación completada")
        
    except Exception as e:
        logger.error(f"❌ Error en verificación: {e}")

if __name__ == "__main__":
    verificar_columnas() 