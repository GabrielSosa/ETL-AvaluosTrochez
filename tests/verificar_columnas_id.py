#!/usr/bin/env python3
"""
Script para verificar columnas que puedan servir como ID único en mi_tabla
"""

import pandas as pd
import logging
from database_connection import DatabaseConnection

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verificar_columnas_id():
    """Verificar columnas que puedan servir como ID único"""
    try:
        # Conectar a la base de datos
        db_connection = DatabaseConnection()
        if not db_connection.test_connection():
            logger.error("❌ No se pudo conectar a la base de datos")
            return
        
        logger.info("🔍 Verificando columnas candidatas para ID único...")
        
        # Verificar todas las columnas de mi_tabla
        query_columnas = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'mi_tabla' 
            ORDER BY column_name
        """
        df_columnas = pd.read_sql_query(query_columnas, db_connection.get_engine())
        
        logger.info(f"📊 Total de columnas en mi_tabla: {len(df_columnas)}")
        
        # Buscar columnas que podrían servir como ID
        columnas_candidatas = []
        for _, row in df_columnas.iterrows():
            col_name = row['column_name'].upper()
            if any(keyword in col_name for keyword in ['ID', 'KEY', 'CERT', 'LLAVE', 'UNICO', 'UNIQUE']):
                columnas_candidatas.append(row)
        
        if columnas_candidatas:
            logger.info("📊 Columnas candidatas para ID único:")
            for row in columnas_candidatas:
                logger.info(f"   - {row['column_name']} ({row['data_type']}, nullable: {row['is_nullable']})")
        else:
            logger.info("📊 No se encontraron columnas candidatas para ID único")
        
        # Mostrar las primeras 10 columnas para referencia
        logger.info("📊 Primeras 10 columnas de mi_tabla:")
        for i, row in df_columnas.head(10).iterrows():
            logger.info(f"   - {row['column_name']} ({row['data_type']})")
        
        db_connection.close_connection()
        logger.info("✅ Verificación completada")
        
    except Exception as e:
        logger.error(f"❌ Error en verificación: {e}")

if __name__ == "__main__":
    verificar_columnas_id() 