#!/usr/bin/env python3
"""
Script para verificar la situación actual de NUMERO_CER en mi_tabla
"""

import pandas as pd
import logging
from sqlalchemy import text
from database_connection import DatabaseConnection

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verificar_numero_cer():
    """Verificar la situación actual de NUMERO_CER en mi_tabla"""
    try:
        # Conectar a la base de datos
        db_connection = DatabaseConnection()
        if not db_connection.test_connection():
            logger.error("❌ No se pudo conectar a la base de datos")
            return
        
        logger.info("🔍 Verificando situación de NUMERO_CER en mi_tabla...")
        
        # Verificar total de registros
        query_total = "SELECT COUNT(*) as total FROM mi_tabla"
        df_total = pd.read_sql_query(query_total, db_connection.get_engine())
        total_registros = df_total['total'].iloc[0]
        logger.info(f"📊 Total de registros en mi_tabla: {total_registros}")
        
        # Verificar valores NULL en NUMERO_CER
        query_nulos = "SELECT COUNT(*) as nulos FROM mi_tabla WHERE \"NUMERO_CER\" IS NULL"
        df_nulos = pd.read_sql_query(query_nulos, db_connection.get_engine())
        nulos = df_nulos['nulos'].iloc[0]
        logger.info(f"📊 Registros con NUMERO_CER NULL: {nulos}")
        
        # Verificar valores únicos de NUMERO_CER
        query_unicos = "SELECT COUNT(DISTINCT \"NUMERO_CER\") as unicos FROM mi_tabla WHERE \"NUMERO_CER\" IS NOT NULL"
        df_unicos = pd.read_sql_query(query_unicos, db_connection.get_engine())
        unicos = df_unicos['unicos'].iloc[0]
        logger.info(f"📊 Valores únicos de NUMERO_CER: {unicos}")
        
        # Verificar duplicados
        query_duplicados = """
            SELECT "NUMERO_CER", COUNT(*) as cantidad
            FROM mi_tabla 
            WHERE "NUMERO_CER" IS NOT NULL
            GROUP BY "NUMERO_CER"
            HAVING COUNT(*) > 1
            ORDER BY cantidad DESC
            LIMIT 10
        """
        df_duplicados = pd.read_sql_query(query_duplicados, db_connection.get_engine())
        
        if len(df_duplicados) > 0:
            logger.warning(f"⚠️ Se encontraron {len(df_duplicados)} valores duplicados de NUMERO_CER:")
            for _, row in df_duplicados.iterrows():
                logger.warning(f"   - NUMERO_CER: {row['NUMERO_CER']}, Cantidad: {row['cantidad']}")
        else:
            logger.info("✅ No se encontraron duplicados en NUMERO_CER")
        
        # Verificar si ya existe una columna de ID único
        query_columnas = """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'mi_tabla' 
            AND column_name LIKE '%ID%' OR column_name LIKE '%id%'
            ORDER BY column_name
        """
        df_columnas = pd.read_sql_query(query_columnas, db_connection.get_engine())
        
        if len(df_columnas) > 0:
            logger.info(f"📊 Columnas que podrían servir como ID único:")
            for _, row in df_columnas.iterrows():
                logger.info(f"   - {row['column_name']} ({row['data_type']})")
        else:
            logger.info("📊 No se encontraron columnas con 'ID' en el nombre")
        
        # Verificar si existe una columna de llave única
        query_llave = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'mi_tabla' 
            AND column_name IN ('id', 'ID', 'llave', 'LLAVE', 'key', 'KEY', 'cert', 'CERT')
            ORDER BY column_name
        """
        df_llave = pd.read_sql_query(query_llave, db_connection.get_engine())
        
        if len(df_llave) > 0:
            logger.info(f"📊 Columnas que podrían ser la nueva llave:")
            for _, row in df_llave.iterrows():
                logger.info(f"   - {row['column_name']} ({row['data_type']}, nullable: {row['is_nullable']})")
        else:
            logger.info("📊 No se encontraron columnas candidatas para llave única")
        
        db_connection.close_connection()
        logger.info("✅ Verificación completada")
        
    except Exception as e:
        logger.error(f"❌ Error en verificación: {e}")

if __name__ == "__main__":
    verificar_numero_cer() 