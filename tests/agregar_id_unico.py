#!/usr/bin/env python3
"""
Script para agregar una columna de ID único a mi_tabla
"""

import pandas as pd
import logging
from sqlalchemy import text
from database_connection import DatabaseConnection

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def agregar_id_unico():
    """Agregar una columna de ID único a mi_tabla"""
    try:
        # Conectar a la base de datos
        db_connection = DatabaseConnection()
        if not db_connection.test_connection():
            logger.error("❌ No se pudo conectar a la base de datos")
            return
        
        logger.info("🔍 Agregando columna de ID único a mi_tabla...")
        
        # Verificar si ya existe la columna
        query_verificar = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'mi_tabla' 
            AND column_name = 'id_unico'
        """
        df_verificar = pd.read_sql_query(query_verificar, db_connection.get_engine())
        
        if len(df_verificar) > 0:
            logger.info("⚠️ La columna 'id_unico' ya existe en mi_tabla")
            return
        
        # Agregar la columna id_unico
        with db_connection.get_engine().connect() as conn:
            # Agregar columna
            query_add = """
                ALTER TABLE mi_tabla 
                ADD COLUMN id_unico SERIAL
            """
            conn.execute(text(query_add))
            conn.commit()
            logger.info("✅ Columna 'id_unico' agregada exitosamente")
        
        # Verificar que se agregó correctamente
        query_verificar_final = """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'mi_tabla' 
            AND column_name = 'id_unico'
        """
        df_verificar_final = pd.read_sql_query(query_verificar_final, db_connection.get_engine())
        
        if len(df_verificar_final) > 0:
            logger.info(f"✅ Columna 'id_unico' creada con tipo: {df_verificar_final['data_type'].iloc[0]}")
            
            # Verificar algunos valores
            query_ejemplos = "SELECT id_unico FROM mi_tabla ORDER BY id_unico LIMIT 5"
            df_ejemplos = pd.read_sql_query(query_ejemplos, db_connection.get_engine())
            logger.info(f"📊 Ejemplos de IDs generados: {list(df_ejemplos['id_unico'])}")
            
            # Verificar que son únicos
            query_unicos = "SELECT COUNT(DISTINCT id_unico) as unicos, COUNT(*) as total FROM mi_tabla"
            df_unicos = pd.read_sql_query(query_unicos, db_connection.get_engine())
            unicos = df_unicos['unicos'].iloc[0]
            total = df_unicos['total'].iloc[0]
            
            if unicos == total:
                logger.info(f"✅ Todos los {total} registros tienen IDs únicos")
            else:
                logger.warning(f"⚠️ Problema: {total} registros pero solo {unicos} IDs únicos")
        else:
            logger.error("❌ No se pudo verificar la creación de la columna")
        
        db_connection.close_connection()
        logger.info("✅ Proceso completado")
        
    except Exception as e:
        logger.error(f"❌ Error al agregar ID único: {e}")

if __name__ == "__main__":
    agregar_id_unico() 