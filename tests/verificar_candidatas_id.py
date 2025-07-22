#!/usr/bin/env python3
"""
Script para verificar si las columnas candidatas tienen valores únicos
"""

import pandas as pd
import logging
from database_connection import DatabaseConnection

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verificar_candidatas_id():
    """Verificar si las columnas candidatas tienen valores únicos"""
    try:
        # Conectar a la base de datos
        db_connection = DatabaseConnection()
        if not db_connection.test_connection():
            logger.error("❌ No se pudo conectar a la base de datos")
            return
        
        logger.info("🔍 Verificando columnas candidatas para ID único...")
        
        # Columnas candidatas identificadas
        columnas_candidatas = ['CERTIFICA2', 'CERTIFICAD', 'CERT___']
        
        for columna in columnas_candidatas:
            logger.info(f"📊 Analizando columna: {columna}")
            
            # Verificar total de registros
            query_total = f'SELECT COUNT(*) as total FROM mi_tabla WHERE "{columna}" IS NOT NULL'
            df_total = pd.read_sql_query(query_total, db_connection.get_engine())
            total_registros = df_total['total'].iloc[0]
            
            # Verificar valores únicos
            query_unicos = f'SELECT COUNT(DISTINCT "{columna}") as unicos FROM mi_tabla WHERE "{columna}" IS NOT NULL'
            df_unicos = pd.read_sql_query(query_unicos, db_connection.get_engine())
            unicos = df_unicos['unicos'].iloc[0]
            
            # Verificar valores NULL
            query_nulos = f'SELECT COUNT(*) as nulos FROM mi_tabla WHERE "{columna}" IS NULL'
            df_nulos = pd.read_sql_query(query_nulos, db_connection.get_engine())
            nulos = df_nulos['nulos'].iloc[0]
            
            logger.info(f"   - Total registros con valor: {total_registros}")
            logger.info(f"   - Valores únicos: {unicos}")
            logger.info(f"   - Registros NULL: {nulos}")
            
            if total_registros > 0:
                porcentaje_unicos = (unicos / total_registros) * 100
                logger.info(f"   - Porcentaje de valores únicos: {porcentaje_unicos:.2f}%")
                
                if unicos == total_registros:
                    logger.info(f"   ✅ {columna} tiene valores únicos!")
                elif porcentaje_unicos > 95:
                    logger.info(f"   ⚠️ {columna} tiene {porcentaje_unicos:.2f}% valores únicos")
                else:
                    logger.info(f"   ❌ {columna} tiene muchos duplicados")
            
            # Mostrar algunos ejemplos de valores
            query_ejemplos = f'SELECT DISTINCT "{columna}" FROM mi_tabla WHERE "{columna}" IS NOT NULL LIMIT 5'
            df_ejemplos = pd.read_sql_query(query_ejemplos, db_connection.get_engine())
            if len(df_ejemplos) > 0:
                logger.info(f"   - Ejemplos de valores: {list(df_ejemplos[columna].head())}")
            
            logger.info("")
        
        db_connection.close_connection()
        logger.info("✅ Verificación completada")
        
    except Exception as e:
        logger.error(f"❌ Error en verificación: {e}")

if __name__ == "__main__":
    verificar_candidatas_id() 