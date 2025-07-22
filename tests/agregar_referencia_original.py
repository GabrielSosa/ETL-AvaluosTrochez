#!/usr/bin/env python3
"""
Script para agregar la columna referencia_original a la tabla vehicle_appraisal
"""

import logging
from sqlalchemy import text
from database_connection import DatabaseConnection

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def agregar_referencia_original():
    """Agregar la columna referencia_original a vehicle_appraisal"""
    try:
        # Conectar a la base de datos
        db_connection = DatabaseConnection()
        if not db_connection.test_connection():
            logger.error("❌ No se pudo conectar a la base de datos")
            return
        
        logger.info("🔧 Agregando columna referencia_original a vehicle_appraisal...")
        
        # Verificar si la columna ya existe
        query_check = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'vehicle_appraisal' 
            AND column_name = 'referencia_original'
        """
        
        with db_connection.get_engine().connect() as connection:
            result = connection.execute(text(query_check))
            column_exists = result.fetchone() is not None
            
            if column_exists:
                logger.info("✅ La columna referencia_original ya existe")
                return
            
            # Agregar la columna
            query_add = """
                ALTER TABLE vehicle_appraisal 
                ADD COLUMN referencia_original DOUBLE PRECISION
            """
            
            connection.execute(text(query_add))
            connection.commit()
            
            logger.info("✅ Columna referencia_original agregada exitosamente")
            
            # Verificar que se agregó correctamente
            result = connection.execute(text(query_check))
            if result.fetchone():
                logger.info("✅ Verificación: la columna se agregó correctamente")
            else:
                logger.error("❌ Error: la columna no se agregó correctamente")
        
        db_connection.close_connection()
        logger.info("✅ Proceso completado")
        
    except Exception as e:
        logger.error(f"❌ Error agregando columna: {e}")

if __name__ == "__main__":
    agregar_referencia_original() 