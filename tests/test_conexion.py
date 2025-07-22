#!/usr/bin/env python3
"""
Script para probar la conexión mejorada a la base de datos
"""

from database_connection import DatabaseConnection
import time

def test_conexion():
    """Probar la conexión a la base de datos"""
    
    print("🧪 Probando conexión a la base de datos...")
    print("=" * 50)
    
    try:
        # Crear conexión
        db = DatabaseConnection()
        
        # Probar conexión
        if db.test_connection():
            print("✅ Conexión exitosa")
            
            # Probar consulta simple
            try:
                with db.get_engine().connect() as conn:
                    result = conn.execute("SELECT COUNT(*) FROM public.mi_tabla")
                    count = result.fetchone()[0]
                    print(f"📊 Registros en mi_tabla: {count}")
                    
                    # Probar consulta en vehicle_appraisal
                    result = conn.execute("SELECT COUNT(*) FROM public.vehicle_appraisal")
                    count = result.fetchone()[0]
                    print(f"📊 Registros en vehicle_appraisal: {count}")
                    
            except Exception as e:
                print(f"❌ Error en consulta: {e}")
        
        # Cerrar conexión
        db.close_connection()
        print("🔒 Conexión cerrada")
        
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
    
    print("=" * 50)
    print("✅ Prueba completada")

if __name__ == "__main__":
    test_conexion() 