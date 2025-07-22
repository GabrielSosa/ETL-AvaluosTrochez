#!/usr/bin/env python3
"""
Script de prueba para verificar el procesamiento de cilindrada
"""

from etl_avaluos import ETLAvaluos

def test_procesamiento_cilindrada():
    """Probar el procesamiento de diferentes valores de cilindrada"""
    
    etl = ETLAvaluos()
    
    # Casos de prueba
    casos_prueba = [
        (800.0, "800cc -> 0.8L"),
        (1600.0, "1600cc -> 1.6L"),
        (2000.0, "2000cc -> 2.0L"),
        (1.6, "1.6L -> 1.6L"),
        (2.0, "2.0L -> 2.0L"),
        (99.9, "99.9L -> 99.9L"),
        (100.0, "100L -> None (sospechoso)"),
        (None, "None -> None"),
        ("", "Vacío -> None"),
        ("NULL", "NULL -> None"),
    ]
    
    print("🧪 Probando procesamiento de cilindrada:")
    print("=" * 50)
    
    for valor, descripcion in casos_prueba:
        resultado = etl.procesar_cilindrada(valor)
        valor_str = str(valor) if valor is not None else "None"
        resultado_str = str(resultado) if resultado is not None else "None"
        print(f"{descripcion:25} | Entrada: {valor_str:8} | Salida: {resultado_str}")
    
    print("=" * 50)
    print("✅ Pruebas completadas")

if __name__ == "__main__":
    test_procesamiento_cilindrada() 