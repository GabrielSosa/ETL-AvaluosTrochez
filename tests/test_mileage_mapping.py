import pandas as pd
from etl_avaluos import ETLAvaluos
import math

def test_mileage_mapping():
    etl = ETLAvaluos()
    # Simular datos de entrada con diferentes casos
    data = {
        'KMS': [10000, '25000', None, '', 'NULL', -100, 'N/A', '10000km', 0, '000123', '12,345', ' 54321 ', 12345.0, '10.000', '99999.0', '-5000', '1,000,000', 'abc', '  2000', 123.45],
        # Campos mínimos requeridos para que transformar_datos no falle
        'A_O': [2020]*20,
        'CILINDRADA': [2000]*20,
        'COMBUSTIBL': ['Gasolina']*20,
        'id_unico': list(range(1, 21)),
        'NUMERO_CER': [1]*20,
        'SOLICITANT': ['A']*20,
        'PROPIETARI': ['B']*20,
        'MARCA': ['C']*20,
        'MODELO': ['D']*20,
        'COLOR': ['Rojo']*20,
        'PLACAS': ['ABC']*20,
        'NOTA': ['']*20,
        'ACCESORIOS': ['']*20,
        'VIN_CHASIS': ['']*20,
        '__VIN_DE_C': ['']*20,
        '__VIN_DE_M': ['']*20,
        'VIN_DE_MOT': ['']*20,
        'TOTAL_DE_R': [0]*20,
        'MODIF_KM': [0]*20,
        'VALOR_EXTR': [0]*20,
        'DESCUENTOS': [0]*20,
        'AV_BANC_NU': [0]*20,
        'AVALUO_BAN': [0]*20,
        '_FECHAS_1': [None]*20,
        'AVALUO_DIS': [0]*20,
        'VALOR_GIBS': [0]*20,
        'AV_DIST_NU': [0]*20,
        'MOTOR1': [None]*20,
        'MOTOR2': ['']*20,
        'TRANSMISIO': [None]*20,
        'TRANSMICIO': ['']*20,
        'SUSPENSION': [None]*20,
        'SUSPENSIO2': ['']*20,
        'DIRECCION': [None]*20,
        'DIRECCION2': ['']*20,
        'FRENOS': [None]*20,
        'FRENOS2': ['']*20,
        'LLANTAS': [None]*20,
        'RUEDAS': ['']*20,
        'SIST_ELECT': [None]*20,
        'SISTELEC2': ['']*20,
        'INTYACC2': ['']*20
    }
    df = pd.DataFrame(data)
    df_transformado = etl.transformar_datos(df)
    print('KMS original:', df['KMS'].tolist())
    print('mileage transformado:', df_transformado['mileage'].tolist())
    # Esperado: solo los valores enteros puros, positivos y formatos válidos deben aparecer, el resto como None
    esperado = [10000, 25000, None, None, None, None, None, None, 0, 123, 12345, 54321, 12345, 10000, 999990, None, 1000000, None, 2000, None]
    resultado = df_transformado['mileage'].tolist()
    # Normalizar resultado: nan -> None, float entero -> int
    resultado = [int(x) if isinstance(x, (int, float)) and not pd.isna(x) and float(x).is_integer() else None for x in resultado]
    assert resultado == esperado, f"Esperado {esperado}, pero obtuve {resultado}"
    print('✅ Test de mapeo de mileage exitoso')

if __name__ == "__main__":
    test_mileage_mapping() 