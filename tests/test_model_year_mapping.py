import pandas as pd
from etl_avaluos import ETLAvaluos
import math

def test_model_year_mapping():
    etl = ETLAvaluos()
    # Simular datos de entrada con diferentes casos
    data = {
        'A_O': [2020, '2015', None, '', 'NULL', 1899, 2031, 'N/A', '2000a', 1999],
        # Campos mínimos requeridos para que transformar_datos no falle
        'CILINDRADA': [2000]*10,
        'COMBUSTIBL': ['Gasolina']*10,
        'id_unico': list(range(1, 11)),
        'NUMERO_CER': [1]*10,
        'SOLICITANT': ['A']*10,
        'PROPIETARI': ['B']*10,
        'MARCA': ['C']*10,
        'MODELO': ['D']*10,
        'KMS': [10000]*10,
        'COLOR': ['Rojo']*10,
        'PLACAS': ['ABC']*10,
        'NOTA': ['']*10,
        'ACCESORIOS': ['']*10,
        'VIN_CHASIS': ['']*10,
        '__VIN_DE_C': ['']*10,
        '__VIN_DE_M': ['']*10,
        'VIN_DE_MOT': ['']*10,
        'TOTAL_DE_R': [0]*10,
        'MODIF_KM': [0]*10,
        'VALOR_EXTR': [0]*10,
        'DESCUENTOS': [0]*10,
        'AV_BANC_NU': [0]*10,
        'AVALUO_BAN': [0]*10,
        '_FECHAS_1': [None]*10,
        'AVALUO_DIS': [0]*10,
        'VALOR_GIBS': [0]*10,
        'AV_DIST_NU': [0]*10,
        'MOTOR1': [None]*10,
        'MOTOR2': ['']*10,
        'TRANSMISIO': [None]*10,
        'TRANSMICIO': ['']*10,
        'SUSPENSION': [None]*10,
        'SUSPENSIO2': ['']*10,
        'DIRECCION': [None]*10,
        'DIRECCION2': ['']*10,
        'FRENOS': [None]*10,
        'FRENOS2': ['']*10,
        'LLANTAS': [None]*10,
        'RUEDAS': ['']*10,
        'SIST_ELECT': [None]*10,
        'SISTELEC2': ['']*10,
        'INTYACC2': ['']*10
    }
    df = pd.DataFrame(data)
    df_transformado = etl.transformar_datos(df)
    print('A_O original:', df['A_O'].tolist())
    print('model_year transformado:', df_transformado['model_year'].tolist())
    # Esperado: solo los valores entre 1900 y 2030 y que sean enteros deben aparecer, el resto como None
    esperado = [2020, 2015, None, None, None, None, None, None, None, 1999]
    resultado = df_transformado['model_year'].tolist()
    # Normalizar resultado: nan -> None
    resultado = [int(x) if isinstance(x, int) or (isinstance(x, float) and x.is_integer()) else None for x in resultado]
    assert resultado == esperado, f"Esperado {esperado}, pero obtuve {resultado}"
    print('✅ Test de mapeo de model_year exitoso')

if __name__ == "__main__":
    test_model_year_mapping() 