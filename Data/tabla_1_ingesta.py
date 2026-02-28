# =================================================================
# Instalacion de Librerias y Configuracion Inicial
# =================================================================

import yfinance as yf
import pandas as pd
import sqlite3
import datetime
import io
import requests
import os

# 1. Obtener la ruta de la carpeta donde vive ESTE script (SGBD/Data)
directorio_script = os.path.dirname(os.path.abspath(__file__))

# 2. Unir esa ruta con el nombre del archivo de entrada
csv_file = os.path.join(directorio_script, 'tickers.csv')

# Configuración de Fechas
fecha_inicio = "2016-01-01"
fecha_fin = datetime.datetime.now().strftime('%Y-%m-%d')

# 3. Lectura de Tickers con la ruta completa
if os.path.exists(csv_file):
    df_tickers = pd.read_csv(csv_file)
    # Asegúrate de que la columna se llame 'Ticker' con T mayúscula como en tu CSV
    tickers = df_tickers['Ticker'].tolist() 
    print(f"✅ Tickers cargados: {len(tickers)} activos desde {csv_file}.")
else:
    print(f"DEBUG: Python buscó en: {csv_file}")
    raise FileNotFoundError(f"❌ ERROR: No se encontró el archivo '{csv_file}'.")
    
# =================================================================
# Descarga de Datos Macroeconomicos (T10TYIE y CPI UK)
# =================================================================
def get_macro_data():
    print("Descargando indicadores macro...")
    indicators = {'T10YIE': 'us_inflation_exp', 'GBRCPIALLMINMEI': 'uk_cpi'}
    macro_frames = []
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    for fred_id, name in indicators.items():
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={fred_id}"
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code == 200:
            df = pd.read_csv(io.StringIO(res.text.strip()))
            df.columns = [str(c).strip().upper() for c in df.columns]
            col_fecha = 'DATE' if 'DATE' in df.columns else df.columns[0]
            df[col_fecha] = pd.to_datetime(df[col_fecha])
            df.set_index(col_fecha, inplace=True)
            val_col = [c for c in df.columns if c != col_fecha][0]
            df[name] = pd.to_numeric(df[val_col], errors='coerce')
            final_series = df[[name]]
            if name == 'uk_cpi': 
                final_series = final_series.shift(1)
            macro_frames.append(final_series)
            print(f"✅ {name} procesado correctamente.")
            
    return pd.concat(macro_frames, axis=1, sort=True)

# Ejecutamos la función
macro_data = get_macro_data()

# =================================================================
# Extraccion de Datos Financieros (Yahoo Finance API)
# =================================================================

print(f"\nIniciando ingesta masiva de {len(tickers)} activos...")
lista_frames = []

for symbol in tickers:
    try:
        print(f"Procesando: {symbol}          ", end="\r")
        tk = yf.Ticker(symbol)
        hist = tk.history(start=fecha_inicio, end=fecha_fin)
        
        if hist.empty:
            continue

        info = tk.info
        temp_df = pd.DataFrame({
            'fecha': hist.index,
            'ticker': symbol,
            'sector': info.get('sector', 'Unknown'),
            'adj_close': hist['Close'],
            'volume': hist['Volume'],
            'div_yield': info.get('dividendYield', 0),
            'aum': info.get('totalAssets', info.get('marketCap', 0))
        })
        lista_frames.append(temp_df)
    except Exception as e:
        print(f"\nError en {symbol}: {e}")

if not lista_frames:
    raise ValueError("❌ Error: No se pudo descargar datos de ningún ticker.")

tabla_1 = pd.concat(lista_frames)
print(f"\n✅ Ingesta financiera completada. Total filas: {len(tabla_1)}")

# =================================================================
# Unión, Sincronización (Forward Fill) y Persistencia (CSV + SQL)
# =================================================================

print("\nSincronizando con datos Macro y guardando resultados...")

macro_data.index = pd.to_datetime(macro_data.index).tz_localize(None).normalize()
macro_data.index.name = 'fecha'

tabla_1['fecha'] = pd.to_datetime(tabla_1['fecha'], utc=True).dt.tz_localize(None).dt.normalize()

final_df = tabla_1.merge(macro_data, on='fecha', how='left')
final_df.sort_values(['ticker', 'fecha'], inplace=True)

cols_macro = ['us_inflation_exp', 'uk_cpi']
final_df[cols_macro] = final_df.groupby('ticker')[cols_macro].ffill()
final_df[cols_macro] = final_df.groupby('ticker')[cols_macro].bfill()

final_df.dropna(subset=['us_inflation_exp'], inplace=True)

# --- GUARDADO CORREGIDO ---
# Definimos las rutas de salida basadas en la carpeta del script
ruta_csv_salida = os.path.join(directorio_script, 'tabla1_final.csv')
ruta_db_salida = os.path.join(directorio_script, 'investigacion_tfm.db')

try:
    # Guardar CSV en SGBD/Data
    final_df.to_csv(ruta_csv_salida, index=False)
    
    # Guardar SQLite en SGBD/Data
    conn = sqlite3.connect(ruta_db_salida)
    final_df.to_sql('tabla_1_ingesta', conn, if_exists='replace', index=False)
    conn.close()
    
    # RESUMEN FINAL PARA CONSOLA
    print(f"\n========================================================")
    print(f"✅ PROCESO FINALIZADO CON ÉXITO")
    print(f"📍 Archivos guardados en: {directorio_script}")
    print(f"📍 Registros finales: {len(final_df):,}")
    print(f"📍 Tickers únicos: {final_df['ticker'].nunique()}")
    
    uk_count = final_df[final_df['ticker'].str.contains('.L', na=False)]['ticker'].nunique()
    us_count = final_df['ticker'].nunique() - uk_count
    print(f"📍 Activos UK (FTSE): {uk_count}")
    print(f"📍 Activos USA/ETFs: {us_count}")
    print("========================================================\n")

except PermissionError:
    print("❌ ERROR: Cierra Excel o el visor de SQLite y vuelve a correr el script.")