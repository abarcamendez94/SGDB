# Pipeline de Ingesta y Optimización de Portafolio (S&P 500, FTSE 100 y ETFs)

Este repositorio contiene la implementación del modelo físico y la lógica de ingesta para un sistema de optimización de portafolio con horizontes de inversión de medio/largo plazo y rebalanceo mensual.

## 📂 Estructura del Proyecto

* **`/data`**: Contiene el motor de ingesta en Python (`tabla_1_ingesta.py`) y el archivo semilla de tickers (`tickers.csv`).
* **`/sql`**: Scripts DDL y DML para la limpieza de datos, auditoría y creación del universo final de inversión.
* **`/docs`**: Documentación técnica del modelado de datos y arquitectura (SQL + Parquet/Forward Fill).

## 🛠️ Requisitos Previos

Asegúrese de tener instalado Python 3.10+ y las siguientes librerías:
```bash
pip install yfinance pandas requests

## Diccionario de Tablas y Flujo de Datos

El pipeline procesa la información a través de tres estados lógicos, representados por las siguientes tablas en SQLite:

1. **`tabla_1_ingesta`**: Tabla primaria generada por el motor de Python. Contiene los datos crudos de todos los tickers descargados.
2. **`tabla_1_limpia`**: Capa intermedia creada mediante SQL donde se normalizan los sectores (limpieza de 'Unknown') y se unifican criterios regionales.
3. **`universo_final`**: Producto final del modelo físico. Contiene el subset optimizado de ~200 activos que cumplen con los filtros de liquidez (Volumen) y solvencia (AUM) para el modelo XGBoost.

### ⚠️ Nota Importante sobre Nomenclatura
Para que los scripts SQL funcionen correctamente, asegúrese de que la tabla inicial en SQLite tenga el nombre exacto: `tabla_1_ingesta`. 

* **Si usa el Script de Python:** El motor ya está programado para crear la tabla con este nombre automáticamente dentro de `investigacion_tfm.db`.
* **Si importa manualmente un CSV:** Al usar el asistente de importación de SQLite Browser, debe renombrar la tabla de destino a `tabla_1_ingesta` en el campo "Table name".

## 🚀 Instrucciones para levantar el Dataset en Local

Siga estos pasos en orden para reproducir el entorno de datos y generar la base de datos de investigación:

## Paso 1: Ingesta de Datos (Capa Bronze/Silver)
Este paso descarga los datos históricos desde Yahoo Finance y FRED, gestiona los valores nulos mediante *Forward Fill* y consolida la información macroeconómica.
1. Abra su terminal en la carpeta raíz del proyecto.
2. Ejecute el motor de ingesta:
   ```bash
   python data/tabla_1_ingesta.py
3. Resultado: Se creara el archivo data/investigacion_tfm.db (~135MB)

## Paso 2: Transformación y Calidad
Una vez generada la base de datos, se aplica la lógica de negocio y los filtros de calidad (AUM, Volumen y Supervivencia histórica) mediante SQL:
1. Conecte su gestor de base de datos (SQLite Browser, DBeaver o la extensión de VS Code) al archivo investigacion_tfm.db y el archivo tickers.csv
    1.1 Al abrir el archivo tickers.csv asegurarse de seleccionar y darle OK a "Column Names in Fist Line"
2. Ejecute el script de auditoría para verificar la integridad:
   -- Ejecutar contenido de:
sql/01_auditoria_datos.sql
3. Ejecute el script de creación del universo final de 200 activos:
-- Ejecutar contenido de:
sql/02_creacion_universo.sql

## Modelo Fisico (SFBD)
La implementación del modelo físico se basa en una arquitectura relacional sobre SQLite, diseñada para soportar el rebalanceo mensual de la cartera:
Entidades Principales: * tabla_1_ingesta: Datos crudos (Raw Data) con precios ajustados, volumen y macro (CPI/Inflation).

tabla_1_limpia: Capa de datos curados con sectores normalizados.

universo_final: Vista materializada de los activos que cumplen los criterios de inversión para el modelo XGBoost.

Restricciones: Se aplican filtros de supervivencia (mínimo 2,300 días de historia) para evitar el sesgo de supervivencia en el entrenamiento del modelo de Big Data.