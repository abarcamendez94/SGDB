-- 1. VERIFICACIÓN DE INTEGRIDAD DE TICKERS
-- Tickers en CSV semilla no incluidos en la ingesta
SELECT Ticker FROM tickers
EXCEPT
SELECT ticker FROM tabla_1_ingesta;

-- Tickers en ingesta no incluidos en el CSV semilla
SELECT ticker FROM tabla_1_ingesta
EXCEPT
SELECT Ticker FROM tickers;

-- 2. IDENTIFICACIÓN DE SECTORES DESCONOCIDOS
SELECT ticker FROM tabla_1_ingesta
WHERE sector = 'Unknown'
GROUP BY ticker;