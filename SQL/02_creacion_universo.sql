-- 3. LIMPIEZA DE SECTORES Y CREACIÓN DE TABLA_1_LIMPIA
CREATE TABLE IF NOT EXISTS tabla_1_limpia AS
SELECT
    fecha,
    ticker,
    CASE
        WHEN ticker IN ('AGG', 'BND', 'IEI', 'SHY', 'TLT', 'LQD', 'HYG', 'SPY', 'IVV', 
                        'VTI', 'IWM', 'VEA', 'EFA', 'EEM', 'VWO', 'XLK', 'VNQ', 'GLD', 'IAU', 'DBC') 
             THEN 'ETF'
        WHEN ticker IN ('FISV', 'ALW.L', 'FCIT.L', 'III.L', 'PSH.L', 'SMT.L') 
             THEN 'Financial Services'
        WHEN ticker = 'PCT.L' 
             THEN 'Technology'
        ELSE sector 
    END AS sector,
    adj_close, volume, div_yield, aum, us_inflation_exp, uk_cpi
FROM tabla_1_ingesta;

-- 4. MÉTRICAS DE CONTROL Y FILTROS DE CALIDAD
WITH metricas_por_ticker AS (
    SELECT
        ticker, sector,
        MAX(aum) as max_aum,
        AVG(volume) as avg_vol,
        COUNT(*) as total_dias
    FROM tabla_1_limpia
    GROUP BY ticker, sector
)
SELECT
    CASE 
        WHEN sector = 'ETF' THEN 'ETFs (Protegidos)'
        WHEN ticker LIKE '%.L' THEN 'FTSE 100 (UK)'
        ELSE 'S&P 500 (USA)'
    END AS grupo,
    COUNT(*) AS total_en_db,
    SUM(CASE 
        WHEN sector = 'ETF' AND total_dias >= 2300 THEN 1
        WHEN (ticker NOT LIKE '%.L' AND sector != 'ETF' AND max_aum >= 15000000000 AND avg_vol >= 250000 AND total_dias >= 2300) THEN 1
        WHEN (ticker LIKE '%.L' AND max_aum >= 3500000000 AND avg_vol >= 250000 AND total_dias >= 2300) THEN 1
        ELSE 0 
    END) AS pasan_filtro_calidad
FROM metricas_por_ticker
GROUP BY 1;

-- 5. CREACIÓN DEL UNIVERSO FINAL (Muestra reducida a mejores activos)
DROP TABLE IF EXISTS universo_final;
CREATE TABLE universo_final AS
WITH auditoria_calidad AS (
    SELECT ticker, AVG(volume) as avg_vol_historico, COUNT(*) as total_dias
    FROM tabla_1_limpia GROUP BY ticker
),
filtrados_calidad AS (
    SELECT t.* FROM tabla_1_limpia t
    JOIN auditoria_calidad l ON t.ticker = l.ticker
    WHERE (t.sector = 'ETF' AND l.total_dias >= 2300)
       OR (t.ticker NOT LIKE '%.L' AND t.sector != 'ETF' AND t.aum >= 15000000000 AND l.avg_vol_historico >= 250000 AND l.total_dias >= 2300)
       OR (t.ticker LIKE '%.L' AND t.aum >= 3500000000 AND l.avg_vol_historico >= 250000 AND l.total_dias >= 2300)
),
ranking_universo AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY sector, (ticker LIKE '%.L') ORDER BY aum DESC) as pos_sectorial
    FROM filtrados_calidad
)
SELECT * FROM ranking_universo
WHERE (ticker NOT LIKE '%.L' AND sector != 'ETF' AND (
    (sector = 'Technology' AND pos_sectorial <= 38) OR (sector = 'Financial Services' AND pos_sectorial <= 17) OR (sector = 'Healthcare' AND pos_sectorial <= 16) OR (sector = 'Consumer Cyclical' AND pos_sectorial <= 14) OR (sector = 'Industrials' AND pos_sectorial <= 11) OR (sector = 'Communication Services' AND pos_sectorial <= 10) OR (sector = 'Consumer Defensive' AND pos_sectorial <= 8) OR (sector = 'Energy' AND pos_sectorial <= 6) OR (sector = 'Real Estate' AND pos_sectorial <= 4) OR (sector = 'Utilities' AND pos_sectorial <= 3) OR (sector = 'Basic Materials' AND pos_sectorial <= 3)
))
UNION ALL
SELECT * FROM ranking_universo
WHERE (ticker LIKE '%.L' AND sector != 'ETF' AND (
    (sector = 'Financial Services' AND pos_sectorial <= 10) OR (sector = 'Consumer Defensive' AND pos_sectorial <= 9) OR (sector = 'Healthcare' AND pos_sectorial <= 6) OR (sector = 'Energy' AND pos_sectorial <= 6) OR (sector = 'Basic Materials' AND pos_sectorial <= 6) OR (sector = 'Industrials' AND pos_sectorial <= 5) OR (sector = 'Consumer Cyclical' AND pos_sectorial <= 3) OR (sector = 'Communication Services' AND pos_sectorial <= 2) OR (sector = 'Utilities' AND pos_sectorial <= 2) OR (sector = 'Real Estate' AND pos_sectorial <= 1)
))
UNION ALL
SELECT * FROM ranking_universo
WHERE sector = 'ETF' AND pos_sectorial <= 20;