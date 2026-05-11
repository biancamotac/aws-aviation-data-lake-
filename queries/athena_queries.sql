-- =====================================
-- QUERY 1 - Acidentes por Estado
-- =====================================

SELECT
    ocorrencia_uf,
    COUNT(*) AS total_acidentes
FROM acidentes_por_estado
GROUP BY ocorrencia_uf
ORDER BY total_acidentes DESC;


-- =====================================
-- QUERY 2 - Dados de 2018
-- =====================================

SELECT *
FROM aviation
WHERE ano = '2018';


-- =====================================
-- QUERY 3 - Evolução Temporal
-- =====================================

SELECT
    ano,
    COUNT(*) AS total_acidentes
FROM evolucao_temporal
GROUP BY ano
ORDER BY ano;


-- =====================================
-- QUERY 4 - Fatalidades por Fabricante
-- =====================================

SELECT
    aeronave_fabricante,
    fatalidades
FROM fatalidades_por_fabricante
ORDER BY fatalidades DESC;
