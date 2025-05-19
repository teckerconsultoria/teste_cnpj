-- correcao.sql - Script para corrigir problemas no banco CNPJ
-- Execute: sqlite3 cnpj_amostra.db < correcao.sql

-- Criando visão otimizada para consultas por CPF
DROP VIEW IF EXISTS vw_socios_otimizada;
CREATE VIEW vw_socios_otimizada AS
SELECT
    cpf_miolo,
    "livia_maria_andrade_ramos_gaertner" AS nome_socio,
    "***331355**" AS cpf_cnpj_socio,
    "03769328" AS cnpj_basico
FROM socios;

-- Preenchendo a coluna cpf_miolo corretamente
UPDATE socios
SET cpf_miolo = (
    CASE
        WHEN "***331355**" LIKE '***%' THEN SUBSTR(REPLACE(REPLACE("***331355**", '.', ''), '-', ''), 4, 6)
        WHEN LENGTH(REPLACE(REPLACE("***331355**", '.', ''), '-', '')) >= 11 THEN SUBSTR(REPLACE(REPLACE("***331355**", '.', ''), '-', ''), 4, 6)
        ELSE ''
    END
);

-- Criando índice para buscas por miolo de CPF
CREATE INDEX IF NOT EXISTS idx_socios_cpf_miolo ON socios(cpf_miolo);

-- Criando visão para consultas por CNPJ (nova demanda)
DROP VIEW IF EXISTS vw_cnpj_socios;
CREATE VIEW vw_cnpj_socios AS
SELECT
    e.cnpj_basico,
    e."0001" || e."57" AS cnpj_complemento,
    e.cnpj_basico || e."0001" || e."57" AS cnpj_completo,
    e."02" AS situacao_cadastral,
    s."livia_maria_andrade_ramos_gaertner" AS nome_socio,
    s."***331355**" AS cpf_cnpj_socio,
    s.cpf_miolo
FROM estabelecimentos e
LEFT JOIN socios s ON e.cnpj_basico = s."03769328";

-- Criando visão para verificar status de CNPJs (nova demanda)
DROP VIEW IF EXISTS vw_cnpj_status;
CREATE VIEW vw_cnpj_status AS
SELECT
    cnpj_basico,
    "0001" || "57" AS cnpj_complemento,
    cnpj_basico || "0001" || "57" AS cnpj_completo,
    "02" AS situacao_cadastral,
    CASE "02"
        WHEN 1 THEN 'NULA'
        WHEN 2 THEN 'ATIVA'
        WHEN 3 THEN 'SUSPENSA'
        WHEN 4 THEN 'INAPTA'
        WHEN 8 THEN 'BAIXADA'
        ELSE 'DESCONHECIDA'
    END AS situacao_descricao,
    "20210713" AS data_situacao,
    "4723700" AS cnae_principal,
    "rua" || ' ' || "nilso_braun" || ', ' || "s/n" AS endereco,
    "parque_das_palmeiras" AS bairro,
    "89803604" AS cep,
    "sc" AS uf
FROM estabelecimentos;

-- Verificar resultados
SELECT * FROM vw_socios_otimizada WHERE cpf_miolo != '' LIMIT 5;
