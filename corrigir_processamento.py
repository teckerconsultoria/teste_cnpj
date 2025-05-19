#!/usr/bin/env python3
# corrigir_processamento.py - Script para corrigir a estrutura do banco de dados
import os
import sqlite3
import re
import pandas as pd
from tqdm import tqdm
import time

def normalizar_nome(nome):
    """Normaliza o nome para comparação"""
    import unicodedata
    import re
    
    if not isinstance(nome, str):
        return ""
    
    # Remover acentos
    nome = unicodedata.normalize('NFKD', nome)
    nome = nome.encode('ascii', errors='ignore').decode('ascii')
    
    # Converter para maiúsculas
    nome = nome.upper()
    
    # Remover caracteres especiais e números
    nome = re.sub(r'[^A-Z ]', '', nome)
    
    # Remover espaços extras
    nome = ' '.join(nome.split())
    
    return nome

def corrigir_banco_dados(db_path):
    """
    Corrige a estrutura do banco de dados:
    - Adiciona a coluna cpf_miolo à tabela de sócios
    - Extrai o miolo do CPF para cada registro
    - Cria índices otimizados
    - Cria visões SQL para consultas
    """
    print(f"Iniciando correção do banco de dados: {db_path}")
    inicio = time.time()
    
    # Verificar se o banco existe
    if not os.path.exists(db_path):
        print(f"Erro: Banco de dados {db_path} não encontrado.")
        return False
    
    # Conectar ao banco
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Verificar tabelas existentes
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tabelas = [t[0] for t in cursor.fetchall()]
    print(f"Tabelas encontradas: {tabelas}")
    
    # Identificar a tabela de sócios
    tabela_socios = None
    for tabela in tabelas:
        if tabela.lower() == 'socios':
            tabela_socios = tabela
            break
    
    # Se não encontrar explicitamente 'socios', procurar por nome similar
    if not tabela_socios:
        for tabela in tabelas:
            if 'soc' in tabela.lower():
                tabela_socios = tabela
                print(f"Tabela de sócios identificada como: {tabela_socios}")
                break
    
    # Última tentativa - verificar se é a tabela do arquivo
    if not tabela_socios:
        for tabela in tabelas:
            if 'k3241' in tabela.lower():
                tabela_socios = tabela
                print(f"Tabela de sócios provavelmente é: {tabela_socios}")
                break
    
    if not tabela_socios:
        print("Erro: Não foi possível identificar a tabela de sócios.")
        conn.close()
        return False
    
    # Verificar se a tabela de sócios já tem a coluna cpf_miolo
    cursor.execute(f"PRAGMA table_info({tabela_socios})")
    colunas = {col[1].lower(): col[0] for col in cursor.fetchall()}
    
    # Verificar se a coluna cpf_miolo já existe
    if 'cpf_miolo' in colunas:
        print("Coluna cpf_miolo já existe na tabela de sócios.")
    else:
        print(f"Adicionando coluna cpf_miolo à tabela {tabela_socios}...")
        try:
            # Adicionar a coluna cpf_miolo
            cursor.execute(f"ALTER TABLE {tabela_socios} ADD COLUMN cpf_miolo TEXT")
            conn.commit()
            print("Coluna adicionada com sucesso.")
        except Exception as e:
            print(f"Erro ao adicionar coluna: {e}")
            conn.close()
            return False
    
    # Identificar a coluna que contém o CPF/CNPJ
    coluna_cpf = None
    for col_nome in colunas:
        if 'cpf' in col_nome or 'cnpj_socio' in col_nome:
            coluna_cpf = col_nome
            break
    
    # Tentar identificar pelo número da coluna se for no formato antigo
    if not coluna_cpf:
        # Tentar identificar a coluna 3 (índice 2) que geralmente contém o CPF
        cursor.execute(f"SELECT * FROM {tabela_socios} LIMIT 1")
        nomes_colunas = [description[0] for description in cursor.description]
        
        for i, col in enumerate(nomes_colunas):
            if i == 2 or i == 3:  # Posições comuns para CPF/CNPJ
                coluna_cpf = col
                print(f"Coluna CPF/CNPJ identificada pelo índice: {coluna_cpf}")
                break
    
    if not coluna_cpf:
        print("Não foi possível identificar a coluna com o CPF/CNPJ. Tentando inferir...")
        # Verificar todas as colunas
        cursor.execute(f"SELECT * FROM {tabela_socios} LIMIT 1000")
        resultados = cursor.fetchall()
        nomes_colunas = [description[0] for description in cursor.description]
        
        # Tentar identificar qual coluna tem o padrão de CPF (digitos ou formato mascarado)
        for i, col in enumerate(nomes_colunas):
            # Analisar valores para ver se parecem CPF
            valores = [row[i] for row in resultados if row[i]]
            if not valores:
                continue
            
            # Verificar se algum valor tem formato de CPF (***XXX.XXX-**)
            for valor in valores:
                valor_str = str(valor)
                if '***' in valor_str or (len(valor_str) >= 6 and valor_str.replace('.', '').replace('-', '').isdigit()):
                    coluna_cpf = col
                    print(f"Coluna CPF/CNPJ inferida: {coluna_cpf}")
                    break
            
            if coluna_cpf:
                break
    
    if not coluna_cpf:
        # Última tentativa: usar a coluna específica do arquivo processado
        tentativas = ['col_3', '3', 'cpf_cnpj_socio', 'unnamed:_3']
        for tentativa in tentativas:
            if tentativa.lower() in colunas:
                coluna_cpf = tentativa
                print(f"Usando coluna alternativa para CPF: {coluna_cpf}")
                break
    
    if not coluna_cpf:
        print("Erro: Não foi possível identificar a coluna que contém o CPF.")
        conn.close()
        return False
    
    # Extrair miolo do CPF
    print(f"Extraindo miolo do CPF da coluna {coluna_cpf}...")
    
    # Verificar amostra da coluna CPF para identificar o formato
    cursor.execute(f"SELECT {coluna_cpf} FROM {tabela_socios} LIMIT 10")
    amostras = [str(r[0]) for r in cursor.fetchall() if r[0]]
    
    # Descobrir o formato dos CPFs
    formato_mascara = any(['***' in amostra for amostra in amostras])
    
    try:
        # Atualizar a coluna cpf_miolo
        if formato_mascara:
            # Para CPFs mascarados como ***XXX.XXX-**
            print("Detectado formato de CPF mascarado.")
            cursor.execute(f"""
            UPDATE {tabela_socios}
            SET cpf_miolo = (
                CASE 
                    WHEN {coluna_cpf} LIKE '***%' 
                    THEN SUBSTR(REPLACE(REPLACE({coluna_cpf}, '.', ''), '-', ''), 4, 6)
                    ELSE ''
                END
            )
            """)
        else:
            # Para CPFs numéricos, extrair posições 4-9 (índices 3-8)
            print("Extraindo dígitos 4-9 do CPF...")
            cursor.execute(f"""
            UPDATE {tabela_socios}
            SET cpf_miolo = (
                CASE 
                    WHEN LENGTH(REPLACE(REPLACE(REPLACE({coluna_cpf}, '.', ''), '-', ''), ' ', '')) >= 11 
                    THEN SUBSTR(REPLACE(REPLACE(REPLACE({coluna_cpf}, '.', ''), '-', ''), ' ', ''), 4, 6)
                    WHEN LENGTH(REPLACE(REPLACE(REPLACE({coluna_cpf}, '.', ''), '-', ''), ' ', '')) >= 6 
                    THEN SUBSTR(REPLACE(REPLACE(REPLACE({coluna_cpf}, '.', ''), '-', ''), ' ', ''), 1, 6)
                    ELSE ''
                END
            )
            """)
        
        conn.commit()
        print("Miolo do CPF extraído com sucesso!")
    except Exception as e:
        print(f"Erro ao extrair miolo do CPF: {e}")
        conn.close()
        return False
    
    # Criar índice para a coluna cpf_miolo
    print("Criando índice para cpf_miolo...")
    try:
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{tabela_socios}_cpf_miolo ON {tabela_socios}(cpf_miolo)")
        conn.commit()
        print("Índice criado com sucesso!")
    except Exception as e:
        print(f"Erro ao criar índice: {e}")
    
    # Identificar tabelas de empresas e estabelecimentos
    tabela_empresas = None
    tabela_estabelecimentos = None
    
    # Procurar tabela de empresas
    for tabela in tabelas:
        if tabela.lower() == 'empresas':
            tabela_empresas = tabela
            break
    
    if not tabela_empresas:
        for tabela in tabelas:
            if 'empre' in tabela.lower():
                tabela_empresas = tabela
                print(f"Tabela de empresas identificada como: {tabela_empresas}")
                break
    
    # Procurar tabela de estabelecimentos
    for tabela in tabelas:
        if tabela.lower() == 'estabelecimentos':
            tabela_estabelecimentos = tabela
            break
    
    if not tabela_estabelecimentos:
        for tabela in tabelas:
            if 'estab' in tabela.lower():
                tabela_estabelecimentos = tabela
                print(f"Tabela de estabelecimentos identificada como: {tabela_estabelecimentos}")
                break
    
    # Tentar criar visões para consultas
    print("\nCriando visões para facilitar consultas...")
    
    # Verificar quais tabelas estão disponíveis para visões
    tabelas_disponiveis = []
    if tabela_socios:
        tabelas_disponiveis.append("socios")
    if tabela_empresas:
        tabelas_disponiveis.append("empresas")
    if tabela_estabelecimentos:
        tabelas_disponiveis.append("estabelecimentos")
    
    if "socios" in tabelas_disponiveis:
        # Criar visão só com a tabela de sócios (mínimo necessário)
        try:
            cursor.execute(f"""
            CREATE VIEW IF NOT EXISTS vw_socios AS
            SELECT 
                cpf_miolo,
                {coluna_cpf} AS cpf_cnpj_socio,
                * 
            FROM {tabela_socios}
            """)
            print("  Visão criada: vw_socios")
        except Exception as e:
            print(f"  Erro ao criar visão vw_socios: {e}")
    
    # Criar visões mais completas se tiver as tabelas relacionadas
    if all(t in tabelas_disponiveis for t in ["socios", "empresas"]):
        try:
            # Identificar coluna de CNPJ básico em socios
            cursor.execute(f"PRAGMA table_info({tabela_socios})")
            colunas_socios = {col[1].lower(): col[0] for col in cursor.fetchall()}
            
            coluna_cnpj_socios = None
            for col_nome in colunas_socios:
                if 'cnpj' in col_nome and 'socio' not in col_nome:
                    coluna_cnpj_socios = col_nome
                    break
            
            if not coluna_cnpj_socios:
                # Tentar pela primeira coluna (geralmente CNPJ básico)
                cursor.execute(f"SELECT * FROM {tabela_socios} LIMIT 1")
                coluna_cnpj_socios = cursor.description[0][0]
                print(f"Coluna CNPJ em sócios inferida: {coluna_cnpj_socios}")
            
            # Identificar coluna de CNPJ básico em empresas
            cursor.execute(f"PRAGMA table_info({tabela_empresas})")
            colunas_empresas = {col[1].lower(): col[0] for col in cursor.fetchall()}
            
            coluna_cnpj_empresas = None
            for col_nome in colunas_empresas:
                if 'cnpj' in col_nome:
                    coluna_cnpj_empresas = col_nome
                    break
            
            if not coluna_cnpj_empresas:
                # Tentar pela primeira coluna (geralmente CNPJ básico)
                cursor.execute(f"SELECT * FROM {tabela_empresas} LIMIT 1")
                coluna_cnpj_empresas = cursor.description[0][0]
                print(f"Coluna CNPJ em empresas inferida: {coluna_cnpj_empresas}")
            
            # Criar visão socios_empresas
            cursor.execute(f"""
            CREATE VIEW IF NOT EXISTS vw_socios_empresas AS
            SELECT 
                s.cpf_miolo,
                s.{coluna_cpf} AS cpf_cnpj_socio,
                s.{coluna_cnpj_socios} AS cnpj_basico,
                e.*
            FROM {tabela_socios} s
            JOIN {tabela_empresas} e ON s.{coluna_cnpj_socios} = e.{coluna_cnpj_empresas}
            """)
            print("  Visão criada: vw_socios_empresas")
        except Exception as e:
            print(f"  Erro ao criar visão vw_socios_empresas: {e}")
    
    # Visão completa com as três tabelas
    if all(t in tabelas_disponiveis for t in ["socios", "empresas", "estabelecimentos"]):
        try:
            # Identificar coluna CNPJ em estabelecimentos
            cursor.execute(f"PRAGMA table_info({tabela_estabelecimentos})")
            colunas_estab = {col[1].lower(): col[0] for col in cursor.fetchall()}
            
            coluna_cnpj_estab = None
            for col_nome in colunas_estab:
                if 'cnpj' in col_nome and 'basico' in col_nome:
                    coluna_cnpj_estab = col_nome
                    break
            
            if not coluna_cnpj_estab:
                # Tentar pela primeira coluna (geralmente CNPJ básico)
                cursor.execute(f"SELECT * FROM {tabela_estabelecimentos} LIMIT 1")
                coluna_cnpj_estab = cursor.description[0][0]
                print(f"Coluna CNPJ em estabelecimentos inferida: {coluna_cnpj_estab}")
            
            # Criar visão socios_completo
            cursor.execute(f"""
            CREATE VIEW IF NOT EXISTS vw_socios_completo AS
            SELECT 
                s.cpf_miolo,
                s.{coluna_cpf} AS cpf_cnpj_socio,
                s.{coluna_cnpj_socios} AS cnpj_basico,
                e.*,
                est.*
            FROM {tabela_socios} s
            JOIN {tabela_empresas} e ON s.{coluna_cnpj_socios} = e.{coluna_cnpj_empresas}
            JOIN {tabela_estabelecimentos} est ON s.{coluna_cnpj_socios} = est.{coluna_cnpj_estab}
            """)
            print("  Visão criada: vw_socios_completo")
        except Exception as e:
            print(f"  Erro ao criar visão vw_socios_completo: {e}")
    
    # Verificar as visões criadas
    cursor.execute("SELECT name FROM sqlite_master WHERE type='view'")
    visoes = [v[0] for v in cursor.fetchall()]
    print(f"\nVisões disponíveis após correção: {visoes}")
    
    conn.commit()
    conn.close()
    
    fim = time.time()
    print(f"\nCorreção concluída em {(fim - inicio):.2f} segundos")
    return True

if __name__ == "__main__":
    # Caminho do banco de dados
    db_path = "cnpj_amostra.db"
    
    # Executar correção
    if corrigir_banco_dados(db_path):
        print("\nBanco de dados corrigido com sucesso! O sistema está pronto para execução dos testes de desempenho.")
    else:
        print("\nOcorreram erros durante a correção. Verifique as mensagens acima.")
