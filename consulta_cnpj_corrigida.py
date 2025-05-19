#!/usr/bin/env python3
# consulta_cnpj_corrigida.py - Versão com correções para os problemas identificados
import sqlite3
import argparse
import pandas as pd
import os
import re
import json
from difflib import SequenceMatcher

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

def similaridade(a, b):
    """Calcula a similaridade entre duas strings"""
    if not a or not b:
        return 0
    return SequenceMatcher(None, a, b).ratio()

def extrair_miolo_cpf(cpf):
    """Extrai o miolo do CPF (6 dígitos centrais)"""
    # Remove caracteres não numéricos
    cpf_limpo = ''.join(c for c in str(cpf) if c.isdigit())
    
    # Verifica se tem 11 dígitos
    if len(cpf_limpo) == 11:
        return cpf_limpo[3:9]  # Posições 4 a 9
    
    # Para CPFs com menos de 11 dígitos, tenta extrair 6 dígitos
    if len(cpf_limpo) >= 6:
        return cpf_limpo[:6]  # Retorna os primeiros 6 dígitos
    
    return cpf_limpo  # Retorna o que tiver

def mapear_situacao_cadastral(codigo):
    """Mapeamento detalhado das situações cadastrais"""
    mapeamento = {
        "1": "NULA",
        "2": "ATIVA",
        "3": "SUSPENSA",
        "4": "INAPTA",
        "8": "BAIXADA",
        # Códigos adicionais
        "01": "NULA",
        "02": "ATIVA",
        "03": "SUSPENSA",
        "04": "INAPTA",
        "08": "BAIXADA",
        "05": "CANCELADA", 
        "06": "IRREGULAR",
        "07": "LIQUIDAÇÃO EXTRAJUDICIAL"
    }
    
    # Converter para string para garantir compatibilidade
    codigo_str = str(codigo).strip() if codigo else ""
    
    # Tentar buscar no mapeamento
    return mapeamento.get(codigo_str, f"DESCONHECIDA ({codigo_str})")

def obter_nome_empresa(conn, cnpj_basico):
    """Função corrigida para obter o nome da empresa de forma mais confiável"""
    cursor = conn.cursor()
    nome_empresa = None
    
    # 1. Tentar na tabela k3241k03200y0d (identificada como de empresas)
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='k3241k03200y0d'")
        if cursor.fetchone():
            cursor.execute("SELECT col_1 FROM k3241k03200y0d WHERE col_0 = ? LIMIT 1", (cnpj_basico,))
            result = cursor.fetchone()
            if result and result[0]:
                nome_empresa = result[0]
                print(f"Nome encontrado em k3241k03200y0d: {nome_empresa}")
                return nome_empresa
    except Exception as e:
        print(f"Erro ao buscar em k3241k03200y0d: {e}")
    
    # 2. Tentar na tabela outros
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='outros'")
        if cursor.fetchone():
            cursor.execute("SELECT col_1 FROM outros WHERE col_0 = ? LIMIT 1", (cnpj_basico,))
            result = cursor.fetchone()
            if result and result[0]:
                nome_empresa = result[0]
                print(f"Nome encontrado em outros: {nome_empresa}")
                return nome_empresa
    except Exception as e:
        print(f"Erro ao buscar em outros: {e}")
    
    # 3. Tentar na tabela empresas
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='empresas'")
        if cursor.fetchone():
            cursor.execute("SELECT razao_social FROM empresas WHERE cnpj_basico = ? LIMIT 1", (cnpj_basico,))
            result = cursor.fetchone()
            if result and result[0]:
                nome_empresa = result[0]
                print(f"Nome encontrado em empresas: {nome_empresa}")
                return nome_empresa
    except Exception as e:
        print(f"Erro ao buscar em empresas: {e}")
    
    print("Nome da empresa não encontrado")
    return "NOME NÃO DISPONÍVEL"

def buscar_informacoes_empresa(conn, cnpj_basico, debug=False):
    """Busca informações detalhadas da empresa"""
    cursor = conn.cursor()
    empresas = []
    
    try:
        # 1. Buscar nome da empresa
        nome_empresa = obter_nome_empresa(conn, cnpj_basico)
        
        # 2. Buscar na tabela de estabelecimentos para dados de contato, situação, etc.
        cursor.execute("PRAGMA table_info(estabelecimentos)")
        colunas_info = cursor.fetchall()
        colunas_nomes = [col[1] for col in colunas_info]
        
        # Encontrar colunas relevantes
        col_situacao = "02" if "02" in colunas_nomes else "situacao_cadastral"
        col_cnae = "4723700" if "4723700" in colunas_nomes else "cnae_principal"
        col_rua = "rua" if "rua" in colunas_nomes else None
        col_numero = "nilso_braun" if "nilso_braun" in colunas_nomes else None
        col_bairro = "parque_das_palmeiras" if "parque_das_palmeiras" in colunas_nomes else "bairro"
        col_uf = "sc" if "sc" in colunas_nomes else "uf"
        
        # Construir consulta dinâmica
        query = f"""
        SELECT 
            cnpj_basico,
            [{col_situacao}] AS situacao_cadastral
        """
        
        if col_rua:
            query += f", [{col_rua}] as rua"
        if col_numero:
            query += f", [{col_numero}] as numero"
        if col_bairro:
            query += f", [{col_bairro}] as bairro"
        if col_uf:
            query += f", [{col_uf}] as uf"
        if col_cnae:
            query += f", [{col_cnae}] as cnae_principal"
        
        query += " FROM estabelecimentos WHERE cnpj_basico = ?"
        
        cursor.execute(query, (cnpj_basico,))
        estabelecimentos = cursor.fetchall()
        
        # Se não encontrou estabelecimentos
        if not estabelecimentos or len(estabelecimentos) == 0:
            print("Nenhum estabelecimento encontrado")
            
            # Retornar informações básicas mesmo sem estabelecimento
            return [{
                "cnpj_basico": cnpj_basico,
                "nome_empresa": nome_empresa,
                "situacao_cadastral": "DESCONHECIDA",
                "situacao_descricao": "DESCONHECIDA",
                "endereco": "ENDEREÇO NÃO DISPONÍVEL",
                "bairro": "",
                "uf": "",
                "cnae_principal": ""
            }]
        
        # Obter nomes das colunas da consulta
        colunas = [col[0] for col in cursor.description]
        
        # Processar cada estabelecimento
        for estab in estabelecimentos:
            # Mapear valores
            estab_dict = {}
            for i, col in enumerate(colunas):
                if i < len(estab):
                    estab_dict[col] = estab[i]
            
            # Construir endereço corretamente
            endereco = None
            if 'rua' in estab_dict and 'numero' in estab_dict:
                rua = estab_dict.get('rua')
                numero = estab_dict.get('numero')
                
                # Verificar se os valores são válidos
                if rua is not None and str(rua).lower() != 'none' and str(rua).strip():
                    endereco = str(rua)
                    if numero is not None and str(numero).lower() != 'none' and str(numero).strip():
                        endereco += f", {numero}"
            
            # Construir objeto de empresa
            empresa_dict = {
                "cnpj_basico": cnpj_basico,
                "nome_empresa": nome_empresa,
                "situacao_cadastral": estab_dict.get("situacao_cadastral"),
                "situacao_descricao": mapear_situacao_cadastral(estab_dict.get("situacao_cadastral")),
                "endereco": endereco if endereco else "ENDEREÇO NÃO DISPONÍVEL",
                "bairro": estab_dict.get("bairro", ""),
                "uf": estab_dict.get("uf", ""),
                "cnae_principal": estab_dict.get("cnae_principal", "")
            }
            
            empresas.append(empresa_dict)
    
    except Exception as e:
        print(f"Erro ao buscar informações da empresa: {e}")
        
        # Retornar informações mínimas em caso de erro
        empresas.append({
            "cnpj_basico": cnpj_basico,
            "nome_empresa": "ERRO AO BUSCAR NOME",
            "situacao_cadastral": "ERRO",
            "situacao_descricao": f"ERRO: {str(e)}",
            "endereco": "ENDEREÇO NÃO DISPONÍVEL",
            "bairro": "",
            "uf": "",
            "cnae_principal": ""
        })
    
    return empresas

def consulta_socio_direta(db_path, nome, cpf, limiar_similaridade=0.7, debug=False):
    """
    Consulta diretamente na tabela de sócios, sem depender da coluna cpf_miolo
    """
    print(f"Consultando sócio diretamente: {nome} (CPF: {cpf})")
    
    # Extrair miolo do CPF
    miolo_cpf = extrair_miolo_cpf(cpf)
    print(f"Miolo extraído: {miolo_cpf}")
    
    if not miolo_cpf or len(miolo_cpf) < 3:
        print("CPF inválido ou muito curto para extração do miolo")
        return {
            "nome": nome,
            "cpf": cpf,
            "miolo_cpf": miolo_cpf,
            "status": "CPF inválido",
            "score": 0,
            "empresas": []
        }
    
    # Conectar ao banco
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Verificar se já existem registros com cpf_miolo corrigido
        cursor.execute("""
        SELECT COUNT(*) FROM socios 
        WHERE LENGTH(cpf_miolo) = 6 AND cpf_miolo GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]'
        """)
        
        count_corretos = cursor.fetchone()[0]
        print(f"Encontrados {count_corretos} registros com cpf_miolo corrigido.")
        
        if count_corretos > 1000:
            print(f"Usando coluna cpf_miolo.")
            # Usar a coluna cpf_miolo para consulta
            cursor.execute("""
            SELECT * FROM socios
            WHERE cpf_miolo = ?
            LIMIT 100
            """, (miolo_cpf,))
        else:
            print("Poucos registros com cpf_miolo corrigido. Fazendo extração direta.")
            # Usar extração direta (mais lento)
            cursor.execute("""
            SELECT * FROM socios
            WHERE (
                -- Para CPFs mascarados (***XXXXXX**)
                ([***331355**] LIKE '***%' AND SUBSTR(REPLACE(REPLACE([***331355**], '.', ''), '-', ''), 4, 6) = ?)
                OR
                -- Para CPFs completos (11+ dígitos)
                (LENGTH(REPLACE(REPLACE([***331355**], '.', ''), '-', '')) >= 11 
                 AND SUBSTR(REPLACE(REPLACE([***331355**], '.', ''), '-', ''), 4, 6) = ?)
                OR
                -- Para CPFs parciais mas com 6+ dígitos
                (LENGTH(REPLACE(REPLACE([***331355**], '.', ''), '-', '')) >= 6
                 AND LENGTH(REPLACE(REPLACE([***331355**], '.', ''), '-', '')) < 11
                 AND SUBSTR(REPLACE(REPLACE([***331355**], '.', ''), '-', ''), 1, 6) = ?)
            )
            LIMIT 100
            """, (miolo_cpf, miolo_cpf, miolo_cpf))
        
        socios = cursor.fetchall()
        
        # Obter nomes das colunas
        colunas = [col[0] for col in cursor.description]
        
        # Se não encontrou resultados
        if not socios:
            print("Nenhum sócio encontrado.")
            return {
                "nome": nome,
                "cpf": cpf,
                "miolo_cpf": miolo_cpf,
                "status": "Não encontrado",
                "score": 0,
                "empresas": []
            }
        
        print(f"Encontrados {len(socios)} sócios com este miolo de CPF.")
        
        # Normalizar nome de entrada para comparação
        nome_normalizado = normalizar_nome(nome)
        
        # Identificar índice da coluna com nome do sócio
        idx_nome = None
        for i, col in enumerate(colunas):
            if col.lower() in ['livia_maria_andrade_ramos_gaertner', 'nome_socio']:
                idx_nome = i
                break
        
        if idx_nome is None:
            # Tentar pela terceira coluna (normalmente é o nome)
            idx_nome = 2 if len(colunas) > 2 else 0
        
        # Calcular similaridade para cada resultado
        resultados_com_score = []
        for socio in socios:
            # Converter para dicionário
            socio_dict = {}
            for i, col in enumerate(colunas):
                socio_dict[col] = socio[i]
            
            # Calcular similaridade do nome
            nome_socio = socio[idx_nome]
            nome_socio_norm = normalizar_nome(nome_socio) if nome_socio else ""
            score = similaridade(nome_normalizado, nome_socio_norm)
            
            socio_dict['score'] = score
            resultados_com_score.append(socio_dict)
        
        # Ordenar por similaridade
        resultados_com_score.sort(key=lambda x: x['score'], reverse=True)
        
        # Se encontrou resultado com score acima do limiar
        if resultados_com_score and resultados_com_score[0]['score'] >= limiar_similaridade:
            melhor_resultado = resultados_com_score[0]
            
            # Índice de coluna de CNPJ básico
            idx_cnpj = None
            for i, col in enumerate(colunas):
                if col.lower() in ['03769328', 'cnpj_basico']:
                    idx_cnpj = i
                    break
            
            if idx_cnpj is None:
                # Tentar pela primeira coluna (normalmente é o CNPJ básico)
                idx_cnpj = 0
            
            # Buscar empresas associadas ao CNPJ
            cnpj_basico = socio[idx_cnpj]
            empresas = buscar_informacoes_empresa(conn, cnpj_basico, debug)
            
            # Formatar resultado
            nome_col = colunas[idx_nome]
            cpf_col = colunas[3] if len(colunas) > 3 else None  # Normalmente é a quarta coluna
            
            resultado = {
                "nome": nome,
                "cpf": cpf,
                "miolo_cpf": miolo_cpf,
                "status": "Encontrado",
                "nome_encontrado": melhor_resultado.get(nome_col),
                "cpf_encontrado": melhor_resultado.get(cpf_col) if cpf_col else "Desconhecido",
                "score": melhor_resultado['score'],
                "empresas": empresas
            }
            
            print(f"Encontrado! Nome: {resultado['nome_encontrado']}, Score: {resultado['score']:.2f}")
            print(f"Empresas encontradas: {len(empresas)}")
            
            return resultado
        
        # Se encontrou resultados, mas nenhum com score adequado
        print(f"Nomes não correspondem. Melhor score: {resultados_com_score[0]['score']:.2f}")
        return {
            "nome": nome,
            "cpf": cpf,
            "miolo_cpf": miolo_cpf,
            "status": "Nome não corresponde",
            "score": resultados_com_score[0]['score'] if resultados_com_score else 0,
            "empresas": []
        }
    
    except Exception as e:
        print(f"Erro na consulta: {e}")
        return {
            "nome": nome,
            "cpf": cpf,
            "miolo_cpf": miolo_cpf,
            "status": f"Erro: {str(e)}",
            "score": 0,
            "empresas": []
        }
    
    finally:
        conn.close()

def verificar_cnpj_direto(db_path, cnpj, debug=False):
    """
    Verifica um CNPJ diretamente no banco
    """
    print(f"Verificando CNPJ: {cnpj}")
    
    # Limpar CNPJ - apenas dígitos
    cnpj_limpo = ''.join(c for c in str(cnpj) if c.isdigit())
    
    if len(cnpj_limpo) < 8:
        print("CNPJ inválido - precisa ter pelo menos 8 dígitos (CNPJ básico)")
        return {
            "cnpj": cnpj,
            "status": "CNPJ inválido",
            "socios": []
        }
    
    # Extrair CNPJ básico (8 primeiros dígitos)
    cnpj_basico = cnpj_limpo[:8]
    print(f"CNPJ básico: {cnpj_basico}")
    
    # Conectar ao banco
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Verificar se o CNPJ existe
        cursor.execute("""
        SELECT 
            cnpj_basico,
            [02] AS situacao_cadastral
        FROM estabelecimentos
        WHERE cnpj_basico = ?
        LIMIT 1
        """, (cnpj_basico,))
        
        estabelecimento = cursor.fetchone()
        
        if not estabelecimento:
            print("CNPJ não encontrado.")
            return {
                "cnpj": cnpj,
                "status": "Não encontrado",
                "socios": []
            }
        
        # Buscar informações da empresa
        empresas = buscar_informacoes_empresa(conn, cnpj_basico, debug)
        
        if not empresas:
            print("Informações da empresa não encontradas.")
            return {
                "cnpj": cnpj,
                "cnpj_basico": cnpj_basico,
                "nome_empresa": "NOME NÃO DISPONÍVEL",
                "status": "Não encontrado",
                "socios": []
            }
        
        # Usar a primeira empresa para informações gerais
        empresa = empresas[0]
        
        # Buscar sócios
        try:
            cursor.execute("""
            SELECT 
                [livia_maria_andrade_ramos_gaertner] AS nome_socio,
                [***331355**] AS cpf_cnpj_socio
            FROM socios
            WHERE [03769328] = ?
            """, (cnpj_basico,))
            
            socios = []
            for socio in cursor.fetchall():
                socios.append({
                    "nome": socio[0],
                    "cpf": socio[1]
                })
            
            print(f"Encontrados {len(socios)} sócios.")
        except Exception as e:
            print(f"Erro ao buscar sócios: {e}")
            socios = []
        
        # IMPORTANTE: Verificar se empresa está ativa antes de retornar
        situacao = empresa.get("situacao_cadastral", "")
        situacao_desc = empresa.get("situacao_descricao", "DESCONHECIDA")
        
        # Formatar resultado enfatizando situação e sócios (conforme feedback)
        resultado = {
            "cnpj": cnpj,
            "cnpj_basico": cnpj_basico,
            "nome_empresa": empresa.get("nome_empresa", "NOME NÃO DISPONÍVEL"),
            "situacao": situacao,
            "situacao_descricao": situacao_desc,
            "esta_ativa": situacao == "2" or situacao_desc == "ATIVA",
            "endereco": empresa.get("endereco", "ENDEREÇO NÃO DISPONÍVEL"),
            "bairro": empresa.get("bairro", ""),
            "uf": empresa.get("uf", ""),
            "cnae_principal": empresa.get("cnae_principal", ""),
            "socios": socios
        }
        
        return resultado
    
    except Exception as e:
        print(f"Erro na consulta: {e}")
        return {
            "cnpj": cnpj,
            "status": f"Erro: {str(e)}",
            "socios": []
        }
    
    finally:
        conn.close()

def processar_arquivo_socios(db_path, arquivo, limiar=0.7, debug=False):
    """Processa um arquivo com lista de sócios (nome e CPF)"""
    print(f"Processando arquivo de sócios: {arquivo}")
    
    if not os.path.exists(arquivo):
        print(f"Erro: Arquivo {arquivo} não encontrado")
        return []
    
    # Determinar tipo de arquivo pela extensão
    ext = os.path.splitext(arquivo)[1].lower()
    
    socios = []
    try:
        if ext == '.csv':
            # Tentar diferentes delimitadores
            for delim in [',', ';', '\t', '|']:
                try:
                    df = pd.read_csv(arquivo, delimiter=delim)
                    # Identificar colunas de nome e CPF
                    col_nome = None
                    col_cpf = None
                    
                    for col in df.columns:
                        if 'nome' in col.lower():
                            col_nome = col
                        elif 'cpf' in col.lower():
                            col_cpf = col
                    
                    # Se não encontrou por nome, usar primeiras colunas
                    if not col_nome and not col_cpf and len(df.columns) >= 2:
                        col_nome = df.columns[0]
                        col_cpf = df.columns[1]
                    
                    if col_nome and col_cpf:
                        for _, row in df.iterrows():
                            socios.append({
                                'nome': str(row[col_nome]),
                                'cpf': str(row[col_cpf])
                            })
                        break
                except:
                    continue
        elif ext == '.txt':
            # Assumir uma linha por sócio
            with open(arquivo, 'r') as f:
                linhas = f.readlines()
            
            for linha in linhas:
                linha = linha.strip()
                if not linha:
                    continue
                
                # Tentar separar nome e CPF
                partes = re.split(r'[;,\t]', linha)
                if len(partes) >= 2:
                    socios.append({
                        'nome': partes[0].strip(),
                        'cpf': partes[1].strip()
                    })
                else:
                    # Tentar extrair CPF da linha
                    match = re.search(r'(\d{3}\.?\d{3}\.?\d{3}-?\d{2}|\d{11})', linha)
                    if match:
                        cpf = match.group(1)
                        nome = linha[:match.start()].strip()
                        socios.append({
                            'nome': nome,
                            'cpf': cpf
                        })
    except Exception as e:
        print(f"Erro ao processar arquivo: {e}")
    
    if not socios:
        print("Nenhum sócio encontrado no arquivo")
        return []
    
    print(f"Encontrados {len(socios)} sócios no arquivo")
    
    # Consultar cada sócio
    resultados = []
    for i, socio in enumerate(socios):
        print(f"\nConsultando sócio {i+1}/{len(socios)}: {socio['nome']}")
        resultado = consulta_socio_direta(db_path, socio['nome'], socio['cpf'], limiar, debug)
        resultados.append(resultado)
    
    # Salvar resultados em JSON
    nome_saida = os.path.splitext(arquivo)[0] + "_resultados.json"
    with open(nome_saida, 'w', encoding='utf-8') as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)
    
    print(f"\nResultados salvos em {nome_saida}")
    
    # Gerar CSV resumido
    try:
        df_resumo = pd.DataFrame([
            {
                'nome': r['nome'],
                'cpf': r['cpf'],
                'status': r['status'],
                'score': r.get('score', 0),
                'nome_encontrado': r.get('nome_encontrado', ''),
                'qtd_empresas': len(r.get('empresas', [])),
                'empresas': ', '.join([str(e.get('nome_empresa', 'N/A')) for e in r.get('empresas', [])])
            }
            for r in resultados
        ])
        
        nome_csv = os.path.splitext(arquivo)[0] + "_resultados.csv"
        df_resumo.to_csv(nome_csv, index=False)
        print(f"Resumo salvo em {nome_csv}")
    except Exception as e:
        print(f"Erro ao gerar CSV resumido: {e}")
    
    return resultados

def processar_arquivo_cnpjs(db_path, arquivo, debug=False):
    """Processa um arquivo com lista de CNPJs"""
    print(f"Processando arquivo de CNPJs: {arquivo}")
    
    if not os.path.exists(arquivo):
        print(f"Erro: Arquivo {arquivo} não encontrado")
        return []
    
    # Determinar tipo de arquivo pela extensão
    ext = os.path.splitext(arquivo)[1].lower()
    
    cnpjs = []
    try:
        if ext == '.csv':
            # Tentar diferentes delimitadores
            for delim in [',', ';', '\t', '|']:
                try:
                    df = pd.read_csv(arquivo, delimiter=delim)
                    # Identificar coluna de CNPJ
                    col_cnpj = None
                    
                    for col in df.columns:
                        if 'cnpj' in col.lower():
                            col_cnpj = col
                            break
                    
                    # Se não encontrou, usar primeira coluna
                    if not col_cnpj and len(df.columns) >= 1:
                        col_cnpj = df.columns[0]
                    
                    if col_cnpj:
                        cnpjs = [str(x) for x in df[col_cnpj].tolist()]
                        break
                except:
                    continue
        elif ext == '.txt':
            # Assumir um CNPJ por linha
            with open(arquivo, 'r') as f:
                cnpjs = [linha.strip() for linha in f.readlines() if linha.strip()]
    except Exception as e:
        print(f"Erro ao processar arquivo: {e}")
    
    if not cnpjs:
        print("Nenhum CNPJ encontrado no arquivo")
        return []
    
    # Remover não dígitos e valores inválidos
    cnpjs = [''.join(c for c in cnpj if c.isdigit()) for cnpj in cnpjs]
    cnpjs = [cnpj for cnpj in cnpjs if len(cnpj) >= 8]
    
    print(f"Encontrados {len(cnpjs)} CNPJs válidos no arquivo")
    
    # Consultar cada CNPJ
    resultados = []
    for i, cnpj in enumerate(cnpjs):
        print(f"\nVerificando CNPJ {i+1}/{len(cnpjs)}: {cnpj}")
        resultado = verificar_cnpj_direto(db_path, cnpj, debug)
        resultados.append(resultado)
    
    # Salvar resultados em JSON
    nome_saida = os.path.splitext(arquivo)[0] + "_resultados.json"
    with open(nome_saida, 'w', encoding='utf-8') as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)
    
    print(f"\nResultados salvos em {nome_saida}")
    
    # Gerar CSV resumido com foco na situação da empresa e sócios
    try:
        df_resumo = pd.DataFrame([
            {
                'cnpj': r['cnpj'],
                'cnpj_basico': r.get('cnpj_basico', ''),
                'nome_empresa': str(r.get('nome_empresa', 'Não encontrado')),
                'situacao': r.get('situacao_descricao', r.get('status', '')),
                'esta_ativa': "SIM" if r.get('esta_ativa', False) else "NÃO",
                'qtd_socios': len(r.get('socios', [])),
                'socios': ', '.join([str(s.get('nome', 'N/A')) for s in r.get('socios', [])])
            }
            for r in resultados
        ])
        
        nome_csv = os.path.splitext(arquivo)[0] + "_resultados.csv"
        df_resumo.to_csv(nome_csv, index=False)
        print(f"Resumo salvo em {nome_csv}")
    except Exception as e:
        print(f"Erro ao gerar CSV resumido: {e}")
    
    return resultados

def download_arquivo(url, output_path, attempt=1, max_attempts=3):
    """
    Baixa um arquivo da URL especificada para o caminho de saída
    com barra de progresso e suporte a retentativas
    """
    import requests
    from tqdm import tqdm
    
    if os.path.exists(output_path):
        file_size = os.path.getsize(output_path)
        print(f"Arquivo já existe: {output_path} ({file_size/1024/1024:.1f} MB)")
        return True
    
    try:
        # Cria o diretório de saída se não existir
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Faz a requisição com streaming
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        # Obtém o tamanho total do arquivo
        total_size = int(response.headers.get('content-length', 0))
        
        # Baixa o arquivo com barra de progresso
        with open(output_path, 'wb') as f, tqdm(
                desc=os.path.basename(output_path),
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
            ) as bar:
            for chunk in response.iter_content(chunk_size=8192):
                size = f.write(chunk)
                bar.update(size)
        
        return True
        
    except requests.exceptions.RequestException as e:
        if attempt < max_attempts:
            print(f"Erro ao baixar {url}: {e}. Tentativa {attempt} de {max_attempts}")
            return download_arquivo(url, output_path, attempt + 1, max_attempts)
        else:
            print(f"Erro ao baixar {url} após {max_attempts} tentativas: {e}")
            return False
    except Exception as e:
        print(f"Erro inesperado ao baixar {url}: {e}")
        return False

def gerar_script_download_base():
    """Gera um script para download da base completa da Receita Federal"""
    script = """#!/usr/bin/env python3
# download_base_completa.py - Script para baixar a base completa da Receita Federal
import os
import requests
import sys
from tqdm import tqdm
import concurrent.futures
import argparse

def download_file(url, output_path, attempt=1, max_attempts=3):
    """
    Baixa um arquivo da URL especificada para o caminho de saída
    com barra de progresso e suporte a retentativas
    """
    if os.path.exists(output_path):
        file_size = os.path.getsize(output_path)
        print(f"Arquivo já existe: {output_path} ({file_size/1024/1024:.1f} MB)")
        return True
    
    try:
        # Cria o diretório de saída se não existir
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Faz a requisição com streaming
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        # Obtém o tamanho total do arquivo
        total_size = int(response.headers.get('content-length', 0))
        
        # Baixa o arquivo com barra de progresso
        with open(output_path, 'wb') as f, tqdm(
                desc=os.path.basename(output_path),
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
            ) as bar:
            for chunk in response.iter_content(chunk_size=8192):
                size = f.write(chunk)
                bar.update(size)
        
        return True
        
    except requests.exceptions.RequestException as e:
        if attempt < max_attempts:
            print(f"Erro ao baixar {url}: {e}. Tentativa {attempt} de {max_attempts}")
            return download_file(url, output_path, attempt + 1, max_attempts)
        else:
            print(f"Erro ao baixar {url} após {max_attempts} tentativas: {e}")
            return False
    except Exception as e:
        print(f"Erro inesperado ao baixar {url}: {e}")
        return False

def baixar_base_completa(output_dir, num_workers=3, socios_only=False):
    """
    Baixa a base completa da Receita Federal
    
    Args:
        output_dir (str): Diretório de saída
        num_workers (int): Número de workers para download paralelo
        socios_only (bool): Se True, baixa apenas os arquivos de sócios
    """
    # URL base da Receita Federal
    base_url = "https://dadosabertos.rfb.gov.br/CNPJ/"
    
    # Criar diretório de saída
    os.makedirs(output_dir, exist_ok=True)
    
    # Construir URLs para os arquivos
    arquivos = []
    
    # Arquivos de empresas (10 arquivos)
    if not socios_only:
        for i in range(10):
            arquivos.append({
                'url': f"{base_url}/Empresas{i}.zip",
                'output': os.path.join(output_dir, f"empresas{i}.zip")
            })
    
    # Arquivos de estabelecimentos (10 arquivos)
    if not socios_only:
        for i in range(10):
            arquivos.append({
                'url': f"{base_url}/Estabelecimentos{i}.zip",
                'output': os.path.join(output_dir, f"estabelecimentos{i}.zip")
            })
    
    # Arquivos de sócios (9 arquivos)
    for i in range(9):
        arquivos.append({
            'url': f"{base_url}/Socios{i}.zip",
            'output': os.path.join(output_dir, f"socios{i}.zip")
        })
    
    # Arquivos de tabelas auxiliares
    arquivos_aux = [
        {'url': f"{base_url}/Cnaes.zip", 'output': os.path.join(output_dir, "cnaes.zip")},
        {'url': f"{base_url}/Motivos.zip", 'output': os.path.join(output_dir, "motivos.zip")},
        {'url': f"{base_url}/Municipios.zip", 'output': os.path.join(output_dir, "municipios.zip")},
        {'url': f"{base_url}/Naturezas.zip", 'output': os.path.join(output_dir, "naturezas.zip")},
        {'url': f"{base_url}/Paises.zip", 'output': os.path.join(output_dir, "paises.zip")},
        {'url': f"{base_url}/Qualificacoes.zip", 'output': os.path.join(output_dir, "qualificacoes.zip")},
    ]
    
    if not socios_only:
        arquivos.extend(arquivos_aux)
    
    # Download paralelo
    print(f"Iniciando download de {len(arquivos)} arquivos com {num_workers} workers")
    sucessos = 0
    falhas = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {}
        
        for arquivo in arquivos:
            future = executor.submit(download_file, arquivo['url'], arquivo['output'])
            futures[future] = arquivo
        
        for future in concurrent.futures.as_completed(futures):
            arquivo = futures[future]
            try:
                if future.result():
                    sucessos += 1
                else:
                    falhas += 1
            except Exception as e:
                print(f"Erro ao baixar {arquivo['url']}: {e}")
                falhas += 1
    
    print(f"Download concluído: {sucessos} sucessos, {falhas} falhas")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download da base completa da Receita Federal")
    parser.add_argument("--dir", type=str, default="base_completa", help="Diretório de saída")
    parser.add_argument("--workers", type=int, default=3, help="Número de workers para download paralelo")
    parser.add_argument("--socios-only", action="store_true", help="Baixar apenas os arquivos de sócios")
    
    args = parser.parse_args()
    
    print(f"Iniciando download da base {'completa' if not args.socios_only else 'de sócios'}")
    baixar_base_completa(args.dir, args.workers, args.socios_only)
"""
    
    return script

def main():
    parser = argparse.ArgumentParser(description='Consulta de sócios/CNPJs no banco de dados CNPJ')
    
    subparsers = parser.add_subparsers(dest='comando', help='Comandos disponíveis')
    
    # Subcomando para consulta de sócio
    parser_socio = subparsers.add_parser('socio', help='Consultar sócio por nome e CPF')
    parser_socio.add_argument('--nome', type=str, help='Nome do sócio')
    parser_socio.add_argument('--cpf', type=str, help='CPF do sócio')
    parser_socio.add_argument('--arquivo', type=str, help='Arquivo com lista de sócios (CSV ou TXT)')
    parser_socio.add_argument('--limiar', type=float, default=0.7, help='Limiar de similaridade (0.0-1.0)')
    parser_socio.add_argument('--banco', type=str, default='cnpj_amostra.db', help='Caminho para o banco de dados')
    parser_socio.add_argument('--debug', action='store_true', help='Modo debug com informações detalhadas')
    
    # Subcomando para verificação de CNPJ
    parser_cnpj = subparsers.add_parser('cnpj', help='Verificar CNPJ e seus sócios')
    parser_cnpj.add_argument('--cnpj', type=str, help='CNPJ a verificar')
    parser_cnpj.add_argument('--arquivo', type=str, help='Arquivo com lista de CNPJs (CSV ou TXT)')
    parser_cnpj.add_argument('--banco', type=str, default='cnpj_amostra.db', help='Caminho para o banco de dados')
    parser_cnpj.add_argument('--debug', action='store_true', help='Modo debug com informações detalhadas')
    
    # Subcomando para gerar script de download
    parser_download = subparsers.add_parser('download', help='Gerar script para baixar base completa')
    parser_download.add_argument('--output', type=str, default='download_base_completa.py', 
                             help='Arquivo de saída para o script de download')
    
    args = parser.parse_args()
    
    if args.comando == 'socio':
        if args.arquivo:
            # Processar arquivo com múltiplos sócios
            processar_arquivo_socios(args.banco, args.arquivo, args.limiar, args.debug)
        elif args.nome and args.cpf:
            # Consultar um único sócio
            resultado = consulta_socio_direta(args.banco, args.nome, args.cpf, args.limiar, args.debug)
            
            print("\nRESULTADO DA CONSULTA:")
            print("-" * 60)
            for key, value in resultado.items():
                if key != 'empresas':
                    print(f"{key}: {value}")
            
            if resultado['empresas']:
                print("\nEMPRESAS:")
                for i, empresa in enumerate(resultado['empresas']):
                    print(f"\nEmpresa {i+1}:")
                    # Mostrar nome e situação primeiro
                    print(f"  Nome: {empresa.get('nome_empresa', 'Nome não encontrado')}")
                    print(f"  Situação: {empresa.get('situacao_descricao', 'Não informada')}")
                    
                    # Depois mostrar outros detalhes
                    for key, value in empresa.items():
                        if key not in ['nome_empresa', 'situacao_descricao']:
                            print(f"  {key}: {value}")
        else:
            parser_socio.print_help()
    
    elif args.comando == 'cnpj':
        if args.arquivo:
            # Processar arquivo com múltiplos CNPJs
            processar_arquivo_cnpjs(args.banco, args.arquivo, args.debug)
        elif args.cnpj:
            # Verificar um único CNPJ
            resultado = verificar_cnpj_direto(args.banco, args.cnpj, args.debug)
            
            print("\nRESULTADO DA CONSULTA:")
            print("-" * 60)
            
            # Mostrar informação de status primeiro (prioridade alta)
            if 'situacao_descricao' in resultado:
                print(f"Status: {resultado['situacao_descricao']}")
                esta_ativa = resultado.get('esta_ativa', False)
                print(f"Empresa ativa: {'SIM' if esta_ativa else 'NÃO'}")
            
            # Informações da empresa
            if 'nome_empresa' in resultado:
                print(f"Nome: {resultado['nome_empresa']}")
            
            # Mostrar outros campos
            print("\nDados adicionais:")
            for key, value in resultado.items():
                if key not in ['socios', 'nome_empresa', 'situacao_descricao', 'esta_ativa']:
                    print(f"  {key}: {value}")
            
            # Mostrar sócios (prioridade alta para esta consulta)
            if resultado.get('socios'):
                print("\nSÓCIOS:")
                for i, socio in enumerate(resultado['socios']):
                    print(f"  {i+1}. {socio.get('nome')} - CPF: {socio.get('cpf')}")
            else:
                print("\nNenhum sócio encontrado para este CNPJ.")
        else:
            parser_cnpj.print_help()
    
    elif args.comando == 'download':
        # Gerar script para download da base completa
        script = gerar_script_download_base()
        with open(args.output, 'w') as f:
            f.write(script)
        os.chmod(args.output, 0o755)  # Tornar executável
        print(f"Script de download gerado em {args.output}")
        print("Para baixar a base completa, execute:")
        print(f"python {args.output} --dir base_completa")
        print("Para baixar apenas os arquivos de sócios:")
        print(f"python {args.output} --dir base_socios --socios-only")
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main()