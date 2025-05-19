#!/usr/bin/env python3
# consulta_direta.py - Script para consulta direta de sócios/CNPJs sem correção completa
import sqlite3
import argparse
import pandas as pd
import os
import re
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

def consulta_socio_direta(db_path, nome, cpf, limiar_similaridade=0.7):
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
        # Buscar sócios pelo CPF original usando SUBSTR
        print("Executando consulta...")
        
        # Verificar se já existem registros com cpf_miolo corrigido
        cursor.execute("""
        SELECT COUNT(*) FROM socios 
        WHERE LENGTH(cpf_miolo) = 6 AND cpf_miolo GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]'
        """)
        
        count_corretos = cursor.fetchone()[0]
        
        if count_corretos > 1000:
            print(f"Encontrados {count_corretos} registros com cpf_miolo corrigido. Usando coluna cpf_miolo.")
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
            empresas = []
            cnpj_basico = socio[idx_cnpj]
            
            # Verificar se existe a tabela de estabelecimentos
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='estabelecimentos'")
            tem_tabela_estab = cursor.fetchone() is not None
            
            if tem_tabela_estab:
                # Buscar informações da empresa
                try:
                    cursor.execute("""
                    SELECT 
                        cnpj_basico,
                        [4723700] AS cnae_principal,
                        [rua] || ' ' || [nilso_braun] AS endereco,
                        [02] AS situacao_cadastral
                    FROM estabelecimentos
                    WHERE cnpj_basico = ?
                    """, (cnpj_basico,))
                    
                    empresas_db = cursor.fetchall()
                    colunas_empresa = [col[0] for col in cursor.description]
                    
                    for empresa in empresas_db:
                        empresa_dict = {}
                        for i, col in enumerate(colunas_empresa):
                            empresa_dict[col] = empresa[i]
                        
                        # Mapear situação cadastral
                        situacao = empresa_dict.get("situacao_cadastral", "")
                        if situacao == "2":
                            situacao_descr = "ATIVA"
                        elif situacao == "3":
                            situacao_descr = "SUSPENSA"
                        elif situacao == "4":
                            situacao_descr = "INAPTA"
                        elif situacao == "8":
                            situacao_descr = "BAIXADA"
                        else:
                            situacao_descr = "DESCONHECIDA"
                        
                        empresa_dict["situacao_descricao"] = situacao_descr
                        empresas.append(empresa_dict)
                except Exception as e:
                    print(f"Erro ao buscar empresas: {e}")
            
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

def verificar_cnpj_direto(db_path, cnpj):
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
        
        # Mapear situação cadastral
        situacao = estabelecimento[1]
        if situacao == "2":
            situacao_descr = "ATIVA"
        elif situacao == "3":
            situacao_descr = "SUSPENSA"
        elif situacao == "4":
            situacao_descr = "INAPTA"
        elif situacao == "8":
            situacao_descr = "BAIXADA"
        else:
            situacao_descr = "DESCONHECIDA"
        
        print(f"CNPJ encontrado. Situação: {situacao_descr}")
        
        # Buscar sócios
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
        
        return {
            "cnpj": cnpj,
            "cnpj_basico": cnpj_basico,
            "situacao": situacao,
            "situacao_descricao": situacao_descr,
            "socios": socios
        }
    
    except Exception as e:
        print(f"Erro na consulta: {e}")
        return {
            "cnpj": cnpj,
            "status": f"Erro: {str(e)}",
            "socios": []
        }
    
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description='Consulta direta de sócios/CNPJs sem correção completa')
    
    subparsers = parser.add_subparsers(dest='comando', help='Comandos disponíveis')
    
    # Subcomando para consulta de sócio
    parser_socio = subparsers.add_parser('socio', help='Consultar sócio por nome e CPF')
    parser_socio.add_argument('--nome', type=str, required=True, help='Nome do sócio')
    parser_socio.add_argument('--cpf', type=str, required=True, help='CPF do sócio')
    parser_socio.add_argument('--limiar', type=float, default=0.7, help='Limiar de similaridade (0.0-1.0)')
    parser_socio.add_argument('--banco', type=str, default='cnpj_amostra.db', help='Caminho para o banco de dados')
    
    # Subcomando para verificação de CNPJ
    parser_cnpj = subparsers.add_parser('cnpj', help='Verificar CNPJ e seus sócios')
    parser_cnpj.add_argument('--cnpj', type=str, required=True, help='CNPJ a verificar')
    parser_cnpj.add_argument('--banco', type=str, default='cnpj_amostra.db', help='Caminho para o banco de dados')
    
    args = parser.parse_args()
    
    if args.comando == 'socio':
        resultado = consulta_socio_direta(args.banco, args.nome, args.cpf, args.limiar)
        
        print("\nRESULTADO DA CONSULTA:")
        print("-" * 60)
        for key, value in resultado.items():
            if key != 'empresas':
                print(f"{key}: {value}")
        
        if resultado['empresas']:
            print("\nEMPRESAS:")
            for i, empresa in enumerate(resultado['empresas']):
                print(f"\nEmpresa {i+1}:")
                for key, value in empresa.items():
                    print(f"  {key}: {value}")
    
    elif args.comando == 'cnpj':
        resultado = verificar_cnpj_direto(args.banco, args.cnpj)
        
        print("\nRESULTADO DA CONSULTA:")
        print("-" * 60)
        for key, value in resultado.items():
            if key != 'socios':
                print(f"{key}: {value}")
        
        if resultado.get('socios'):
            print("\nSÓCIOS:")
            for i, socio in enumerate(resultado['socios']):
                print(f"\nSócio {i+1}:")
                for key, value in socio.items():
                    print(f"  {key}: {value}")
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
