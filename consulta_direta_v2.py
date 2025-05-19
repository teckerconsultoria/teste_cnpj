#!/usr/bin/env python3
# consulta_direta_v2.py - Versão aprimorada com nome da empresa e situação detalhada
import sqlite3
import argparse
import pandas as pd
import os
import re
from difflib import SequenceMatcher
import json

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
    return mapeamento.get(codigo_str, f"CÓDIGO {codigo_str}")

def buscar_informacoes_empresa(cursor, cnpj_basico):
    """Busca informações detalhadas da empresa"""
    empresas = []
    
    try:
        # Verificar a existência da tabela 'outros'
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='outros'")
        tem_tabela_outros = cursor.fetchone() is not None
        
        # Verificar 'empresas'
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='empresas'")
        tem_tabela_empresas = cursor.fetchone() is not None
        
        # Consulta na tabela de estabelecimentos para obter informações básicas
        query_estabelecimento = """
        SELECT 
            cnpj_basico,
            [02] AS situacao_cadastral,
            [4723700] AS cnae_principal,
            [rua] || ' ' || [nilso_braun] AS endereco,
            [parque_das_palmeiras] AS bairro,
            [sc] AS uf
        FROM estabelecimentos
        WHERE cnpj_basico = ?
        """
        
        cursor.execute(query_estabelecimento, (cnpj_basico,))
        estabelecimentos = cursor.fetchall()
        
        if not estabelecimentos:
            return []
        
        colunas_estab = [col[0] for col in cursor.description]
        
        # Nome da empresa - tentar buscar em diferentes tabelas
        nome_empresa = "Nome não encontrado"
        
        # Tentar na tabela 'empresas' primeiro (opção mais provável)
        if tem_tabela_empresas:
            try:
                cursor.execute("""
                SELECT * FROM empresas 
                WHERE cnpj_basico = ? 
                LIMIT 1
                """, (cnpj_basico,))
                
                empresa = cursor.fetchone()
                if empresa:
                    # Procurar coluna com nome da empresa
                    colunas_empresa = [col[0].lower() for col in cursor.description]
                    idx_nome = None
                    
                    # Procurar colunas candidatas
                    for i, col in enumerate(colunas_empresa):
                        if 'razao' in col or 'nome' in col or 'social' in col or col == 'col_1':
                            idx_nome = i
                            break
                    
                    if idx_nome is not None:
                        nome_empresa = empresa[idx_nome]
            except Exception as e:
                print(f"Aviso: Erro ao buscar na tabela empresas: {e}")
        
        # Tentar na tabela 'outros' se o nome ainda não foi encontrado
        if nome_empresa == "Nome não encontrado" and tem_tabela_outros:
            try:
                cursor.execute("""
                SELECT * FROM outros 
                WHERE col_0 = ? 
                LIMIT 1
                """, (cnpj_basico,))
                
                empresa = cursor.fetchone()
                if empresa:
                    # Procurar coluna com nome da empresa
                    colunas_outros = [col[0].lower() for col in cursor.description]
                    idx_nome = None
                    
                    # Procurar colunas candidatas
                    for i, col in enumerate(colunas_outros):
                        if 'razao' in col or 'nome' in col or 'social' in col or col == 'col_1':
                            idx_nome = i
                            break
                    
                    if idx_nome is not None and len(empresa) > idx_nome:
                        nome_empresa = empresa[idx_nome]
            except Exception as e:
                print(f"Aviso: Erro ao buscar na tabela outros: {e}")
        
        # Montar informações para cada estabelecimento
        for estabelecimento in estabelecimentos:
            empresa_dict = {}
            for i, col in enumerate(colunas_estab):
                empresa_dict[col] = estabelecimento[i]
            
            # Adicionar nome da empresa
            empresa_dict["nome_empresa"] = nome_empresa
            
            # Mapear situação cadastral
            situacao = empresa_dict.get("situacao_cadastral", "")
            empresa_dict["situacao_descricao"] = mapear_situacao_cadastral(situacao)
            
            empresas.append(empresa_dict)
    
    except Exception as e:
        print(f"Erro ao buscar informações da empresa: {e}")
    
    return empresas

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
            cnpj_basico = socio[idx_cnpj]
            empresas = buscar_informacoes_empresa(cursor, cnpj_basico)
            
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
        # Buscar informações do estabelecimento
        empresas = buscar_informacoes_empresa(cursor, cnpj_basico)
        
        if not empresas:
            print("CNPJ não encontrado.")
            return {
                "cnpj": cnpj,
                "status": "Não encontrado",
                "socios": []
            }
        
        # Usar a primeira empresa da lista para informações gerais
        empresa = empresas[0]
        situacao = empresa.get("situacao_cadastral", "")
        situacao_descr = empresa.get("situacao_descricao", "")
        
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
            "nome_empresa": empresa.get("nome_empresa", "Nome não encontrado"),
            "situacao": situacao,
            "situacao_descricao": situacao_descr,
            "endereco": empresa.get("endereco", ""),
            "bairro": empresa.get("bairro", ""),
            "uf": empresa.get("uf", ""),
            "cnae_principal": empresa.get("cnae_principal", ""),
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

def processar_arquivo_socios(db_path, arquivo, limiar=0.7):
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
        resultado = consulta_socio_direta(db_path, socio['nome'], socio['cpf'], limiar)
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
                'empresas': ', '.join([e.get('nome_empresa', 'N/A') for e in r.get('empresas', [])])
            }
            for r in resultados
        ])
        
        nome_csv = os.path.splitext(arquivo)[0] + "_resultados.csv"
        df_resumo.to_csv(nome_csv, index=False)
        print(f"Resumo salvo em {nome_csv}")
    except Exception as e:
        print(f"Erro ao gerar CSV resumido: {e}")
    
    return resultados

def processar_arquivo_cnpjs(db_path, arquivo):
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
        resultado = verificar_cnpj_direto(db_path, cnpj)
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
                'cnpj': r['cnpj'],
                'cnpj_basico': r.get('cnpj_basico', ''),
                'nome_empresa': r.get('nome_empresa', 'Não encontrado'),
                'situacao': r.get('situacao_descricao', r.get('status', '')),
                'qtd_socios': len(r.get('socios', [])),
                'socios': ', '.join([s.get('nome', 'N/A') for s in r.get('socios', [])])
            }
            for r in resultados
        ])
        
        nome_csv = os.path.splitext(arquivo)[0] + "_resultados.csv"
        df_resumo.to_csv(nome_csv, index=False)
        print(f"Resumo salvo em {nome_csv}")
    except Exception as e:
        print(f"Erro ao gerar CSV resumido: {e}")
    
    return resultados

def main():
    parser = argparse.ArgumentParser(description='Consulta direta de sócios/CNPJs sem correção completa')
    
    subparsers = parser.add_subparsers(dest='comando', help='Comandos disponíveis')
    
    # Subcomando para consulta de sócio
    parser_socio = subparsers.add_parser('socio', help='Consultar sócio por nome e CPF')
    parser_socio.add_argument('--nome', type=str, help='Nome do sócio')
    parser_socio.add_argument('--cpf', type=str, help='CPF do sócio')
    parser_socio.add_argument('--arquivo', type=str, help='Arquivo com lista de sócios (CSV ou TXT)')
    parser_socio.add_argument('--limiar', type=float, default=0.7, help='Limiar de similaridade (0.0-1.0)')
    parser_socio.add_argument('--banco', type=str, default='cnpj_amostra.db', help='Caminho para o banco de dados')
    
    # Subcomando para verificação de CNPJ
    parser_cnpj = subparsers.add_parser('cnpj', help='Verificar CNPJ e seus sócios')
    parser_cnpj.add_argument('--cnpj', type=str, help='CNPJ a verificar')
    parser_cnpj.add_argument('--arquivo', type=str, help='Arquivo com lista de CNPJs (CSV ou TXT)')
    parser_cnpj.add_argument('--banco', type=str, default='cnpj_amostra.db', help='Caminho para o banco de dados')
    
    args = parser.parse_args()
    
    if args.comando == 'socio':
        if args.arquivo:
            # Processar arquivo com múltiplos sócios
            processar_arquivo_socios(args.banco, args.arquivo, args.limiar)
        elif args.nome and args.cpf:
            # Consultar um único sócio
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
            processar_arquivo_cnpjs(args.banco, args.arquivo)
        elif args.cnpj:
            # Verificar um único CNPJ
            resultado = verificar_cnpj_direto(args.banco, args.cnpj)
            
            print("\nRESULTADO DA CONSULTA:")
            print("-" * 60)
            
            # Mostrar nome e situação primeiro
            if 'nome_empresa' in resultado:
                print(f"Nome: {resultado['nome_empresa']}")
            
            if 'situacao_descricao' in resultado:
                print(f"Situação: {resultado['situacao_descricao']}")
            
            # Depois mostrar outros campos
            for key, value in resultado.items():
                if key not in ['socios', 'nome_empresa', 'situacao_descricao']:
                    print(f"{key}: {value}")
            
            if resultado.get('socios'):
                print("\nSÓCIOS:")
                for i, socio in enumerate(resultado['socios']):
                    print(f"\nSócio {i+1}:")
                    for key, value in socio.items():
                        print(f"  {key}: {value}")
        else:
            parser_cnpj.print_help()
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
