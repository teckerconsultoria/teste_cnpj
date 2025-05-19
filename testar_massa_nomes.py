#!/usr/bin/env python3
# testar_massa_nomes.py - Script para processar lista de nomes/CPFs
import sqlite3
import argparse
import pandas as pd
import os
import csv
import re
from tqdm import tqdm
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

def consultar_socio(conn, nome, cpf, limiar_similaridade=0.7):
    """Consulta um sócio pelo miolo do CPF e nome no banco"""
    cursor = conn.cursor()
    
    # Extrair miolo do CPF
    miolo_cpf = extrair_miolo_cpf(cpf)
    
    if not miolo_cpf or len(miolo_cpf) < 3:
        return {
            "nome": nome,
            "cpf": cpf,
            "miolo_cpf": miolo_cpf,
            "status": "CPF inválido",
            "score": 0,
            "empresas": []
        }
    
    # Verificar se a visão otimizada existe
    cursor.execute("SELECT name FROM sqlite_master WHERE type='view' AND name='vw_socios_otimizada'")
    tem_visao_otimizada = cursor.fetchone() is not None
    
    try:
        # Usar a visão se existir
        if tem_visao_otimizada:
            query = """
            SELECT 
                cpf_miolo, 
                nome_socio, 
                cpf_cnpj_socio, 
                cnpj_basico
            FROM vw_socios_otimizada
            WHERE cpf_miolo = ?
            """
        else:
            # Tentar usar a tabela de sócios diretamente
            query = """
            SELECT 
                cpf_miolo, 
                "livia_maria_andrade_ramos_gaertner" AS nome_socio,
                "***331355**" AS cpf_cnpj_socio,
                "03769328" AS cnpj_basico
            FROM socios
            WHERE cpf_miolo = ?
            """
        
        cursor.execute(query, (miolo_cpf,))
        resultados = cursor.fetchall()
        
        # Se não encontrou resultados
        if not resultados:
            return {
                "nome": nome,
                "cpf": cpf,
                "miolo_cpf": miolo_cpf,
                "status": "Não encontrado",
                "score": 0,
                "empresas": []
            }
        
        # Normalizar nome de entrada para comparação
        nome_normalizado = normalizar_nome(nome)
        
        # Calcular similaridade para cada resultado
        resultados_com_score = []
        for resultado in resultados:
            # Criar dicionário com os resultados
            res_dict = {}
            for i, col in enumerate(cursor.description):
                res_dict[col[0]] = resultado[i]
            
            # Calcular similaridade do nome
            nome_socio = resultado[1]  # Índice 1 = nome_socio
            nome_socio_norm = normalizar_nome(nome_socio) if nome_socio else ""
            score = similaridade(nome_normalizado, nome_socio_norm)
            
            res_dict['score'] = score
            resultados_com_score.append(res_dict)
        
        # Ordenar por similaridade
        resultados_com_score.sort(key=lambda x: x['score'], reverse=True)
        
        # Se encontrou resultado com score acima do limiar
        if resultados_com_score and resultados_com_score[0]['score'] >= limiar_similaridade:
            melhor_resultado = resultados_com_score[0]
            
            # Buscar empresas associadas ao CNPJ
            empresas = []
            
            # Verificar se existe a tabela de estabelecimentos
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='estabelecimentos'")
            tem_tabela_estab = cursor.fetchone() is not None
            
            if tem_tabela_estab:
                # Buscar informações da empresa
                try:
                    cnpj_basico = melhor_resultado['cnpj_basico']
                    cursor.execute("""
                    SELECT 
                        cnpj_basico,
                        "4723700" AS cnae_principal,
                        "rua" || ' ' || "nilso_braun" AS endereco,
                        "02" AS situacao_cadastral
                    FROM estabelecimentos
                    WHERE cnpj_basico = ?
                    """, (cnpj_basico,))
                    
                    empresas_db = cursor.fetchall()
                    for empresa in empresas_db:
                        empresa_dict = {}
                        for i, col in enumerate(cursor.description):
                            empresa_dict[col[0]] = empresa[i]
                        
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
            
            return {
                "nome": nome,
                "cpf": cpf,
                "miolo_cpf": miolo_cpf,
                "status": "Encontrado",
                "nome_encontrado": melhor_resultado['nome_socio'],
                "cpf_encontrado": melhor_resultado['cpf_cnpj_socio'],
                "score": melhor_resultado['score'],
                "empresas": empresas
            }
        
        # Se encontrou resultados, mas nenhum com score adequado
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

def carregar_socios_do_arquivo(arquivo):
    """
    Carrega uma lista de sócios (nome e CPF) de um arquivo
    """
    socios = []
    
    if not os.path.exists(arquivo):
        print(f"Erro: Arquivo {arquivo} não encontrado.")
        return []
    
    # Determinar o tipo de arquivo pela extensão
    extensao = os.path.splitext(arquivo)[1].lower()
    
    try:
        if extensao == '.csv':
            # Tentar diferentes delimitadores
            delimitadores = [',', ';', '\t', '|']
            for delimitador in delimitadores:
                try:
                    df = pd.read_csv(arquivo, delimiter=delimitador)
                    
                    # Procurar colunas de Nome e CPF
                    col_nome = None
                    col_cpf = None
                    
                    for coluna in df.columns:
                        if 'nome' in coluna.lower():
                            col_nome = coluna
                        elif 'cpf' in coluna.lower():
                            col_cpf = coluna
                    
                    # Se não encontrou explicitamente, tentar inferir
                    if not col_nome and not col_cpf and len(df.columns) >= 2:
                        col_nome = df.columns[0]
                        col_cpf = df.columns[1]
                    
                    if col_nome and col_cpf:
                        for _, row in df.iterrows():
                            socios.append({
                                'nome': str(row[col_nome]),
                                'cpf': str(row[col_cpf])
                            })
                        
                        print(f"Encontrados {len(socios)} sócios no arquivo CSV")
                        break
                except:
                    continue
            
            if not socios:
                raise Exception("Não foi possível identificar nomes e CPFs no arquivo CSV")
        
        elif extensao == '.txt':
            # Assume um sócio por linha (nome e CPF separados)
            with open(arquivo, 'r') as f:
                linhas = f.readlines()
            
            for linha in linhas:
                linha = linha.strip()
                if not linha:
                    continue
                
                # Tentar extrair nome e CPF da linha
                partes = re.split(r'[;,\t]', linha)
                
                if len(partes) >= 2:
                    # Se tiver separador explícito
                    nome = partes[0].strip()
                    cpf = partes[1].strip()
                else:
                    # Tentar extrair CPF usando expressão regular
                    match = re.search(r'(\d{3}\.?\d{3}\.?\d{3}-?\d{2}|\d{11})', linha)
                    if match:
                        cpf = match.group(1)
                        nome = linha[:match.start()].strip()
                    else:
                        # Não conseguiu identificar
                        continue
                
                socios.append({
                    'nome': nome,
                    'cpf': cpf
                })
            
            print(f"Encontrados {len(socios)} sócios no arquivo de texto")
        
        else:
            print(f"Formato de arquivo não suportado: {extensao}")
            return []
    
    except Exception as e:
        print(f"Erro ao carregar arquivo: {e}")
        return []
    
    return socios

def processar_socios(db_path, socios, saida=None, limiar_similaridade=0.7):
    """
    Processa uma lista de sócios e consulta o banco
    """
    if not os.path.exists(db_path):
        print(f"Erro: Banco de dados {db_path} não encontrado.")
        return False
    
    # Conectar ao banco
    conn = sqlite3.connect(db_path)
    
    # Preparar arquivo de saída
    if saida:
        arquivo_saida = saida
    else:
        arquivo_saida = "resultados_socios.csv"
    
    # Preparar cabeçalho do CSV
    cabecalho = ['Nome', 'CPF', 'Status', 'Score', 'Nome_Encontrado', 'CPF_Encontrado', 'Qtd_Empresas', 'Empresas']
    resultados = []
    
    print(f"Processando {len(socios)} sócios...")
    
    for socio in tqdm(socios):
        nome = socio['nome']
        cpf = socio['cpf']
        
        # Consultar sócio no banco
        resultado = consultar_socio(conn, nome, cpf, limiar_similaridade)
        
        # Formatar empresas para CSV
        empresas_texto = ""
        if resultado.get('empresas'):
            empresas_lista = []
            for empresa in resultado['empresas']:
                cnpj = empresa.get('cnpj_basico', '')
                situacao = empresa.get('situacao_descricao', '')
                empresas_lista.append(f"{cnpj} ({situacao})")
            
            empresas_texto = " | ".join(empresas_lista)
        
        # Adicionar ao resultado
        resultados.append([
            nome,
            cpf,
            resultado.get('status', ''),
            resultado.get('score', 0),
            resultado.get('nome_encontrado', ''),
            resultado.get('cpf_encontrado', ''),
            len(resultado.get('empresas', [])),
            empresas_texto
        ])
    
    # Salvar resultados
    try:
        with open(arquivo_saida, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(cabecalho)
            writer.writerows(resultados)
        
        print(f"Resultados salvos em {arquivo_saida}")
    except Exception as e:
        print(f"Erro ao salvar resultados: {e}")
    
    conn.close()
    
    # Resumo dos resultados
    try:
        df_resultados = pd.DataFrame(resultados, columns=cabecalho)
        
        # Contagem por status
        contagem_status = df_resultados['Status'].value_counts()
        
        print("\n=== RESUMO DOS RESULTADOS ===")
        print(f"Total de sócios consultados: {len(socios)}")
        print("Status das consultas:")
        for status, contagem in contagem_status.items():
            print(f"  - {status}: {contagem} ({contagem/len(socios)*100:.1f}%)")
        
        # Score médio para encontrados
        encontrados = df_resultados[df_resultados['Status'] == 'Encontrado']
        if not encontrados.empty:
            score_medio = encontrados['Score'].mean()
            print(f"Score médio para encontrados: {score_medio:.2f}")
        
        # Quantidade média de empresas
        media_empresas = df_resultados['Qtd_Empresas'].mean()
        print(f"Média de empresas por sócio: {media_empresas:.1f}")
        
        return True
    except Exception as e:
        print(f"Erro ao gerar resumo: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Processar lista de sócios (nome e CPF)')
    
    # Argumentos de linha de comando
    parser.add_argument('--arquivo', type=str, help='Arquivo com lista de sócios (CSV ou TXT)')
    parser.add_argument('--nome', type=str, help='Nome do sócio para consulta única')
    parser.add_argument('--cpf', type=str, help='CPF do sócio para consulta única')
    parser.add_argument('--saida', type=str, default='resultados_socios.csv', help='Arquivo de saída (padrão: resultados_socios.csv)')
    parser.add_argument('--banco', type=str, default='cnpj_amostra.db', help='Caminho para o banco de dados (padrão: cnpj_amostra.db)')
    parser.add_argument('--limiar', type=float, default=0.7, help='Limiar de similaridade para nomes (padrão: 0.7)')
    
    args = parser.parse_args()
    
    # Verificar se foi fornecido um arquivo ou nome+cpf
    if not args.arquivo and not (args.nome and args.cpf):
        print("Erro: Forneça um arquivo com sócios ou nome e CPF para consulta única")
        parser.print_help()
        return
    
    # Lista para armazenar os sócios
    socios = []
    
    # Se foi fornecido nome e CPF
    if args.nome and args.cpf:
        socios.append({
            'nome': args.nome,
            'cpf': args.cpf
        })
    
    # Se foi fornecido um arquivo
    if args.arquivo:
        socios = carregar_socios_do_arquivo(args.arquivo)
    
    if not socios:
        print("Erro: Nenhum sócio válido encontrado")
        return
    
    # Processar os sócios
    processar_socios(args.banco, socios, args.saida, args.limiar)

if __name__ == "__main__":
    main()
