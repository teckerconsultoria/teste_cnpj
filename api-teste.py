#!/usr/bin/env python3
# api_teste.py
import sqlite3
from difflib import SequenceMatcher
from flask import Flask, request, jsonify
import time
import unicodedata
import re
import os

app = Flask(__name__)

# Função para normalizar nomes
def normalizar_nome(nome):
    """Normaliza o nome para comparação"""
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

# Função para calcular similaridade
def similaridade(a, b):
    """Calcula a similaridade entre duas strings"""
    if not a or not b:
        return 0
    return SequenceMatcher(None, a, b).ratio()

# Função para extrair miolo do CPF
def extrair_miolo_cpf(cpf):
    """Extrai o miolo do CPF (6 dígitos centrais)"""
    # Remover caracteres não numéricos
    cpf_limpo = ''.join(filter(str.isdigit, str(cpf)))
    
    # Verificar se tem 11 dígitos
    if len(cpf_limpo) != 11:
        return None
    
    # Extrair miolo (posições 3 a 8, 6 dígitos centrais)
    return cpf_limpo[3:9]

# Rota para consulta de sócio por CPF e nome
@app.route('/api/consultar_socio', methods=['POST'])
def consultar_socio_api():
    """Endpoint para consultar sócio por CPF e nome"""
    # Obter dados da requisição
    dados = request.json
    
    if not dados:
        return jsonify({
            'status': 'erro',
            'mensagem': 'Dados não fornecidos'
        }), 400
    
    cpf = dados.get('cpf')
    nome = dados.get('nome')
    
    if not cpf:
        return jsonify({
            'status': 'erro',
            'mensagem': 'CPF é obrigatório'
        }), 400
    
    # Extrair miolo do CPF
    miolo_cpf = extrair_miolo_cpf(cpf)
    
    if not miolo_cpf:
        return jsonify({
            'status': 'erro',
            'mensagem': 'CPF inválido'
        }), 400
    
    # Iniciar tempo de processamento
    inicio = time.time()
    
    # Conectar ao banco SQLite
    conn = sqlite3.connect('cnpj_amostra.db')
    cursor = conn.cursor()
    
    try:
        # Buscar por miolo de CPF
        cursor.execute('''
        SELECT cpf_miolo, nome_socio, cnpj, cpf_numeros
        FROM socios
        WHERE cpf_miolo = ?
        LIMIT 100
        ''', (miolo_cpf,))
        
        resultados = cursor.fetchall()
        
        # Se não encontrou, retorna vazio
        if not resultados:
            fim = time.time()
            return jsonify({
                'nome': nome,
                'cpf': cpf,
                'status': 'CPF não localizado',
                'empresas': [],
                'tempo_ms': (fim - inicio) * 1000
            })
        
        # Se não foi fornecido nome, retorna todos os resultados encontrados
        if not nome:
            empresas = []
            for _, nome_socio, cnpj, _ in resultados:
                # Verificar se a empresa já está na lista
                if not any(e.get('cnpj') == cnpj for e in empresas):
                    empresas.append({
                        'cnpj': cnpj,
                        'nome': nome_socio
                    })
            
            fim = time.time()
            return jsonify({
                'nome': nome,
                'cpf': cpf,
                'status': 'sucesso',
                'empresas': empresas,
                'tempo_ms': (fim - inicio) * 1000
            })
        
        # Filtrar resultados por similaridade de nome
        nome_normalizado = normalizar_nome(nome)
        
        resultados_com_score = []
        
        for cpf_miolo, nome_socio, cnpj, cpf_numeros in resultados:
            nome_socio_norm = normalizar_nome(nome_socio)
            score = similaridade(nome_normalizado, nome_socio_norm)
            
            # Adicionar se score for adequado
            if score > 0.7:  # Limiar de similaridade configurável
                # Verificar se já temos este CNPJ
                existente = next((r for r in resultados_com_score if r['cnpj'] == cnpj), None)
                
                if not existente:
                    resultados_com_score.append({
                        'cpf_miolo': cpf_miolo,
                        'nome': nome_socio,
                        'cnpj': cnpj,
                        'cpf': cpf_numeros,
                        'score': score
                    })
        
        # Ordenar por similaridade
        resultados_com_score.sort(key=lambda x: x['score'], reverse=True)
        
        # Preparar resposta
        empresas = []
        
        for resultado in resultados_com_score:
            empresas.append({
                'cnpj': resultado['cnpj'],
                'nome': resultado['nome'],
                'score': resultado['score']
            })
        
        # Verificar status baseado nos resultados
        status = 'sucesso' if empresas else 'Nome não corresponde ao CPF'
        
        fim = time.time()
        
        return jsonify({
            'nome': nome,
            'cpf': cpf,
            'status': status,
            'empresas': empresas,
            'tempo_ms': (fim - inicio) * 1000
        })
        
    except Exception as e:
        fim = time.time()
        return jsonify({
            'nome': nome,
            'cpf': cpf,
            'status': 'erro',
            'mensagem': str(e),
            'empresas': [],
            'tempo_ms': (fim - inicio) * 1000
        }), 500
    
    finally:
        conn.close()

# Rota para informações sobre o banco de dados
@app.route('/api/info', methods=['GET'])
def info_api():
    """Endpoint para obter informações sobre o banco de dados"""
    try:
        if not os.path.exists('cnpj_amostra.db'):
            return jsonify({
                'status': 'erro',
                'mensagem': 'Banco de dados não encontrado'
            }), 404
        
        # Obter tamanho do banco
        tamanho_mb = os.path.getsize('cnpj_amostra.db') / (1024 * 1024)
        
        # Conectar ao banco
        conn = sqlite3.connect('cnpj_amostra.db')
        cursor = conn.cursor()
        
        # Obter lista de tabelas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tabelas = cursor.fetchall()
        
        # Obter contagem de registros por tabela
        tabelas_info = []
        
        for tabela in tabelas:
            nome_tabela = tabela[0]
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {nome_tabela}").fetchone()[0]
                tabelas_info.append({
                    'nome': nome_tabela,
                    'registros': count
                })
            except:
                pass
        
        # Obter índices
        cursor.execute("SELECT name, tbl_name FROM sqlite_master WHERE type='index';")
        indices = cursor.fetchall()
        
        indices_info = []
        for nome_indice, tabela in indices:
            indices_info.append({
                'nome': nome_indice,
                'tabela': tabela
            })
        
        # Obter estatísticas de miolos
        estatisticas_miolos = {}
        
        if 'socios' in [t[0] for t in tabelas]:
            try:
                miolos_unicos = conn.execute(
                    "SELECT COUNT(DISTINCT cpf_miolo) FROM socios WHERE cpf_miolo != ''"
                ).fetchone()[0]
                
                estatisticas_miolos['miolos_unicos'] = miolos_unicos
            except:
                pass
        
        conn.close()
        
        return jsonify({
            'status': 'sucesso',
            'tamanho_mb': tamanho_mb,
            'tabelas': tabelas_info,
            'indices': indices_info,
            'estatisticas_miolos': estatisticas_miolos
        })
        
    except Exception as e:
        return jsonify({
            'status': 'erro',
            'mensagem': str(e)
        }), 500

# Rota inicial para teste
@app.route('/', methods=['GET'])
def home():
    """Rota inicial para teste da API"""
    return jsonify({
        'status': 'online',
        'mensagem': 'API de teste para consulta CNPJ por miolo de CPF',
        'endpoints': {
            '/api/consultar_socio': 'POST - Consulta sócio por CPF e nome',
            '/api/info': 'GET - Informações sobre o banco de dados'
        }
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)