#!/usr/bin/env python3
# corrigir_lotes_robusta.py - Versão super robusta para problemas específicos
import sqlite3
import os
import time
from tqdm import tqdm

def corrigir_em_lotes_robusta(db_path, tamanho_lote=1000, max_lotes=None):
    """Versão robusta para processamento em lotes"""
    print(f"Iniciando correção em lotes no banco {db_path}...")
    
    if not os.path.exists(db_path):
        print(f"Erro: Banco de dados não encontrado: {db_path}")
        return
    
    # Primeiro configurar as pragmas fora de qualquer transação
    conn = sqlite3.connect(db_path)
    conn.isolation_level = None  # Autocommit mode
    cursor = conn.cursor()
    
    # Configurações moderadas para melhorar desempenho
    try:
        cursor.execute("PRAGMA synchronous = NORMAL")
        cursor.execute("PRAGMA journal_mode = WAL")
        cursor.execute("PRAGMA cache_size = 10000")
    except Exception as e:
        print(f"Aviso: Não foi possível configurar PRAGMA: {e}")
    
    # Verificar e corrigir tabela de controle
    try:
        # Verificar se a tabela existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='correcao_controle'")
        tabela_existe = cursor.fetchone() is not None
        
        if tabela_existe:
            # SOLUÇÃO: Criar uma nova tabela temporária e depois substituir
            print("Recriando tabela de controle com estrutura correta...")
            cursor.execute("BEGIN TRANSACTION")
            cursor.execute("CREATE TABLE correcao_controle_nova (ultimo_id INTEGER, registros_processados INTEGER)")
            
            try:
                # Tenta recuperar dados da tabela antiga
                cursor.execute("SELECT ultimo_id, registros_processados FROM correcao_controle")
                dados = cursor.fetchone()
                if dados:
                    ultimo_id, registros_processados = dados
                    cursor.execute("INSERT INTO correcao_controle_nova VALUES (?, ?)", 
                                  (ultimo_id, registros_processados))
                else:
                    cursor.execute("INSERT INTO correcao_controle_nova VALUES (0, 0)")
            except Exception as e:
                print(f"Recuperando dados básicos: {e}")
                # Se não conseguir recuperar, cria registro zerado
                cursor.execute("INSERT INTO correcao_controle_nova VALUES (0, 0)")
            
            # Substituir tabela antiga pela nova
            cursor.execute("DROP TABLE correcao_controle")
            cursor.execute("ALTER TABLE correcao_controle_nova RENAME TO correcao_controle")
            cursor.execute("COMMIT")
        else:
            # Tabela não existe, criar com estrutura correta
            cursor.execute("CREATE TABLE correcao_controle (ultimo_id INTEGER, registros_processados INTEGER)")
            cursor.execute("INSERT INTO correcao_controle VALUES (0, 0)")
    except Exception as e:
        print(f"Erro ao recriar tabela de controle: {e}")
        cursor.execute("ROLLBACK")
        # Último recurso - ignorar controle antigo 
        try:
            cursor.execute("DROP TABLE IF EXISTS correcao_controle")
            cursor.execute("CREATE TABLE correcao_controle (ultimo_id INTEGER, registros_processados INTEGER)")
            cursor.execute("INSERT INTO correcao_controle VALUES (0, 0)")
        except Exception as e2:
            print(f"Erro crítico na tabela de controle: {e2}")
            conn.close()
            return
    
    # Agora continuamos com o processamento normal
    try:
        # Verificar total de registros a processar
        cursor.execute("""
        SELECT COUNT(*) FROM socios 
        WHERE cpf_miolo NOT GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]'
            OR LENGTH(cpf_miolo) != 6
            OR cpf_miolo IS NULL 
            OR cpf_miolo = ''
        """)
        total_registros = cursor.fetchone()[0]
        print(f"Total de {total_registros} registros para processar")
        
        # Obter progresso
        cursor.execute("SELECT ultimo_id, registros_processados FROM correcao_controle LIMIT 1")
        result = cursor.fetchone()
        if not result:
            print("Nenhum registro de controle encontrado, iniciando do zero")
            ultimo_id = 0
            processados = 0
        else:
            ultimo_id, processados = result
            print(f"Retomando de progresso anterior: ID {ultimo_id}, {processados} registros processados")
        
        inicio_geral = time.time()
        lotes_processados = 0
        
        with tqdm(total=total_registros, initial=processados, desc="Processando", unit="reg") as pbar:
            while True:
                # Buscar próximo lote
                cursor.execute(f"""
                SELECT rowid, [***331355**]
                FROM socios 
                WHERE rowid > ? AND (
                    cpf_miolo NOT GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]'
                    OR LENGTH(cpf_miolo) != 6
                    OR cpf_miolo IS NULL 
                    OR cpf_miolo = ''
                )
                ORDER BY rowid
                LIMIT ?
                """, (ultimo_id, tamanho_lote))
                
                lote = cursor.fetchall()
                
                if not lote:
                    print("\nTodos os registros processados. Concluído!")
                    break
                
                # Iniciar transação para o lote
                cursor.execute("BEGIN TRANSACTION")
                
                # Processar lote
                max_id_lote = ultimo_id
                for rowid, cpf_original in lote:
                    miolo = ""
                    if cpf_original:
                        # Remover caracteres não numéricos
                        if isinstance(cpf_original, str):
                            digitos = ''.join(c for c in cpf_original if c.isdigit())
                            
                            # Extrair miolo conforme formato
                            if '***' in cpf_original:
                                # Formato: ***XXXXXX**
                                if len(digitos) >= 6:
                                    miolo = digitos[:6]
                            elif len(digitos) >= 11:
                                # CPF completo
                                miolo = digitos[3:9]
                            elif len(digitos) >= 6:
                                # Parcial mas com digitos suficientes
                                miolo = digitos[:6]
                    
                    # Atualizar registro
                    cursor.execute("""
                    UPDATE socios
                    SET cpf_miolo = ?
                    WHERE rowid = ?
                    """, (miolo, rowid))
                    
                    # Atualizar último ID processado
                    max_id_lote = max(max_id_lote, rowid)
                
                # Commit o lote inteiro
                cursor.execute("COMMIT")
                
                # Atualizar último ID e contadores
                ultimo_id = max_id_lote
                processados += len(lote)
                lotes_processados += 1
                pbar.update(len(lote))
                
                # Atualizar controle em uma transação separada
                cursor.execute("BEGIN TRANSACTION")
                cursor.execute("UPDATE correcao_controle SET ultimo_id = ?, registros_processados = ?", 
                              (ultimo_id, processados))
                cursor.execute("COMMIT")
                
                # Mostrar estatísticas
                if lotes_processados % 5 == 0:
                    tempo_total = time.time() - inicio_geral
                    velocidade = processados / tempo_total if tempo_total > 0 else 0
                    
                    # Estimar tempo restante
                    if velocidade > 0:
                        segundos = (total_registros - processados) / velocidade
                        tempo_restante = f"{int(segundos//3600)}h {int((segundos%3600)//60)}m {int(segundos%60)}s"
                        
                        print(f"\nProgresso: {processados}/{total_registros} ({processados/total_registros*100:.1f}%)")
                        print(f"Velocidade: {velocidade:.1f} reg/s")
                        print(f"Tempo estimado restante: {tempo_restante}")
                
                # Verificar limite de lotes
                if max_lotes and lotes_processados >= max_lotes:
                    print(f"\nLimite de {max_lotes} lotes atingido. Parando.")
                    break
                
                # Permitir interrupção controlada
                if lotes_processados % 10 == 0 and lotes_processados > 0:
                    try:
                        resposta = input("\nContinuar processamento? (S/n): ")
                        if resposta.lower() == 'n':
                            print("Processamento pausado pelo usuário.")
                            break
                    except:
                        pass  # Ignora erros de input em ambientes não interativos
                
    except Exception as e:
        print(f"Erro durante o processamento: {e}")
        try:
            cursor.execute("ROLLBACK")
        except:
            pass
    finally:
        # Restaurar configurações padrão - FORA de qualquer transação
        conn.isolation_level = None  # Modo autocommit
        try:
            cursor.execute("PRAGMA synchronous = FULL")
            cursor.execute("PRAGMA journal_mode = DELETE")
        except Exception as e:
            print(f"Aviso ao restaurar configurações: {e}")
        
        conn.close()
        print("\nProcessamento finalizado!")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Corrige a coluna cpf_miolo em lotes (versão robusta)')
    parser.add_argument('--banco', type=str, default='cnpj_amostra.db', help='Caminho para o banco de dados')
    parser.add_argument('--lote', type=int, default=1000, help='Tamanho do lote')
    parser.add_argument('--max', type=int, default=None, help='Número máximo de lotes a processar')
    
    args = parser.parse_args()
    
    corrigir_em_lotes_robusta(args.banco, args.lote, args.max)