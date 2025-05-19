#!/usr/bin/env python3
# corrigir_lotes.py - Corrige a coluna cpf_miolo em pequenos lotes
import sqlite3
import os
import time
from tqdm import tqdm

def corrigir_em_lotes(db_path, tamanho_lote=1000, max_lotes=None):
    """
    Corrige a coluna cpf_miolo processando em pequenos lotes
    com feedback de progresso e possibilidade de interrupção
    """
    print(f"Iniciando correção em lotes no banco {db_path}...")
    
    if not os.path.exists(db_path):
        print(f"Erro: Banco de dados não encontrado: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 1. Criar tabela de controle se não existir
        print("Preparando tabela de controle...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS correcao_controle (
            id INTEGER PRIMARY KEY,
            ultimo_id INTEGER,
            registros_processados INTEGER,
            iniciado TEXT,
            ultima_atualizacao TEXT,
            status TEXT
        )
        """)
        conn.commit()
        
        # 2. Verificar se já existe um processamento em andamento
        cursor.execute("SELECT * FROM correcao_controle ORDER BY id DESC LIMIT 1")
        controle = cursor.fetchone()
        
        ultimo_id = 0
        registros_processados = 0
        
        if controle and controle[5] == 'em_andamento':
            ultimo_id = controle[1]
            registros_processados = controle[2]
            print(f"Retomando processamento a partir do ID {ultimo_id}")
            print(f"Já foram processados {registros_processados} registros")
        else:
            # Criar novo registro de controle
            cursor.execute("""
            INSERT INTO correcao_controle (ultimo_id, registros_processados, iniciado, ultima_atualizacao, status)
            VALUES (0, 0, datetime('now'), datetime('now'), 'em_andamento')
            """)
            conn.commit()
        
        # 3. Contar quantos registros precisam ser processados
        print("Contando registros a processar...")
        cursor.execute("""
        SELECT COUNT(*) FROM socios 
        WHERE rowid > ? AND (
            cpf_miolo NOT GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]'
            OR LENGTH(cpf_miolo) != 6
            OR cpf_miolo IS NULL 
            OR cpf_miolo = ''
        )
        """, (ultimo_id,))
        
        total_restante = cursor.fetchone()[0]
        print(f"Restam {total_restante} registros para processar")
        
        if total_restante == 0:
            print("Não há registros para corrigir. O processo está completo!")
            
            # Atualizar status
            cursor.execute("""
            UPDATE correcao_controle 
            SET status = 'concluido', ultima_atualizacao = datetime('now')
            WHERE id = (SELECT MAX(id) FROM correcao_controle)
            """)
            conn.commit()
            
            conn.close()
            return
        
        # 4. Processar em lotes
        lotes_processados = 0
        inicio_geral = time.time()
        
        try:
            while True:
                inicio_lote = time.time()
                
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
                LIMIT {tamanho_lote}
                """, (ultimo_id,))
                
                lote = cursor.fetchall()
                
                if not lote:
                    print("Não há mais registros para processar. Concluído!")
                    break
                
                print(f"\nProcessando lote {lotes_processados+1} com {len(lote)} registros...")
                
                # Atualizar cada registro no lote
                for i, (rowid, cpf_original) in enumerate(tqdm(lote, desc="Atualizando")):
                    # Extrair miolo
                    miolo = ""
                    
                    if cpf_original:
                        # Remover caracteres não numéricos
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
                    
                    # Atualizar o registro
                    cursor.execute("""
                    UPDATE socios
                    SET cpf_miolo = ?
                    WHERE rowid = ?
                    """, (miolo, rowid))
                    
                    # Atualizar último ID processado
                    ultimo_id = rowid
                
                # Commit ao final do lote
                conn.commit()
                
                # Atualizar contadores
                registros_processados += len(lote)
                lotes_processados += 1
                
                # Atualizar registro de controle
                cursor.execute("""
                UPDATE correcao_controle 
                SET ultimo_id = ?, registros_processados = ?, ultima_atualizacao = datetime('now')
                WHERE id = (SELECT MAX(id) FROM correcao_controle)
                """, (ultimo_id, registros_processados))
                conn.commit()
                
                # Calcular estatísticas
                tempo_lote = time.time() - inicio_lote
                registros_por_segundo = len(lote) / tempo_lote if tempo_lote > 0 else 0
                
                tempo_total = time.time() - inicio_geral
                registros_por_segundo_total = registros_processados / tempo_total if tempo_total > 0 else 0
                
                # Estimar tempo restante
                if registros_por_segundo_total > 0:
                    segundos_restantes = (total_restante - registros_processados) / registros_por_segundo_total
                    horas = int(segundos_restantes / 3600)
                    minutos = int((segundos_restantes % 3600) / 60)
                    segundos = int(segundos_restantes % 60)
                    tempo_restante = f"{horas}h {minutos}m {segundos}s"
                else:
                    tempo_restante = "desconhecido"
                
                # Exibir estatísticas
                print(f"Progresso: {registros_processados}/{total_restante+registros_processados} registros ({(registros_processados/(total_restante+registros_processados)*100):.1f}%)")
                print(f"Velocidade: {registros_por_segundo:.1f} reg/s (lote atual), {registros_por_segundo_total:.1f} reg/s (média)")
                print(f"Tempo estimado restante: {tempo_restante}")
                
                # Verificar se deve parar por limite de lotes
                if max_lotes and lotes_processados >= max_lotes:
                    print(f"\nLimite de {max_lotes} lotes atingido. Pausa automática.")
                    print("Você pode continuar o processamento posteriormente executando este script novamente.")
                    break
                
                # Perguntar se deseja continuar a cada 5 lotes
                if lotes_processados % 5 == 0 and lotes_processados > 0:
                    resposta = input("\nContinuar processamento? (S/n): ")
                    if resposta.lower() == 'n':
                        print("Processamento pausado pelo usuário.")
                        print("Você pode continuar o processamento posteriormente executando este script novamente.")
                        break
        
        except KeyboardInterrupt:
            print("\nProcessamento interrompido pelo usuário (Ctrl+C).")
            print("O progresso foi salvo e você pode continuar posteriormente.")
        
        # 5. Atualizar status como 'pausado' se não terminou
        if total_restante - registros_processados > 0:
            cursor.execute("""
            UPDATE correcao_controle 
            SET status = 'pausado', ultima_atualizacao = datetime('now')
            WHERE id = (SELECT MAX(id) FROM correcao_controle)
            """)
        else:
            cursor.execute("""
            UPDATE correcao_controle 
            SET status = 'concluido', ultima_atualizacao = datetime('now')
            WHERE id = (SELECT MAX(id) FROM correcao_controle)
            """)
        
        conn.commit()
    
    except Exception as e:
        print(f"Erro durante o processamento: {e}")
    
    finally:
        conn.close()
        print("\nProcessamento finalizado!")

def criar_indice_otimizado(db_path):
    """Cria um índice otimizado para a coluna cpf_miolo"""
    print(f"Criando índice otimizado para cpf_miolo em {db_path}...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        print("Removendo índice existente se houver...")
        cursor.execute("DROP INDEX IF EXISTS idx_socios_cpf_miolo")
        conn.commit()
        
        print("Criando novo índice...")
        cursor.execute("CREATE INDEX idx_socios_cpf_miolo ON socios(cpf_miolo)")
        conn.commit()
        
        print("Índice criado com sucesso!")
    except Exception as e:
        print(f"Erro ao criar índice: {e}")
    
    conn.close()

def executar_teste_consulta(db_path):
    """Executa uma consulta de teste para verificar se a correção funcionou"""
    print(f"Executando consulta de teste em {db_path}...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Buscar um CPF corrigido para teste
        cursor.execute("""
        SELECT cpf_miolo
        FROM socios
        WHERE LENGTH(cpf_miolo) = 6 AND cpf_miolo GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]'
        LIMIT 1
        """)
        
        result = cursor.fetchone()
        
        if result:
            miolo = result[0]
            print(f"Testando consulta com miolo: {miolo}")
            
            cursor.execute("""
            SELECT COUNT(*)
            FROM socios
            WHERE cpf_miolo = ?
            """, (miolo,))
            
            count = cursor.fetchone()[0]
            print(f"Resultados encontrados: {count}")
            
            if count > 0:
                cursor.execute("""
                SELECT * FROM socios WHERE cpf_miolo = ? LIMIT 3
                """, (miolo,))
                
                results = cursor.fetchall()
                print("Amostra de resultados:")
                for i, res in enumerate(results):
                    print(f"  Resultado {i+1}: {res}")
                
                return True
            else:
                print("Nenhum resultado encontrado.")
                return False
        else:
            print("Não foi possível encontrar um miolo para teste.")
            return False
    
    except Exception as e:
        print(f"Erro ao executar teste: {e}")
        return False
    
    finally:
        conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Corrige a coluna cpf_miolo em lotes')
    parser.add_argument('--banco', type=str, default='cnpj_amostra.db', help='Caminho para o banco de dados')
    parser.add_argument('--lote', type=int, default=1000, help='Tamanho do lote')
    parser.add_argument('--max', type=int, default=None, help='Número máximo de lotes a processar')
    parser.add_argument('--indice', action='store_true', help='Apenas criar índice otimizado')
    parser.add_argument('--teste', action='store_true', help='Apenas executar teste de consulta')
    
    args = parser.parse_args()
    
    if args.indice:
        criar_indice_otimizado(args.banco)
    elif args.teste:
        executar_teste_consulta(args.banco)
    else:
        corrigir_em_lotes(args.banco, args.lote, args.max)
