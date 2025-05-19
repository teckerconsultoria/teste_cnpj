# Sistema de Consulta CNPJ

Este projeto implementa um sistema de consulta de CNPJs e sócios utilizando dados abertos da Receita Federal, com foco na validação de miolo de CPF.

## Estrutura do Projeto

```
cnpj-consulta/
├── scripts/
│   ├── consulta/             # Scripts de consulta
│   ├── download/             # Scripts de download
│   ├── processamento/        # Scripts de processamento
│   ├── diagnostico/          # Scripts de diagnóstico
│   └── testes/               # Scripts de teste
├── docs/                     # Documentação
├── data/                     # Diretórios para dados (vazio no repo)
│   ├── amostra/
│   ├── completo/
│   └── extraidos/
├── .gitignore
├── requirements.txt
└── README.md
```

## Requisitos

- Python 3.9 ou superior
- Dependências listadas em `requirements.txt`
- Mínimo de 5GB de espaço livre em disco para testes com amostra
- Mínimo de 100GB de espaço livre para a base completa

## Instalação

1. Clone este repositório:
   ```bash
   git clone https://github.com/seu-usuario/cnpj-consulta.git
   ```

2. Crie um ambiente virtual e instale as dependências:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   venv\Scripts\activate     # Windows
   pip install -r requirements.txt
   ```

3. Crie os diretórios de dados necessários:
   ```bash
   mkdir -p data/amostra data/completo data/extraidos
   ```

## Execução

### 1. Teste com Amostra

Para executar um teste completo com amostra de dados:

```bash
python scripts/testes/executar_teste.py
```

Este comando irá:
1. Baixar uma amostra dos arquivos da Receita Federal
2. Processar os arquivos e criar um banco SQLite
3. Realizar testes de desempenho
4. Exibir estatísticas e resultados

### 2. Processamento da Base Completa

Para processar a base completa da Receita Federal:

```bash
# 1. Baixar a base completa
python scripts/download/download_cnpj_completo.py --dir data/completo

# 2. Processar a base completa
python scripts/processamento/processar_completo.py --input-dir data/completo --output-dir data/extraidos --db-path cnpj_completo.db
```

Para o processamento em lotes (recomendado para melhor controle):
```bash
python scripts/processamento/processar_completo.py --tipos socios --batch-size 50000 --recriar-tabelas
```

### 3. Consultas

Para consultar CNPJs e sócios:

```bash
# Consultar um sócio por CPF
python scripts/consulta/consulta_final.py socio --nome "NOME DO SOCIO" --cpf "12345678900"

# Verificar um CNPJ
python scripts/consulta/consulta_final.py cnpj --cnpj "12345678000199"

# Processar um arquivo com lista de sócios
python scripts/consulta/consulta_final.py socio --arquivo "caminho/para/lista_socios.csv"

# Processar um arquivo com lista de CNPJs
python scripts/consulta/consulta_final.py cnpj --arquivo "caminho/para/lista_cnpjs.csv"
```

## Funcionalidades

- **Download de dados**: Scripts para baixar amostras ou a base completa da Receita Federal
- **Processamento**: Conversão dos arquivos CSV para banco SQLite com índices otimizados
- **Consulta por miolo de CPF**: Busca sócios pelo "miolo" do CPF (6 dígitos centrais)
- **Verificação de CNPJs**: Consulta informações detalhadas de empresas por CNPJ
- **Processamento em lotes**: Importação em lotes para melhor controle e desempenho
- **Diagnóstico**: Scripts para diagnosticar e corrigir problemas na base de dados

## Licença

Este projeto está licenciado sob a licença MIT - veja o arquivo LICENSE para detalhes.

## Agradecimentos

- Receita Federal do Brasil pelos dados abertos do CNPJ
