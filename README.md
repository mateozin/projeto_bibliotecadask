# Projeto Biblioteca Dask

Projeto desenvolvido para comparar o desempenho das bibliotecas **Pandas** e **Dask** no processamento de grandes volumes de dados. O sistema gera conjuntos de dados simulando vendas, realiza análises estatísticas e produz gráficos comparativos de desempenho.

## Funcionalidades

- Geração automática de bases de dados em diferentes tamanhos:
  - Pequeno (1MB)
  - Médio (80MB)
  - Grande (330MB)
  - Grande Complexo (320MB com 15 colunas)

- Processamento dos dados utilizando:
  - Pandas
  - Dask

- Comparação de desempenho entre as bibliotecas:
  - Tempo de execução
  - Velocidade relativa
  - Agregações estatísticas

- Geração automática de gráficos comparativos.

## Como Usar? (Windows)

Crie e ative um ambiente virtual:

```bash
python -m venv .venv
.venv\Scripts\activate
```

Instale as dependências:

```bash
pip install -r requirements.txt
```

Execute o programa:

```bash
python vendas_dask.py
```

Selecione uma das opções disponíveis para realizar a análise dos dados.

## Saídas Geradas

Os gráficos são salvos automaticamente na pasta `output`.

Exemplos:

```text
output/comparacao_peq.png
output/comparacao_med.png
output/comparacao_grande.png
output/comparacao_grcomp.png
```

Cada gráfico apresenta:

- Tempo de execução do Pandas e Dask
- Fator de velocidade entre as bibliotecas
- Faturamento por categoria (Apenas um exemplo dos resultados possíveis)

## Estrutura do Projeto

```text
gerar_vendas_pequeno.py
gerar_vendas_medio.py
gerar_vendas_grande.py
gerar_vendas_grande_complexo.py
vendas_dask.py

dados_pequeno/
dados_medio/
dados_grande/
dados_grandecomp/

output/
```

## Observação

Em conjuntos de dados menores, o Pandas pode apresentar desempenho semelhante ou superior devido ao menor custo de processamento. Em bases maiores, o Dask tende a obter melhores resultados por utilizar processamento paralelo e particionamento dos dados.
