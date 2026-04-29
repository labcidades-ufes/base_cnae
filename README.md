Pipeline de coleta e padronização de todas as versões disponíveis da Classificação Nacional de Atividades Econômicas (CNAE), publicadas pelo CONCLA/IBGE.

## Estrutura do projeto

- `coleta/`: raspa a página de downloads do CONCLA, baixa todos os arquivos XLS/XLSX de estrutura detalhada e salva na camada bronze.
- `pre_processamento/`: remove cabeçalhos residuais, valida o formato dos códigos de subclasse e normaliza tipos para a camada silver.
- `processamento/`: aplica roteamento por estrutura de colunas, limpeza de texto e schema final para gerar a camada gold.
- `utils.R`: funções compartilhadas para leitura/escrita de dados no MinIO via DuckDB.
- `base_cnae.py`: DAG do Airflow orquestrando as três etapas via DockerOperator.

## Fluxo base do pipeline

1. Coleta: CONCLA/IBGE → bronze
2. Pré-processamento: bronze → silver
3. Processamento: silver → gold

## Convenção de camadas

- Bronze: arquivos XLS/XLSX brutos de cada versão CNAE empilhados em colunas posicionais (`c1` a `c6`)
- Silver: dados normalizados com tipos corretos, cabeçalhos residuais removidos e formato de `cod_subclasse` validado
- Gold: schema final com hierarquia completa, texto padronizado em maiúsculas sem acento e versões identificadas

## Fonte oficial

| Recurso | URL |
|---|---|
| Página de downloads | https://cnae.ibge.gov.br/classificacoes/download-concla/8265-download |

## Detecção automática de versão

O script de coleta raspa a tabela da página do CONCLA via `rvest` e baixa todos os arquivos listados com classificação CNAE. Novas versões publicadas pelo IBGE são coletadas automaticamente sem alteração no código.

## Versões coletadas

| Versão | Nível | Subclasses |
|---|---|---|
| CNAE 2.3 Subclasses | Subclasse | 1.331 |
| CNAE 2.2 Subclasses | Subclasse | 1.329 |
| CNAE 2.1 Subclasses | Subclasse | 1.318 |
| CNAE 2.0 Subclasses | Subclasse | 1.301 |
| CNAE-Fiscal 1.1 | Subclasse | 1.182 |
| CNAE 2.0 Classes | Classe | 673 |
| CNAE 1.0 | Classe | 564 |
| CNAE | Classe | 564 |
| CNAE-Domiciliar 2.0 | Subclasse | 243 |
| CNAE-Domiciliar | Código | 273 |

## Validações aplicadas no pré-processamento

- Remoção de linhas com cabeçalhos residuais (textos descritivos que passam pelo filtro de leitura)
- Verificação do formato `NNNN-N/NN` no campo `cod_subclasse` para versões com subclasse
- Log de registros fora do padrão sem interrupção do pipeline
- Contagem de versões coletadas com sucesso

## Schema gold

```
versao_cnae           — nome completo da classificação (ex: "CNAE 2.3 Subclasses")
cod_secao             — letra da seção (ex: "A")
cod_divisao           — código de 2 dígitos (ex: "01")
cod_grupo             — código de 3 dígitos (ex: "01.1")
cod_classe            — código com hífen (ex: "01.11-3")
cod_subclasse         — código completo (ex: "0111-3/01") — NA para versões no nível de Classe
nome_subclasse        — denominação em maiúsculas sem acento (uso em joins)
nome_original         — denominação exata conforme publicado pelo IBGE
```

> Para uso em produção, filtre por `versao_cnae == "CNAE 2.3 Subclasses"`.
> Versões anteriores estão disponíveis no mesmo arquivo para análises históricas e de compatibilidade.
