# Fechamento da revisão técnica — base_cnae

Data da consolidação: 2026-10-29.

Situação: revisão técnica consolidada com pendências.
Estado da base: homologacao.
Este documento não promove a base a produção nem autoriza publicação.

## Escopo e resultados

- Quatro classificações CNAE conferidas no escopo documentado.
- Controles bloqueantes de completude, conformidade e chaves implementados.
- Gravação de gold parcial corrigida e testada com entradas inválidas.
- ZIPs originais e manifesto preservados pela coleta.
- Responsável nominal conferido nas tarefas.
- Alertas de falha testados, com recebimento confirmado.
- Gravação condicional testada, preservando objetos existentes.
- Conta técnica própria por domínio, com testes de permissões e acesso R.
- Execução completa pelo Airflow com a conta própria concluída com sucesso.

Volumes conferidos no trabalho: Bronze: 9.179; silver: 8.952; gold: 4.635.

Execução de referência: `manual__contas_dominio_20261027T163427Z_cnae`.
Carga: `8655ff76ac8aac94cc6a96b1608d39c4e604a5f80b24e1a6e093bb7a31d15d48`.
Registro: `include/curadoria_cnae_carga_contas_dominio_20261027T163427Z.json`.

As conferências detalhadas e seus limites permanecem registrados
em `metadados.yml`, `docs/dicionario.md` e nas evidências referenciadas.
Sucesso de execução não substitui conferência de conteúdo.

## Pendências para admissão

- Substituir a credencial administrativa padrão do MinIO, tratando as dependências dos demais pipelines.
- Padrão datado G-6.3: implementação e conferência aprovadas em 29/10/2026.
- Conferir a completude da procedência das cargas escolhidas para admissão. Os registros retrospectivos existentes cobrem as cargas de 27/10/2026; não devem ser atribuídos automaticamente às cargas posteriores.
- Confirmar condições de uso da fonte, consumidores e classificação conforme a finalidade efetiva.
- Teste de recuperação das cargas de referência aprovado. Definir backup periódico em destino independente e comprovar recuperação dos demais componentes da plataforma.
- Aplicar o checklist institucional de admissão e registrar a aprovação do Gestor.

## Procedência enquanto não houver automatização completa

Para cada nova carga, registrar fonte, data de coleta, execução, entradas,
saídas e versão do pipeline junto aos dados. Identificar explicitamente
os registros produzidos retrospectivamente e suas limitações.
Conferir os objetos e guardar a evidência na ficha antes de propor
essa carga para consumo em produção.

A automatização adicional é uma melhoria futura.
O atendimento ao requisito de procedência continua necessário.

## Aprovação

Conferência e aceite do Curador: pendentes de registro formal.
Admissão em produção pelo Gestor: pendente.
Referência à decisão ou ata: não informada.


## Responsáveis e política — registro de 2026-10-29

A usuária confirmou que o LabCidades adotou a política enviada como regra.
A referência documental ao ato de adoção e o cabeçalho de proposta
devem ser reconciliados na documentação institucional.

Curadora: Julia Ricas Nunes Pereira.
Gestor informado: Pedro; identificação completa a complementar.
A aprovação de admissão em produção permanece pendente.


## Recuperação das cargas — teste de 29/10/2026

Teste `20261029T190536Z-6825768ba06f` aprovado.
Objetos desta base conferidos: 11.
Camadas abrangidas: bronze, silver e gold.

A restauração ocorreu em chaves de teste, com comparação de SHA-256
e tamanho. As cópias no host também foram conferidas.

Evidência: `include/evidencias_curadoria/20261029T190536Z-6825768ba06f-recuperacao/resultado.json`.

O teste não demonstra recuperação completa da plataforma.
A cópia está no mesmo computador. Backup periódico, destino independente
e recuperação dos demais componentes continuam pendentes.


## Padrão vigente de caminhos — validado em 29/10/2026

Parquets vigentes:

- `bronze/base_cnae/raw_cnae_<AAAAMMDD>.parquet`.
- `silver/base_cnae/silver_cnae_<AAAAMMDD>.parquet`.
- `gold/base_cnae/gold_cnae_<AAAAMMDD>.parquet`.

A data é obtida do início do DagRun em UTC e compartilhada pelas três
etapas. O consumo utiliza caminhos explícitos. Fontes e manifestos
continuam identificados por carga_id.

Execução: `manual__padrao_datado_20261029T191754Z_cnae`.
Evidência: `include/evidencias_curadoria/conferencia-datados-20261029T192806Z-488353/resultado.json`.

A gravação permanece condicional. Se o destino datado existir, o envio
é rejeitado. Novo run_id no mesmo dia não permite substituir o objeto.
Preservar cargas parciais para análise; uma nova carga completa exige
destinos livres.

Esta seção substitui as orientações anteriores sobre caminhos Parquet
por carga_id e recuperação apenas por novo run_id.
Os objetos históricos permanecem preservados.
