# Dicionário de dados — CNAE

Estado: homologação.
Cobertura conferida em 14/10/2026, com coleta em 13/10/2026.

## Classificações validadas

| Classificação | Nível terminal | Registros gold |
| --- | --- | ---: |
| CNAE 2.0 Classes | Classe | 673 |
| CNAE 2.0 Subclasses | Subclasse | 1.301 |
| CNAE 2.2 Subclasses | Subclasse | 1.329 |
| CNAE 2.3 Subclasses | Subclasse | 1.332 |

Total gold: 4.635 registros.
Outras versões históricas e domiciliares permanecem pendentes de integração.

## Fonte e bronze

Fonte: ZIPs oficiais do IBGE em
https://ftp.ibge.gov.br/Informacoes_Gerais_e_Referencia/Classificacoes/CNAE/

Caminho: `bronze/base_cnae/raw_cnae_<AAAAMMDD>.parquet`.

A coleta seleciona explicitamente a estrutura detalhada e lê sua primeira
aba como texto. A estrutura de classes tem cinco colunas e recebe c6 nulo
para permitir o empilhamento no esquema de seis colunas posicionais.
ZIPs originais e manifesto ficam em `bronze/base_cnae/cargas/<carga_id>/fontes/`.

## Silver

Caminho: `silver/base_cnae/silver_cnae_<AAAAMMDD>.parquet`.

Tipos esperados: VARCHAR. A silver contém linhas hierárquicas,
não apenas códigos terminais.

| Campo | Domínio ou unidade | Admite nulo | Descrição |
| --- | --- | --- | --- |
| c1 | Código de seção ou conteúdo posicional | Sim | Primeira coluna da estrutura; pode conter títulos. |
| c2 | Divisão ou conteúdo posicional | Sim | Segunda coluna. |
| c3 | Grupo ou conteúdo posicional | Sim | Terceira coluna. |
| c4 | Classe ou conteúdo posicional | Sim | Quarta coluna; contém os códigos terminais do recorte de classes. |
| c5 | Subclasse ou denominação | Conforme layout | Subclasse nos layouts de seis colunas; denominação no de cinco. |
| c6 | Denominação | Conforme layout | Denominação nos layouts de seis colunas; nulo no recorte de classes. |
| versao_label | Rótulo técnico da classificação | Não esperado | Identifica a estrutura no pipeline. |
| classificacao | Nome da classificação | Não | Identifica versão e nível. |
| data_coleta | AAAAMMDD | Não | Data do download, não vigência da classificação. |

Strings são aparadas e valores vazios convertidos em nulo.
Cabeçalhos reconhecidos são removidos.
Não há chave natural declarada para as linhas posicionais da silver.

## Gold

Caminho: `gold/base_cnae/gold_cnae_<AAAAMMDD>.parquet`.

Tipos esperados: VARCHAR. Nulabilidade abaixo representa o contrato,
não restrições NOT NULL impostas pelo Parquet.

| Campo | Domínio ou unidade | Admite nulo | Descrição e regra |
| --- | --- | --- | --- |
| versao_cnae | Uma das quatro classificações validadas | Não | Nome completo da classificação, incluindo nível. |
| cod_secao | Letras A a U | Não | Código de seção; marcadores de continuação não são códigos. |
| cod_divisao | Dois dígitos | Não | Código da divisão, preservando zeros. |
| cod_grupo | NN.N | Não | Código de grupo. |
| cod_classe | NN.NN-N | Não | Classe do registro ou classe à qual pertence a subclasse. |
| cod_subclasse | NNNN-N/NN | Somente em CNAE 2.0 Classes | Código terminal dos recortes de subclasses. |
| nome_subclasse | Denominação normalizada | Não | Texto sem acentos, em maiúsculas e com espaços normalizados; também nomeia classes. |
| nome_original | Denominação recebida da silver | Não | Texto antes da normalização gold; já teve espaços externos aparados na silver. |

A hierarquia disponível é preenchida para baixo a partir dos códigos
reconhecidos na estrutura da fonte.

### Chave condicional

- CNAE 2.0 Classes: `versao_cnae` + `cod_classe`.
- Recortes de subclasses: `versao_cnae` + `cod_subclasse`.

Não usar nome_subclasse como chave.
Não combinar versões sem selecionar explicitamente a classificação.

### Correção documentada da versão 2.2

O registro de corte e dobra de metais recebe `2599-3/02` na gold.
A estrutura coletada repetia `2599-3/01`; a correção se apoia nas notas
explicativas oficiais da edição 2015. A bronze e o ZIP permanecem preservados.

Referência:
https://ftp.ibge.gov.br/Informacoes_Gerais_e_Referencia/Classificacoes/CNAE/cnae2_2/cnae2_2_subclasses_20150609.pdf

## Evidência anterior — coleta de 13/10/2026

- Bronze: 9.179 linhas.
- Silver: 8.952 linhas.
- Gold: 4.635 registros.
- Classificações, quantidades, chaves, formatos e hierarquia:
  aprovados na conferência posterior à carga.
- Bronze datada de 13/10/2026; silver e gold datadas de 14/10/2026.
- data_coleta permanece 20261013 na bronze e silver.

Essa rodada anterior utilizava nomes datados por etapa.
As novas cargas seguem a referência por execução descrita abaixo.

Os campos desta base descrevem classificações econômicas,
sem dado pessoal conhecido.


## Referência por carga — validada em 14/10/2026

Cada execução usa `carga_id = SHA-256(run_id em UTF-8)`.
As três etapas recebem esse identificador por `CNAE_CARGA_ID`.

- Bronze: `bronze/base_cnae/cargas/<carga_id>/raw_cnae.parquet`.
- Silver: `silver/base_cnae/cargas/<carga_id>/silver_cnae.parquet`.
- Gold: `gold/base_cnae/cargas/<carga_id>/gold_cnae.parquet`.
- Manifesto: `bronze/base_cnae/cargas/<carga_id>/fontes/manifesto.csv`.

A silver lê exatamente a bronze da carga; a gold lê exatamente a silver
correspondente. A seleção não depende da ordenação dos nomes nem do dia
em que cada etapa termina.

`data_coleta` registra a data do download, sem representar a vigência da CNAE.

Execução validada: `manual__cnae_referencia-20261014T155310Z-1184`.
Carga: `b16b5cff11ea1770be3eba89201a7a17b39dc494cf53bab155a8dc5d20dbe367`.

Volumes: bronze 9.179; silver 8.952; gold 4.635.
As quatro classificações e suas quantidades foram preservadas.
O manifesto foi conferido contra o identificador, o caminho e os volumes bronze.

Evidência: `include/evidencias_curadoria/homologacao-airflow-cnae-20261013225408-1e3ef87f/b16b5cff11ea1770be3eba89201a7a17b39dc494cf53bab155a8dc5d20dbe367/conferencia_referencia.log`.

Reexecutar tarefas do mesmo run_id reutiliza os mesmos caminhos.
A proteção contra sobrescrita nessa situação permanece pendente.
O estado continua homologacao e o alerta automático permanece pendente.


## Proteção de gravação — validada em 26/10/2026

A gravação usa If-None-Match: * em envio simples. Destino existente é rejeitado pelo MinIO com HTTP 412. A tarefa falha sem substituir o objeto existente. Para refazer a carga completa, iniciar nova execução com outro run_id; preservar a carga anterior para análise.

A proteção é aplicada pelos scripts das três etapas desta base.
Não representa um bloqueio global contra gravações por outros clientes.
Cada arquivo admite até 4 GiB nesta implementação, sem multipart.
O Parquet é gerado em arquivo temporário local antes do envio.

Volumes conferidos: bronze 9179;
silver 8952; gold 4635.

Na CNAE, os quatro ZIPs originais foram conferidos por MD5 e o manifesto foi validado para a mesma carga.

Execução: `manual__protecao_20261026T213713Z_cnae_v2`.

Carga: `c0d361c7e092fc4f5577650e416088ca6410cc008333c47fa1121a2dfe21fa8d`.

Evidência: `include/evidencias_curadoria/homologacao-airflow-cnae-20261013225408-1e3ef87f/c0d361c7e092fc4f5577650e416088ca6410cc008333c47fa1121a2dfe21fa8d/conferencia_protegida.log`.

Este registro atualiza a pendência de proteção contra sobrescrita
mencionada nas seções históricas anteriores.
O estado permanece `homologacao`.
Alertas automáticos e aprovação final da curadoria continuam pendentes.


## Alertas de falha — configurados em 26/10/2026

As tarefas usam `SmtpNotifier` em `on_failure_callback`,
com a conexão Airflow `smtp_curadoria`.

Remetente e destinatário: `julia.rn.pereira@edu.ufes.br`.

O alerta informa a base, a tarefa e o run_id.
As credenciais ficam na conexão Airflow, fora deste documento.

Teste controlado: DAG `teste_alerta_curadoria`,
execução `manual__teste_smtp_curadoria_01`.
A falha intencional ocorreu e o usuário confirmou o recebimento do e-mail.

Esse teste validou o mecanismo compartilhado. A indisponibilidade total
do Airflow exige monitoramento externo.

Este registro atualiza a pendência de alertas mencionada nas seções
históricas anteriores. A base permanece em homologação,
aguardando a revisão dos critérios e a aprovação final da curadoria.


## Situação documental em 27/10/2026

Proteção contra sobrescrita validada em homologação e alertas de falha
configurados, com recebimento confirmado no teste controlado.
Menções anteriores dessas duas pendências descrevem o histórico
e foram superadas pelas evidências de 26/10/2026.

A base permanece em homologação. Este registro não constitui
aprovação para produção, publicação ou compartilhamento.


## Gold completa — validada em 27/10/2026

O processamento falha explicitamente quando encontra uma estrutura
inesperada. Antes da gravação, o resultado consolidado deve conter
exatamente as quatro classificações e suas quantidades documentadas,
totalizando 4.635 registros.

O validador exige oito colunas textuais, campos obrigatórios preenchidos
e unicidade das chaves condicionais. `cod_subclasse` deve ser nulo
no recorte CNAE 2.0 Classes e preenchido nos recortes de subclasses.

Os testes confirmaram a rejeição de classificação ausente e estrutura
inválida. Com a silver de referência, a transformação preservou os
dados da gold anteriormente aprovada.

Nova execução: `manual__gold_completa_20261027T161819Z_cnae`.
Carga: `0989e4cc1e372d3305d7e6a6bf59fc72c67315373fb1f8d64ff63694c206a817`.
Evidência: `include/evidencias_curadoria/homologacao-airflow-cnae-20261013225408-1e3ef87f/0989e4cc1e372d3305d7e6a6bf59fc72c67315373fb1f8d64ff63694c206a817/conferencia-gold-completa-20261027T162243Z-329774/resultado.json`.

Volumes: bronze 9.179; silver 8.952; gold 4.635.
Data de coleta: 27/10/2026.

A conferência posterior verificou também formatos e vínculos entre
divisão, grupo, classe e subclasse. Essa conferência posterior não deve
ser confundida com uma nova regra incorporada ao validador consolidado.

Estado mantido: `homologacao`.


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
