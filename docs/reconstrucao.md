# Reconstrução — base_cnae

Na raiz do projeto datalake, executar:

    make build-dags CURADORIA=1

Requisitos: Docker, Python 3 e acesso aos repositórios de imagens e pacotes R.

O comando utiliza scripts/build_curadoria.py e build/curadoria.json. Constrói a imagem base e as seis imagens CNAE/CNEFE. São necessários os arquivos compartilhados dags/utils/utils.R e dags/utils/r_duckdb/Dockerfile; o repositório isolado da base não contém todas essas dependências.

Registro da construção: include/curadoria_reconstrucao_20261029T223311Z-769901.json.
Evidência: include/evidencias_curadoria/reconstrucao-20261029T231945Z-090221/resultado.json.

Construção, SHA-256 dos scripts embarcados, dependências identificadas e consulta local no DuckDB conferidos.

Esta conferência não executou os pipelines nem acessou o MinIO. O build não altera automaticamente as imagens das DAGs. As referências anteriormente testadas permanecem configuradas.

Conexões e credenciais são configurações externas ao código. Não incluir credenciais no repositório.

Estado mantido: homologacao.
