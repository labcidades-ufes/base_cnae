# Base do R (dags/utils/r_duckdb). O Makefile sobrescreve via --build-arg.
ARG BASE_IMAGE=utils-r_duckdb:4.6.1-1
FROM ${BASE_IMAGE}

# 2. Instala os pacotes R necessários para a coleta
# O rvest e httr2 fazem a raspagem; readxl lê os XLS; os demais tratam os dados
RUN R -q -e "pkgs <- c('readxl','readr','dplyr','glue'); \
             install.packages(pkgs); \
             stopifnot(all(pkgs %in% rownames(installed.packages())))"

# 3. Define onde o trabalho acontece
WORKDIR /app

# 4. Copia os arquivos necessários
# Atenção: O build deve ser rodado da RAIZ do projeto
COPY dags/base_cnae/coleta/coleta_cnae.R .

# 5. Execução
CMD ["Rscript", "coleta_cnae.R"]
