# Base do R (dags/utils/r_duckdb). O Makefile sobrescreve via --build-arg.
ARG BASE_IMAGE=utils-r_duckdb:4.6.1-1
FROM ${BASE_IMAGE}

# 2. Instala os pacotes R necessários para o pré-processamento
# O dplyr e stringr normalizam strings e validam os formatos do bronze
RUN R -q -e "pkgs <- c('dplyr','readr','glue','stringr'); \
             install.packages(pkgs); \
             stopifnot(all(pkgs %in% rownames(installed.packages())))"

# 3. Define onde o trabalho acontece
WORKDIR /app

# 4. Copia os arquivos necessários
# Atenção: O build deve ser rodado da RAIZ do projeto
COPY dags/base_cnae/pre_processamento/pre_processamento_cnae.R .

# 5. Execução
CMD ["Rscript", "--vanilla", "pre_processamento_cnae.R"]
