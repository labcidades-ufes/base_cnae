# 1. Usa a base que você acabou de buildar com sucesso
FROM utils-r_duckdb:latest

# 2. Instala os pacotes R necessários para a coleta
# O rvest e httr2 fazem a raspagem; readxl lê os XLS; os demais tratam os dados
RUN R -e "install.packages(c('rvest','httr2','readxl','dplyr','readr','glue','stringr'), repos='https://cloud.r-project.org/')"

# 3. Define onde o trabalho acontece
WORKDIR /app

# 4. Copia os arquivos necessários
# Atenção: O build deve ser rodado da RAIZ do projeto
COPY dags/utils/utils.R .
COPY dags/base_cnae/coleta/coleta_cnae.R .

# 5. Execução
CMD ["Rscript", "coleta_cnae.R"]
