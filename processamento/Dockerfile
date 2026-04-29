# 1. Usa a base que você acabou de buildar com sucesso
FROM utils-r_duckdb:latest

# 2. Instala os pacotes R necessários para o processamento
# O stringi e stringr limpam e normalizam os textos para o gold
RUN R -e "install.packages(c('dplyr','stringi','stringr','glue','tidyr'), repos='https://cloud.r-project.org/')"

# 3. Define onde o trabalho acontece
WORKDIR /app

# 4. Copia os arquivos necessários
# Atenção: O build deve ser rodado da RAIZ do projeto
COPY dags/utils/utils.R .
COPY dags/base_cnae/processamento/processamento_cnae.R .

# 5. Execução
CMD ["Rscript", "--vanilla", "processamento_cnae.R"]
