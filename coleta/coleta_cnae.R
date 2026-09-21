#!/usr/bin/env Rscript
# ==============================================================================
# COLETA — BASE CNAE
# Baixa diretamente do FTP público do IBGE os arquivos XLS de estrutura
# detalhada (subclasses) das versões da CNAE, sem depender de scraping da
# página do CONCLA (que bloqueia acesso automatizado com HTTP 403).
#
# Fonte: https://ftp.ibge.gov.br/Informacoes_Gerais_e_Referencia/Classificacoes/CNAE/
# Cada entrada de CNAE_FONTES é um .zip contendo um .xls/.xlsx com a
# estrutura detalhada de uma versão da CNAE-Subclasses.
#
# Saída: bronze/base_cnae/raw_cnae_YYYYMMDD.parquet
# Schema: versao_label | classificacao | c1 | c2 | c3 | c4 | c5 | c6 | data_coleta
# ==============================================================================
library(readxl)
library(dplyr)
library(readr)
library(glue)
source("utils.R")
Sys.setlocale("LC_ALL", "C.UTF-8")
options(timeout = max(600, getOption("timeout")))

CNAE_FTP_BASE <- "https://ftp.ibge.gov.br/Informacoes_Gerais_e_Referencia/Classificacoes/CNAE/"

# ------------------------------------------------------------------------------
# FONTES — lista fixa de versões a baixar. Para adicionar uma versão nova
# no futuro (ex.: CNAE 2.4), só acrescentar uma linha aqui — nenhuma outra
# parte do script muda. URLs confirmadas manualmente no índice do FTP.
# ------------------------------------------------------------------------------
CNAE_FONTES <- list(
  list(
    label = "cnae_subclasses_2_0",
    classificacao = "CNAE-Subclasses 2.0",
    url = paste0(CNAE_FTP_BASE, "cnae2.0_subclasses.zip")
  ),
  list(
    label = "cnae_subclasses_2_2",
    classificacao = "CNAE-Subclasses 2.2",
    url = paste0(CNAE_FTP_BASE, "cnae2_2/cnae_2_2_subclasses_xls_20150609.zip")
  ),
  list(
    label = "cnae_subclasses_2_3",
    classificacao = "CNAE-Subclasses 2.3",
    url = paste0(CNAE_FTP_BASE, "cnae2_3/cnae_subclasses_2_3_xls.zip")
  )
)

# ------------------------------------------------------------------------------
# 1. DOWNLOAD + LEITURA — cada versão vira linhas no bronze
# ------------------------------------------------------------------------------
baixar_e_ler_versao <- function(fonte) {
  nome  <- fonte$classificacao
  label <- fonte$label
  url   <- fonte$url

  tmp_zip <- tempfile(fileext = ".zip")
  tmp_dir <- tempfile("cnae_")
  dir.create(tmp_dir, recursive = TRUE, showWarnings = FALSE)
  on.exit({
    if (file.exists(tmp_zip)) unlink(tmp_zip)
    if (dir.exists(tmp_dir)) unlink(tmp_dir, recursive = TRUE, force = TRUE)
  }, add = TRUE)

  ok <- tryCatch({
    download.file(url, tmp_zip, mode = "wb", quiet = TRUE, method = "libcurl")
    file.exists(tmp_zip) && !is.na(file.info(tmp_zip)$size) && file.info(tmp_zip)$size > 0
  }, error = function(e) {
    cat(glue("[COLETA] Falha ao baixar {nome}: {conditionMessage(e)}\n"))
    FALSE
  })
  if (!isTRUE(ok)) return(NULL)

  conteudo_zip <- unzip(tmp_zip, list = TRUE)$Name
  arquivos_excel <- conteudo_zip[grepl("\\.(xls|xlsx)$", conteudo_zip, ignore.case = TRUE)]
  if (length(arquivos_excel) == 0) {
    cat(glue("[COLETA] {nome}: nenhum XLS/XLSX encontrado dentro do ZIP\n"))
    return(NULL)
  }

  unzip(tmp_zip, files = arquivos_excel[1], exdir = tmp_dir)
  arquivo_local <- file.path(tmp_dir, arquivos_excel[1])

  raw <- tryCatch(
    read_excel(arquivo_local, sheet = 1, col_types = "text"),
    error = function(e) {
      cat(glue("[COLETA] Falha ao ler {nome}: {conditionMessage(e)}\n"))
      NULL
    }
  )
  if (is.null(raw) || nrow(raw) == 0) return(NULL)

  # Normaliza para 6 colunas posicionais (preenche com NA se houver menos)
  n <- ncol(raw)
  df <- raw[, seq_len(min(n, 6)), drop = FALSE]
  if (n < 6) for (j in seq(n + 1, 6)) df[[paste0("c", j)]] <- NA_character_
  names(df) <- paste0("c", 1:6)

  df$versao_label  <- label
  df$classificacao <- nome
  df$data_coleta   <- format(Sys.time(), "%Y%m%d")

  cat(glue("[COLETA] {nome}: {nrow(df)} linhas\n"))
  df
}

collect_data <- function() {
  tryCatch({
    cat(glue("[COLETA] Baixando {length(CNAE_FONTES)} versões CNAE do FTP do IBGE\n"))

    lista <- list()
    for (fonte in CNAE_FONTES) {
      df <- baixar_e_ler_versao(fonte)
      if (!is.null(df)) lista[[fonte$label]] <- df
    }

    if (length(lista) == 0) stop("Nenhuma versão coletada com sucesso.")
    bind_rows(lista)
  }, error = function(e) {
    cat("[COLETA] Erro:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}

# ------------------------------------------------------------------------------
# 2. SALVAR NO MINIO
# ------------------------------------------------------------------------------
save_to_minio_duckdb <- function(data) {
  tryCatch({
    timestamp <- format(Sys.time(), "%Y%m%d")
    filepath  <- sprintf("bronze/base_cnae/raw_cnae_%s.parquet", timestamp)
    write_parquet_to_minio(data, filepath)
    filepath
  }, error = function(e) {
    cat("[COLETA] Erro ao salvar:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}

dados   <- collect_data()
arquivo <- save_to_minio_duckdb(dados)
cat("[COLETA] Finalizado:", arquivo, "\n")
cat(glue("[COLETA] {nrow(dados)} linhas | {n_distinct(dados$versao_label)} versões\n"))