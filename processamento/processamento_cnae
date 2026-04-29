#!/usr/bin/env Rscript
# ==============================================================================
# PROCESSAMENTO — BASE CNAE (Gold)
# Lê silver, aplica transformações por tipo de versão (fill hierárquico,
# filtro de subclasses, limpeza de texto) e salva gold.
# Entrada:  silver/base_cnae/silver_cnae_YYYYMMDD.parquet
# Saída:    gold/base_cnae/gold_cnae_YYYYMMDD.parquet
# Schema:   versao_cnae | cod_secao | cod_divisao | cod_grupo | cod_classe |
#           cod_subclasse | nome_subclasse | nome_original
# ==============================================================================
library(dplyr)
library(stringi)
library(stringr)
library(glue)
source("utils.R")
Sys.setlocale("LC_ALL", "C.UTF-8")

# ------------------------------------------------------------------------------
# Utilitários
# ------------------------------------------------------------------------------
limpar_texto <- function(x) {
  x |> stri_trans_general("Latin-ASCII") |> str_to_upper() |> str_squish()
}

# 6 colunas: Seção | Divisão | Grupo | Classe | Subclasse | Denominação
processar_v2x <- function(df, versao) {
  df |>
    rename(cod_secao=c1, cod_divisao=c2, cod_grupo=c3,
           cod_classe=c4, cod_subclasse=c5, nome_original=c6) |>
    mutate(across(c(cod_secao, cod_divisao, cod_grupo, cod_classe), ~ na_if(.x, ""))) |>
    tidyr::fill(cod_secao, cod_divisao, cod_grupo, cod_classe, .direction = "down") |>
    filter(!is.na(cod_subclasse), str_detect(cod_subclasse, "\\d")) |>
    mutate(nome_subclasse = limpar_texto(nome_original), versao_cnae = versao) |>
    select(versao_cnae, cod_secao, cod_divisao, cod_grupo, cod_classe,
           cod_subclasse, nome_subclasse, nome_original)
}

# 5 colunas: Seção | Divisão | Grupo | Classe | Denominação (sem subclasse)
processar_v_classes <- function(df, versao) {
  df |>
    rename(cod_secao=c1, cod_divisao=c2, cod_grupo=c3,
           cod_classe=c4, nome_original=c5) |>
    mutate(across(c(cod_secao, cod_divisao, cod_grupo), ~ na_if(.x, ""))) |>
    tidyr::fill(cod_secao, cod_divisao, cod_grupo, .direction = "down") |>
    filter(!is.na(cod_classe), str_detect(cod_classe, "\\d")) |>
    mutate(cod_subclasse = NA_character_,
           nome_subclasse = limpar_texto(nome_original),
           versao_cnae = versao) |>
    select(versao_cnae, cod_secao, cod_divisao, cod_grupo, cod_classe,
           cod_subclasse, nome_subclasse, nome_original)
}

# 4 colunas: Grupo | Classe | Subclasse | Denominação (Domiciliar 2.0)
processar_domiciliar_2x <- function(df, versao) {
  df |>
    rename(cod_grupo=c1, cod_classe=c2, cod_subclasse=c3, nome_original=c4) |>
    mutate(across(c(cod_grupo, cod_classe), ~ na_if(.x, ""))) |>
    tidyr::fill(cod_grupo, cod_classe, .direction = "down") |>
    filter(!is.na(cod_subclasse), str_detect(cod_subclasse, "\\d")) |>
    mutate(cod_secao = NA_character_, cod_divisao = NA_character_,
           nome_subclasse = limpar_texto(nome_original),
           versao_cnae = versao) |>
    select(versao_cnae, cod_secao, cod_divisao, cod_grupo, cod_classe,
           cod_subclasse, nome_subclasse, nome_original)
}

# 2 colunas: Código | Denominação (Domiciliar original)
processar_domiciliar_orig <- function(df, versao) {
  df |>
    rename(cod_subclasse=c1, nome_original=c2) |>
    filter(!is.na(cod_subclasse), str_detect(cod_subclasse, "\\d")) |>
    mutate(cod_secao=NA_character_, cod_divisao=NA_character_,
           cod_grupo=NA_character_, cod_classe=NA_character_,
           nome_subclasse = limpar_texto(nome_original),
           versao_cnae = versao) |>
    select(versao_cnae, cod_secao, cod_divisao, cod_grupo, cod_classe,
           cod_subclasse, nome_subclasse, nome_original)
}

# ------------------------------------------------------------------------------
# 1. LEITURA DO SILVER
# ------------------------------------------------------------------------------
read_silver <- function() {
  cat("[GOLD] Lendo silver...\n")
  tryCatch({
    arquivos <- list_parquet_files_in_minio("silver/base_cnae/")
    if (length(arquivos) == 0) stop("Nenhum arquivo silver encontrado.")
    caminho <- sub(sprintf("^s3://%s/", Sys.getenv("MINIO_BUCKET", "airflow")), "",
                   sort(arquivos, decreasing = TRUE)[1])
    cat(glue("[GOLD] Lendo: {caminho}\n"))
    read_parquet_from_minio(caminho)
  }, error = function(e) {
    cat("[GOLD] Erro ao ler silver:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}

# ------------------------------------------------------------------------------
# 2. TRANSFORMAÇÃO
# ------------------------------------------------------------------------------
generate_products <- function(silver) {
  tryCatch({
    cat(glue("[GOLD] Processando {n_distinct(silver$versao_label)} versões...\n"))

    versoes <- unique(silver$versao_label)
    resultados <- list()

    for (label in versoes) {
      df  <- silver |> filter(versao_label == label) |> select(c1:c6)
      ver <- unique(silver$classificacao[silver$versao_label == label])[1]

      # Conta colunas não-NA para roteamento
      n_cols <- sum(sapply(paste0("c", 1:6), function(col) !all(is.na(df[[col]]))))

      resultado <- if (grepl("domiciliar_2", label)) {
        processar_domiciliar_2x(df, ver)
      } else if (grepl("domiciliar", label)) {
        processar_domiciliar_orig(df, ver)
      } else if (n_cols >= 6) {
        processar_v2x(df, ver)
      } else if (n_cols == 5) {
        processar_v_classes(df, ver)
      } else {
        cat(glue("[GOLD] {ver}: {n_cols} colunas — pulando.\n"))
        NULL
      }

      if (!is.null(resultado) && nrow(resultado) > 0) {
        resultados[[label]] <- resultado
        cat(glue("[GOLD] {ver}: {nrow(resultado)} registros\n"))
      }
    }

    if (length(resultados) == 0) stop("Nenhuma versão processada.")
    bind_rows(resultados)
  }, error = function(e) {
    cat("[GOLD] Erro na transformação:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}

# ------------------------------------------------------------------------------
# 3. SALVAR GOLD
# ------------------------------------------------------------------------------
save_gold <- function(data) {
  tryCatch({
    filepath <- sprintf("gold/base_cnae/gold_cnae_%s.parquet", format(Sys.time(), "%Y%m%d"))
    write_parquet_to_minio(data, filepath)
    cat("[GOLD] Salvo em:", filepath, "\n")
    filepath
  }, error = function(e) {
    cat("[GOLD] Erro ao salvar:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}

silver  <- read_silver()
gold    <- generate_products(silver)
saida   <- save_gold(gold)
cat(glue("[GOLD] Finalizado: {nrow(gold)} registros | {n_distinct(gold$versao_cnae)} versões\n"))
