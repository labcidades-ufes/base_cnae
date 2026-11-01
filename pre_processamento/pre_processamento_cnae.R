#!/usr/bin/env Rscript
# ==============================================================================
# PRÉ-PROCESSAMENTO — BASE CNAE (Silver)
# Lê bronze, normaliza strings, remove cabeçalhos residuais e valida formatos.
# Entrada:  bronze/base_cnae/raw_cnae_YYYYMMDD.parquet
# Saída:    silver/base_cnae/silver_cnae_YYYYMMDD.parquet
# ==============================================================================
library(dplyr)
library(stringr)
library(glue)
source("utils.R")

# PROTECAO_GRAVACAO_CONDICIONAL_V1
curadoria_upload_exclusivo <- function(local_file, path, bucket = NULL) {
  if (!requireNamespace("aws.s3", quietly = TRUE)) {
    stop("Pacote aws.s3 ausente.")
  }

  if (is.null(bucket)) bucket <- Sys.getenv("MINIO_BUCKET")

  configuracao <- c(
    bucket = bucket,
    endpoint = Sys.getenv("MINIO_ENDPOINT"),
    acesso = Sys.getenv("MINIO_ACCESS_KEY"),
    segredo = Sys.getenv("MINIO_SECRET_KEY")
  )

  if (anyNA(configuracao) || any(!nzchar(configuracao))) {
    stop("Configuração MinIO incompleta.")
  }

  if (length(path) != 1L || is.na(path) || !nzchar(path)) {
    stop("Caminho de destino inválido.")
  }

  info <- file.info(local_file)
  if (nrow(info) != 1L || is.na(info$size) || isTRUE(info$isdir)) {
    stop("Arquivo local inexistente ou inválido.")
  }

  # Limite conservador para este envio simples, sem multipart.
  if (info$size > 4 * 1024^3) {
    stop("Arquivo acima de 4 GiB; envio simples cancelado.")
  }

  ssl <- tolower(Sys.getenv("MINIO_USE_SSL", "false"))
  if (!ssl %in% c("true", "false")) {
    stop("MINIO_USE_SSL inválido.")
  }

  Sys.setenv(
    AWS_ACCESS_KEY_ID = configuracao[["acesso"]],
    AWS_SECRET_ACCESS_KEY = configuracao[["segredo"]],
    AWS_S3_ENDPOINT = configuracao[["endpoint"]],
    AWS_DEFAULT_REGION = "",
    AWS_REGION = ""
  )

  resultado <- tryCatch(
    aws.s3::put_object(
      file = local_file,
      object = path,
      bucket = bucket,
      multipart = FALSE,
      headers = list("If-None-Match" = "*"),
      use_https = identical(ssl, "true"),
      region = "",
      show_progress = FALSE
    ),
    error = function(e) {
      stop(
        "[GRAVACAO BLOQUEADA OU FALHOU] ", path, ": ",
        conditionMessage(e),
        ". Se o destino já existe, preserve a carga e use uma nova execução.",
        call. = FALSE
      )
    }
  )

  if (!isTRUE(resultado)) {
    stop("MinIO não confirmou a gravação: ", path)
  }

  cat("[GRAVACAO EXCLUSIVA] Criado:", path, "\n")
  invisible(TRUE)
}

curadoria_parquet_exclusivo <- function(
    data, path, bucket = NULL, compression = "zstd") {

  arquivo <- tempfile(fileext = ".parquet")
  on.exit(unlink(arquivo), add = TRUE)

  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbWriteTable(con, "dados_curadoria", data, overwrite = TRUE)

  sql <- paste0(
    "COPY dados_curadoria TO ",
    DBI::dbQuoteString(con, arquivo),
    " (FORMAT PARQUET, COMPRESSION ",
    DBI::dbQuoteString(con, compression),
    ")"
  )

  DBI::dbExecute(con, sql)
  curadoria_upload_exclusivo(arquivo, path, bucket)
  invisible(NULL)
}
# FIM_PROTECAO_GRAVACAO_CONDICIONAL_V1


# Referência obrigatória e compartilhada pelas etapas da mesma execução.
cnae_carga_id <- Sys.getenv("CNAE_CARGA_ID")

if (!grepl("^[a-f0-9]{64}$", cnae_carga_id)) {
  stop("CNAE_CARGA_ID ausente ou inválido; esperados 64 caracteres hexadecimais.")
}


# CAMINHOS_DATADOS_CURADORIA_V1
cnae_caminho <- function(camada, arquivo) {
  if (!camada %in% c("bronze", "silver", "gold")) {
    stop("Camada CNAE inválida.")
  }

  # Os arquivos auxiliares preservam sua identificação por carga.
  if (!grepl("\\.parquet$", arquivo)) {
    return(sprintf(
      "%s/base_cnae/cargas/%s/%s",
      camada, cnae_carga_id, arquivo
    ))
  }

  referencia <- Sys.getenv("CNAE_DATA_REFERENCIA")
  data <- suppressWarnings(as.Date(referencia, format = "%Y%m%d"))
  if (length(data) != 1L || is.na(data) ||
      !identical(format(data, "%Y%m%d"), referencia)) {
    stop("DATA_REFERENCIA_INVALIDA")
  }

  permitidos <- c(
    bronze = "raw_cnae.parquet",
    silver = "silver_cnae.parquet",
    gold = "gold_cnae.parquet"
  )
  if (!identical(arquivo, unname(permitidos[[camada]]))) {
    stop("Arquivo CNAE diferente do contrato.")
  }

  sprintf(
    "%s/base_cnae/%s_%s.parquet",
    camada, sub("\\.parquet$", "", arquivo), referencia
  )
}


cat("[CARGA] Referência:", cnae_carga_id, "\n")

Sys.setlocale("LC_ALL", "C.UTF-8")

# ------------------------------------------------------------------------------
# 1. LEITURA DO BRONZE
# ------------------------------------------------------------------------------
read_from_minio_duckdb <- function() {
  cat("[PRE_PROCESSAMENTO] Lendo bronze...\n")
  tryCatch({
    caminho <- cnae_caminho("bronze", "raw_cnae.parquet")
    cat(glue("[PRE_PROCESSAMENTO] Lendo: {caminho}\n"))
    read_parquet_from_minio(caminho)
  }, error = function(e) {
    cat("[PRE_PROCESSAMENTO] Erro ao ler bronze:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}

# ------------------------------------------------------------------------------
# 2. PRÉ-PROCESSAMENTO + VALIDAÇÕES
# ------------------------------------------------------------------------------
process_data <- function(data) {
  tryCatch({
    esperadas <- c(
      paste0("c", 1:6),
      "versao_label", "classificacao", "data_coleta"
    )

    if (is.null(data) || nrow(data) == 0L) {
      stop("Bronze vazia.")
    }

    if (!setequal(names(data), esperadas)) {
      stop("Esquema bronze diferente do contrato.")
    }

    registros <- as_tibble(data) |>
      mutate(
        across(c1:c6, ~ na_if(trimws(as.character(.x)), "")),
        versao_label = as.character(versao_label),
        classificacao = as.character(classificacao),
        data_coleta = as.character(data_coleta)
      )

    manifesto <- c(
      "CNAE 2.0 Classes" = 673L,
      "CNAE 2.0 Subclasses" = 1301L,
      "CNAE 2.2 Subclasses" = 1329L,
      "CNAE 2.3 Subclasses" = 1332L
    )

    if (anyNA(registros$classificacao) ||
        !setequal(unique(registros$classificacao), names(manifesto))) {
      stop("Classificações diferentes do manifesto de quatro estruturas.")
    }

    if (anyNA(registros$data_coleta) ||
        any(!grepl("^[0-9]{8}$", registros$data_coleta))) {
      stop("Data de coleta ausente ou fora do formato AAAAMMDD.")
    }

    # Remover somente cabeçalhos reconhecidos.
    # Não aplicar regex de subclasse à denominação das classes em c5.
    cabecalho <- (
      !is.na(registros$c4) &
        tolower(registros$c4) == "classe"
    ) | (
      !is.na(registros$c5) &
        tolower(registros$c5) == "subclasse"
    )

    registros <- registros[!cabecalho, ]

    for (versao in names(manifesto)) {
      parte <- registros |>
        filter(classificacao == versao)

      somente_classes <- versao == "CNAE 2.0 Classes"
      codigos <- if (somente_classes) parte$c4 else parte$c5

      padrao <- if (somente_classes) {
        "^[0-9]{2}\\.[0-9]{2}-[0-9]$"
      } else {
        "^[0-9]{4}-[0-9]/[0-9]{2}$"
      }

      candidatos <- !is.na(codigos) &
        grepl("[0-9]", codigos)

      validos <- !is.na(codigos) &
        grepl(padrao, codigos)

      if (any(candidatos & !validos)) {
        stop(paste("Código terminal fora do formato:", versao))
      }

      if (sum(validos) != manifesto[[versao]]) {
        stop(paste("Quantidade de códigos diferente do manifesto:", versao))
      }

      cat(
        "[QUALIDADE] OK:", versao,
        "| códigos terminais:", sum(validos), "\n"
      )
    }

    cat(
      "[SILVER] Entrada:", nrow(data),
      "| saída:", nrow(registros),
      "| cabeçalhos removidos:", sum(cabecalho), "\n"
    )

    registros
  }, error = function(e) {
    cat("[SILVER] ERRO:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}

# ------------------------------------------------------------------------------
# 3. SALVAR SILVER
# ------------------------------------------------------------------------------
save_to_minio_duckdb <- function(data) {
  tryCatch({
    filepath <- cnae_caminho("silver", "silver_cnae.parquet")
    cat(glue("[PRE_PROCESSAMENTO] Salvando em: {filepath}\n"))
    curadoria_parquet_exclusivo(data, filepath)
    filepath
  }, error = function(e) {
    cat("[PRE_PROCESSAMENTO] Erro ao salvar:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}

bronze <- read_from_minio_duckdb()
silver <- process_data(bronze)
saida  <- save_to_minio_duckdb(silver)
cat("[PRE_PROCESSAMENTO] Finalizado:", saida, "\n")