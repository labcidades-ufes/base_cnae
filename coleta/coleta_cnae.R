#!/usr/bin/env Rscript

# Coleta de homologação via arquivos oficiais IBGE.
# ESCOPO PARCIAL: CNAE 2.0 Classes e Subclasses; 2.2 e 2.3 Subclasses.
# Não substitui a cobertura histórica completa.

library(curl)
library(readxl)
library(dplyr)

if (Sys.getenv("CNAE_MODO_FTP_TESTE") != "true") {
  cat(
    "[COLETA] Coleta parcial de homologação.\n",
    "Configure CNAE_MODO_FTP_TESTE=true para executar este teste.\n"
  )
  quit(status = 1)
}

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


fontes <- data.frame(
  classificacao = c(
    "CNAE 2.3 Subclasses",
    "CNAE 2.2 Subclasses",
    "CNAE 2.0 Classes",
    "CNAE 2.0 Subclasses"
  ),
  versao_label = c(
    "cnae_2_3_subclasses",
    "cnae_2_2_subclasses",
    "cnae_2_0_classes",
    "cnae_2_0_subclasses"
  ),
  url = paste0(
    "https://ftp.ibge.gov.br/",
    "Informacoes_Gerais_e_Referencia/Classificacoes/CNAE/",
    c(
      "cnae2_3/cnae_subclasses_2_3_xls.zip",
      "cnae2_2/cnae_2_2_subclasses_xls_20150609.zip",
      "cnae2.0.zip",
      "cnae2.0_subclasses.zip"
    )
  ),
  padrao_arquivo = c(
    "Estrutura Detalhada\\.xls$",
    "estrutura_20150609\\.xls$",
    "(^|/)estrutura\\.xls$",
    "(^|/)estrutura\\.xls$"
  ),
  n_colunas = c(6L, 6L, 5L, 6L),
  volume_esperado = c(1332L, 1329L, 673L, 1301L),
  stringsAsFactors = FALSE
)

executar <- function() {
  if (!exists("upload_file_to_minio", mode = "function")) {
    stop(
      "utils.R não possui upload_file_to_minio; ",
      "precisamos ajustar a preservação dos arquivos originais."
    )
  }

  data_carga <- format(Sys.time(), "%Y%m%d")
  diretorio <- tempfile("cnae_ftp_")
  dir.create(diretorio)
  on.exit(unlink(diretorio, recursive = TRUE), add = TRUE)

  resultados <- list()
  manifestos <- list()
  originais <- list()

  for (i in seq_len(nrow(fontes))) {
    fonte <- fontes[i, ]

    cat("[COLETA] Baixando:", fonte$classificacao, "\n")

    resposta <- curl_fetch_memory(
      fonte$url,
      handle = new_handle(
        connecttimeout = 10,
        timeout = 120,
        followlocation = TRUE
      )
    )

    if (resposta$status_code != 200L) {
      stop(sprintf(
        "%s: download retornou HTTP %s.",
        fonte$classificacao, resposta$status_code
      ))
    }

    arquivo_zip <- file.path(
      diretorio,
      paste0(fonte$versao_label, ".zip")
    )
    writeBin(resposta$content, arquivo_zip)

    conteudo <- unzip(arquivo_zip, list = TRUE)

    selecionados <- conteudo$Name[
      grepl(
        fonte$padrao_arquivo,
        conteudo$Name,
        ignore.case = TRUE, useBytes = TRUE
      )
    ]

    if (length(selecionados) != 1L) {
      stop(sprintf(
        "%s: esperava uma estrutura detalhada; encontrei %s.",
        fonte$classificacao, length(selecionados)
      ))
    }

    # Extrair somente a estrutura detalhada.
    # Evita incorporar correspondências e grandes categorias.
    pasta_versao <- file.path(diretorio, fonte$versao_label)
    dir.create(pasta_versao)

    unzip(
      arquivo_zip,
      files = selecionados,
      exdir = pasta_versao
    )

    planilha <- file.path(pasta_versao, selecionados)

    if (!file.exists(planilha)) {
      stop("Planilha selecionada não foi extraída.")
    }

    abas <- excel_sheets(planilha)

    dados <- read_excel(
      planilha,
      sheet = 1,
      col_names = FALSE,
      col_types = "text"
    )

    if (nrow(dados) == 0L || ncol(dados) != fonte$n_colunas) {
      stop(sprintf(
        "%s: estrutura inesperada: %s linhas e %s colunas.",
        fonte$classificacao, nrow(dados), ncol(dados)
      ))
    }

    if (ncol(dados) == 5L) dados[[6]] <- NA_character_
    names(dados) <- paste0("c", 1:6)

    # Conferência mínima do layout, antes de gravar.
    codigos <- if (fonte$n_colunas == 5L) {
      trimws(dados$c4)
    } else {
      trimws(dados$c5)
    }

    padrao_codigo <- if (fonte$n_colunas == 5L) {
      "^[0-9]{2}\\.[0-9]{2}-[0-9]$"
    } else {
      "^[0-9]{4}-[0-9]/[0-9]{2}$"
    }
    quantidade_codigos <- sum(
      !is.na(codigos) &
        grepl(padrao_codigo, codigos)
    )

    if (quantidade_codigos != fonte$volume_esperado) {
      stop(sprintf(
        "%s: quantidade de códigos terminais diferente do manifesto.",
        fonte$classificacao
      ))
    }

    dados$versao_label <- fonte$versao_label
    dados$classificacao <- fonte$classificacao
    dados$data_coleta <- data_carga

    resultados[[fonte$versao_label]] <- dados

    destino_zip <- cnae_caminho(
      "bronze",
      paste0("fontes/", fonte$versao_label, ".zip")
    )

    originais[[i]] <- list(
      arquivo = arquivo_zip,
      destino = destino_zip
    )

    manifestos[[i]] <- data.frame(
      escopo = "homologacao_quatro_classificacoes",
      carga_id = cnae_carga_id,
      arquivo_bronze_minio = cnae_caminho("bronze", "raw_cnae.parquet"),
      classificacao = fonte$classificacao,
      url_solicitada = fonte$url,
      url_final = resposta$url,
      arquivo_zip_minio = destino_zip,
      planilha_no_zip = selecionados,
      aba = abas[1],
      data_coleta = data_carga,
      linhas_bronze = nrow(dados),
      codigos_terminais_reconhecidos = quantidade_codigos,
      md5_zip = unname(tools::md5sum(arquivo_zip)),
      stringsAsFactors = FALSE
    )

    cat(
      "[COLETA]", fonte$classificacao,
      "| linhas:", nrow(dados),
      "| códigos terminais reconhecidos:", quantidade_codigos,
      "\n"
    )
  }

  # Só prosseguir se ambas as versões forem lidas.
  if (length(resultados) != nrow(fontes)) {
    stop("Coleta incompleta em relação ao manifesto do teste.")
  }

  bronze <- bind_rows(resultados)

  manifesto <- bind_rows(manifestos)
  arquivo_manifesto <- file.path(diretorio, "manifesto.csv")

  write.csv(
    manifesto,
    arquivo_manifesto,
    row.names = FALSE,
    fileEncoding = "UTF-8"
  )

  # Preservar os ZIPs originais e a procedência no bucket do teste.
  for (original in originais) {
    curadoria_upload_exclusivo(
      original$arquivo,
      original$destino
    )
  }

  curadoria_upload_exclusivo(
    arquivo_manifesto,
    cnae_caminho("bronze", "fontes/manifesto.csv")
  )

  caminho <- cnae_caminho("bronze", "raw_cnae.parquet")

  curadoria_parquet_exclusivo(bronze, caminho)

  cat("[COLETA] Bronze gravada:", caminho, "\n")
  cat("[COLETA] Escopo: CNAE 2.0 Classes e Subclasses; 2.2 e 2.3 Subclasses.\n")
}

tryCatch(
  executar(),
  error = function(e) {
    cat("[COLETA] ERRO:", conditionMessage(e), "\n")
    quit(status = 1)
  }
)
