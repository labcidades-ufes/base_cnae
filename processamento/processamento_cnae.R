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
# Utilitários
# ------------------------------------------------------------------------------
limpar_texto <- function(x) {
  x |> stri_trans_general("Latin-ASCII") |> str_to_upper() |> str_squish()
}

# 6 colunas: Seção | Divisão | Grupo | Classe | Subclasse | Denominação
processar_v2x <- function(df, versao) {
  # Reconhecer apenas códigos nas colunas hierárquicas.
  # Títulos e marcadores de continuação não são códigos.
  codigo_valido <- function(x, padrao) {
    x <- trimws(as.character(x))
    valido <- !is.na(x) & str_detect(x, padrao)
    x[!valido] <- NA_character_
    x
  }

  resultado <- df |>
    rename(
      cod_secao = c1,
      cod_divisao = c2,
      cod_grupo = c3,
      cod_classe = c4,
      cod_subclasse = c5,
      nome_original = c6
    ) |>
    mutate(
      cod_secao = codigo_valido(cod_secao, "^[A-U]$"),
      cod_divisao = codigo_valido(cod_divisao, "^\\d{2}$"),
      cod_grupo = codigo_valido(cod_grupo, "^\\d{2}\\.\\d$"),
      cod_classe = codigo_valido(cod_classe, "^\\d{2}\\.\\d{2}-\\d$"),
      cod_subclasse = trimws(cod_subclasse)
    ) |>
    tidyr::fill(
      cod_secao, cod_divisao, cod_grupo, cod_classe,
      .direction = "down"
    ) |>
    filter(
      !is.na(cod_subclasse),
      str_detect(cod_subclasse, "^\\d{4}-\\d/\\d{2}$")
    ) |>
    mutate(
      nome_subclasse = limpar_texto(nome_original),
      versao_cnae = versao
    )

  # Correção documental específica da CNAE 2.2.
  # A estrutura do XLS repete /01 para corte e dobra.
  # As notas explicativas oficiais da edição 2015 usam /02:
  # https://ftp.ibge.gov.br/Informacoes_Gerais_e_Referencia/
  # Classificacoes/CNAE/cnae2_2/cnae2_2_subclasses_20150609.pdf
  if (versao == "CNAE 2.2 Subclasses") {
    alvo <- which(
      resultado$nome_subclasse ==
        "SERVICO DE CORTE E DOBRA DE METAIS"
    )

    if (length(alvo) != 1L) {
      stop("CNAE 2.2: registro de corte e dobra ausente ou ambíguo.")
    }

    codigo_anterior <- resultado$cod_subclasse[alvo]

    if (!codigo_anterior %in% c("2599-3/01", "2599-3/02")) {
      stop("CNAE 2.2: código inesperado para corte e dobra.")
    }

    if (codigo_anterior == "2599-3/01") {
      resultado$cod_subclasse[alvo] <- "2599-3/02"
      cat(
        "[CURADORIA] CNAE 2.2: corte e dobra corrigido",
        "de 2599-3/01 para 2599-3/02;",
        "bronze preservada.\n"
      )
    }
  }

  if (nrow(resultado) == 0L) {
    stop("Nenhuma subclasse válida após transformação.")
  }

  obrigatorias <- c(
    "cod_secao", "cod_divisao", "cod_grupo",
    "cod_classe", "cod_subclasse", "nome_subclasse"
  )

  for (coluna in obrigatorias) {
    valores <- resultado[[coluna]]
    if (any(is.na(valores) | trimws(valores) == "")) {
      stop(paste("Campo obrigatório ausente na gold:", coluna))
    }
  }

  if (anyDuplicated(
    resultado[c("versao_cnae", "cod_subclasse")]
  )) {
    stop("Duplicidade de versão e subclasse na gold.")
  }

  esperado <- switch(
    versao,
    "CNAE 2.0 Subclasses" = 1301L,
    "CNAE 2.2 Subclasses" = 1329L,
    "CNAE 2.3 Subclasses" = 1332L,
    NA_integer_
  )

  if (!is.na(esperado) && nrow(resultado) != esperado) {
    stop(sprintf(
      "%s: esperadas %s subclasses; encontradas %s.",
      versao, esperado, nrow(resultado)
    ))
  }

  resultado |>
    select(
      versao_cnae, cod_secao, cod_divisao,
      cod_grupo, cod_classe, cod_subclasse,
      nome_subclasse, nome_original
    )
}

# 5 colunas: Seção | Divisão | Grupo | Classe | Denominação (sem subclasse)
processar_v_classes <- function(df, versao) {
  codigo_valido <- function(x, padrao) {
    x <- trimws(as.character(x))
    valido <- !is.na(x) & str_detect(x, padrao)
    x[!valido] <- NA_character_
    x
  }

  resultado <- df |>
    rename(
      cod_secao = c1,
      cod_divisao = c2,
      cod_grupo = c3,
      cod_classe = c4,
      nome_original = c5
    ) |>
    mutate(
      cod_secao = codigo_valido(cod_secao, "^[A-U]$"),
      cod_divisao = codigo_valido(cod_divisao, "^\\d{2}$"),
      cod_grupo = codigo_valido(cod_grupo, "^\\d{2}\\.\\d$"),
      cod_classe = codigo_valido(cod_classe, "^\\d{2}\\.\\d{2}-\\d$")
    ) |>
    tidyr::fill(
      cod_secao, cod_divisao, cod_grupo,
      .direction = "down"
    ) |>
    filter(!is.na(cod_classe)) |>
    mutate(
      cod_subclasse = NA_character_,
      nome_subclasse = limpar_texto(nome_original),
      versao_cnae = versao
    ) |>
    select(
      versao_cnae, cod_secao, cod_divisao,
      cod_grupo, cod_classe, cod_subclasse,
      nome_subclasse, nome_original
    )

  for (coluna in c(
    "cod_secao", "cod_divisao", "cod_grupo",
    "cod_classe", "nome_subclasse"
  )) {
    valores <- resultado[[coluna]]
    if (any(is.na(valores) | trimws(valores) == "")) {
      stop(paste("Campo obrigatório ausente:", versao, coluna))
    }
  }

  if (anyDuplicated(resultado[c("versao_cnae", "cod_classe")])) {
    stop(paste("Classe duplicada:", versao))
  }

  if (versao == "CNAE 2.0 Classes" && nrow(resultado) != 673L) {
    stop("CNAE 2.0 Classes: quantidade diferente de 673.")
  }

  resultado
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
    caminho <- cnae_caminho("silver", "silver_cnae.parquet")
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

# VALIDACAO_GOLD_COMPLETA_CNAE_V1
validar_gold_completa_cnae <- function(data) {
  exigir <- function(condicao, codigo) {
    if (!isTRUE(condicao)) stop(paste("[QUALIDADE]", codigo))
  }

  manifesto <- c(
    "CNAE 2.0 Classes" = 673L,
    "CNAE 2.0 Subclasses" = 1301L,
    "CNAE 2.2 Subclasses" = 1329L,
    "CNAE 2.3 Subclasses" = 1332L
  )
  campos <- c(
    "versao_cnae", "cod_secao", "cod_divisao", "cod_grupo",
    "cod_classe", "cod_subclasse", "nome_subclasse", "nome_original"
  )

  exigir(is.data.frame(data) && nrow(data) > 0L, "GOLD_VAZIA")
  exigir(
    length(names(data)) == length(campos) &&
    !anyDuplicated(names(data)) &&
    setequal(names(data), campos),
    "ESQUEMA_GOLD"
  )
  exigir(all(vapply(data, is.character, logical(1))), "TIPOS_GOLD")

  obrigatorios <- setdiff(campos, "cod_subclasse")
  for (campo in obrigatorios) {
    exigir(
      !anyNA(data[[campo]]) && all(nzchar(trimws(data[[campo]]))),
      paste0("CAMPO_OBRIGATORIO:", campo)
    )
  }

  exigir(
    setequal(unique(data$versao_cnae), names(manifesto)),
    "CLASSIFICACOES_GOLD"
  )

  for (versao in names(manifesto)) {
    parte <- data[data$versao_cnae == versao, , drop = FALSE]
    exigir(nrow(parte) == manifesto[[versao]], "VOLUME_CLASSIFICACAO")

    if (versao == "CNAE 2.0 Classes") {
      exigir(all(is.na(parte$cod_subclasse)), "SUBCLASSE_EM_CLASSES")
      chave <- parte$cod_classe
    } else {
      exigir(
        !anyNA(parte$cod_subclasse) &&
        all(nzchar(trimws(parte$cod_subclasse))),
        "SUBCLASSE_AUSENTE"
      )
      chave <- parte$cod_subclasse
    }
    exigir(!anyDuplicated(chave), "CHAVE_DUPLICADA")
  }

  cat("[QUALIDADE] Gold completa: quatro classificações e 4.635 registros.\n")
  invisible(TRUE)
}

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
        stop(glue("[GOLD] {ver}: estrutura inesperada com {n_cols} colunas."))
      }

      if (!is.null(resultado) && nrow(resultado) > 0) {
        resultados[[label]] <- resultado
        cat(glue("[GOLD] {ver}: {nrow(resultado)} registros\n"))
      }
    }

    if (length(resultados) == 0) stop("Nenhuma versão processada.")
    consolidado <- bind_rows(resultados)
    validar_gold_completa_cnae(consolidado)
    consolidado
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
    filepath <- cnae_caminho("gold", "gold_cnae.parquet")
    curadoria_parquet_exclusivo(data, filepath)
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
