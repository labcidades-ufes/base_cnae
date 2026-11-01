#!/usr/bin/env Rscript
# ==============================================================================
# PROCESSAMENTO — BASE CNAE (Gold)
# Lê silver, aplica transformações por tipo de versão (fill hierárquico,
# filtro de subclasses, limpeza de texto) e salva gold.
# Também filtra, a partir da própria gold, as subclasses CNAE classificadas
# como "Economia do Mar" por lista oficial curada (embutida abaixo — sem
# depender de nenhum arquivo enviado manualmente em tempo de execução).
#
# Fonte da lista de economia do mar: [FONTE A CONFIRMAR — substituir esta
# linha pela citação completa do documento/base de origem]
#
# Entrada:  silver/base_cnae/silver_cnae_YYYYMMDD.parquet
# Saída:    gold/base_cnae/gold_cnae_YYYYMMDD.parquet
#           gold/base_cnae/cnaes.csv           (nome fixo, sempre atual)
#           gold/base_cnae/cnaes_mar_YYYYMMDD.parquet
#           gold/base_cnae/cnaes_mar.csv       (nome fixo, sempre atual)
# Schema:   versao_cnae | cod_secao | cod_divisao | cod_grupo | cod_classe |
#           cod_subclasse | nome_subclasse | nome_original
# ==============================================================================
library(dplyr)
library(stringi)
library(stringr)
library(glue)
library(readr)
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
# 3. SALVAR GOLD — parquet (histórico) + cnaes.csv (nome fixo)
# ------------------------------------------------------------------------------
save_gold <- function(data) {
  tryCatch({
    filepath <- cnae_caminho("gold", "gold_cnae.parquet")
    curadoria_parquet_exclusivo(data, filepath)
    cat("[GOLD] Salvo em:", filepath, "\n")
    filepath
    filepath <- sprintf("gold/base_cnae/gold_cnae_%s.parquet", format(Sys.time(), "%Y%m%d"))
    write_parquet_to_minio(data, filepath)
    cat("[GOLD] Parquet salvo em:", filepath, "\n")

    filepath_csv <- "gold/base_cnae/cnaes.csv"
    arquivo_csv_local <- tempfile(fileext = ".csv")
    readr::write_csv(data, arquivo_csv_local, na = "")
    sucesso <- upload_file_to_minio(local_file = arquivo_csv_local, path = filepath_csv)
    unlink(arquivo_csv_local)
    if (!isTRUE(sucesso)) stop("Falha ao enviar cnaes.csv para o MinIO")
    cat("[GOLD] CSV salvo em:", filepath_csv, "\n")

    list(parquet = filepath, csv = filepath_csv)
  }, error = function(e) {
    cat("[GOLD] Erro ao salvar:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}

# ------------------------------------------------------------------------------
# 4. SUBCLASSES CNAE DA ECONOMIA DO MAR (lista curada, embutida)
# ------------------------------------------------------------------------------
# Lista fixa de subclasses CNAE (nível mais granular — 7 dígitos, mesmo grão
# do cod_subclasse da gold) classificadas por macrossetor da economia do mar,
# com a flag `proximidade` indicando se a subclasse só conta como economia do
# mar quando o estabelecimento está fisicamente próximo da costa (TRUE) ou
# conta sempre, independente de localização (FALSE).
#
# Embutida como texto CSV (delimitador ";") e parseada abaixo — evita risco
# de erro de transcrição manual linha a linha. Para atualizar esta lista no
# futuro, edite só este bloco de texto — nenhuma outra parte do pipeline muda.
CNAES_ECONOMIA_DO_MAR_CSV <- "cnae;scn;macrossetor;nome;proximidade
0311601;0280;Pesca;Pesca de peixes em água salgada;FALSE
0311602;0280;Pesca;Pesca de crustáceos e moluscos em água salgada;FALSE
0311603;0280;Pesca;Coleta de outros produtos marinhos;FALSE
0311604;0280;Pesca;Atividades de apoio à pesca em água salgada;FALSE
0321301;0280;Pesca;Criação de peixes em água salgada e salobra;FALSE
0321302;0280;Pesca;Criação de camarões em água salgada e salobra;FALSE
0321303;0280;Pesca;Criação de ostras e mexilhões em água salgada e salobra;FALSE
0321304;0280;Pesca;Criação de peixes ornamentais em água salgada e salobra;FALSE
0321305;0280;Pesca;Atividades de apoio à aqüicultura em água salgada e salobra;FALSE
0321399;0280;Pesca;Cultivos e semicultivos da aqüicultura em água salgada e salobra não especificados anteriormente;FALSE
1020101;1000;Pesca;Preservação de peixes, crustáceos e moluscos;FALSE
1020102;1000;Pesca;Fabricação de conservas de peixes, crustáceos e moluscos;FALSE
4634603;4500;Pesca;Comércio atacadista de pescados e frutos do mar;FALSE
4722902;4500;Pesca;Peixaria;FALSE
0600002;0580;Extrativa Mineral;Extração e beneficiamento de xisto;FALSE
0810004;0580;Extrativa Mineral;Extração de calcário e dolomita e beneficiamento associado;FALSE
0810005;0580;Extrativa Mineral;Extração de gesso e caulim;FALSE
0810006;0580;Extrativa Mineral;Extração de areia, cascalho ou pedregulho e beneficiamento associado;FALSE
0810007;0580;Extrativa Mineral;Extração de argila e beneficiamento associado;FALSE
0810008;0580;Extrativa Mineral;Extração de saibro e beneficiamento associado;FALSE
0810009;0580;Extrativa Mineral;Extração de basalto e beneficiamento associado;FALSE
0810099;0580;Extrativa Mineral;Extração e britamento de pedras e outros materiais para construção e beneficiamento associado;FALSE
0892401;0580;Extrativa Mineral;Extração de sal marinho;FALSE
0892402;0580;Extrativa Mineral;Extração de salgema;FALSE
0892403;0580;Extrativa Mineral;Refino e outros tratamentos do sal;FALSE
0893200;0580;Extrativa Mineral;Extração de gemas (pedras preciosas e semipreciosas);FALSE
0600001;0680;Óleo, Gás e Energia;Extração de petróleo e gás natural;FALSE
0910600;0680;Óleo, Gás e Energia;Atividades de apoio à extração de petróleo e gás natural;FALSE
2851800;2500;Óleo, Gás e Energia;Fabricação de máquinas e equipamentos para a prospecção e extração de petróleo, peças e acessórios;FALSE
3314714;2500;Óleo, Gás e Energia;Manutenção e reparação de máquinas e equipamentos para a prospecção e extração de petróleo;FALSE
7119702;6900;Óleo, Gás e Energia;Atividades de estudos geológicos;FALSE
7739001;7800;Óleo, Gás e Energia;Aluguel de máquinas e equipamentos para extração de minérios e petróleo, sem operador;FALSE
3511501;3500;Óleo, Gás e Energia;Geração de energia elétrica;FALSE
3011301;2900;Construção Naval;Construção de embarcações de grande porte;FALSE
3011302;2900;Construção Naval;Construção de embarcações para uso comercial e para usos especiais, exceto de grande porte;FALSE
3012100;2900;Construção Naval;Construção de embarcações para esporte e lazer;FALSE
3317101;2900;Construção Naval;Manutenção e reparação de embarcações e estruturas flutuantes;FALSE
3317102;2900;Construção Naval;Manutenção e reparação de embarcações para esporte e lazer;FALSE
4763605;4500;Construção Naval;Comércio varejista de embarcações e outros veículos recreativos, peças e acessórios;FALSE
5011401;4900;Portos;Transporte marítimo de cabotagem  Carga;FALSE
5011402;4900;Portos;Transporte marítimo de cabotagem  passageiros;FALSE
5012201;4900;Portos;Transporte marítimo de longo curso  Carga;FALSE
5012202;4900;Portos;Transporte marítimo de longo curso  Passageiros;FALSE
5021101;4900;Portos;Transporte por navegação interior de carga, municipal, exceto travessias;FALSE
5021102;4900;Portos;Transporte por navegação interior de carga, intermunicipal, interestadual e internacional, exceto travessia;FALSE
5022001;4900;Portos;Transporte por navegação interior de passageiros em linhas regulares, municipal, exceto travessia;FALSE
5022002;4900;Portos;Transporte por navegação interior de passageiros em linhas regulares, intermunicipal, interestadual e internacional, exceto travessia;FALSE
5030101;5280;Portos;Navegação de apoio marítimo;FALSE
5030102;5280;Portos;Navegação de apoio portuário;FALSE
5091201;4900;Portos;Transporte por navegação de travessia, municipal;FALSE
5091202;4900;Portos;Transporte por navegação de travessia, intermunicipal;FALSE
5099899;4900;Portos;Outros transportes aquaviários não especificados anteriormente;FALSE
4291000;4180;Portos;Obras portuárias, marítimas e fluviais;FALSE
5231101;5280;Portos;Administração da infraestrutura portuária;FALSE
5231102;5280;Portos;Atividades do Operador Portuário;FALSE
5231103;5280;Portos;Gestão de terminais aquaviários;FALSE
5030103;5280;Portos;Serviço de rebocadores e empurradores;FALSE
5239701;5280;Portos;Serviços de praticagem;FALSE
5239799;5280;Portos;Atividades auxiliares dos transportes aquaviários não especificadas anteriormente;FALSE
5232000;5280;Portos;Atividades de agenciamento marítimo;FALSE
7719501;7800;Portos;Locação de embarcações sem tripulação, exceto para fins recreativos;FALSE
5510801;5500;Turismo;Hotéis;TRUE
5510802;5500;Turismo;Aparthotéis;TRUE
5510803;5500;Turismo;Motéis;TRUE
5590601;5500;Turismo;Albergues, exceto assistenciais;TRUE
5590602;5500;Turismo;Campings;TRUE
5590603;5500;Turismo;Pensões (alojamento);TRUE
5590699;5500;Turismo;Outros alojamentos não especificados anteriormente;TRUE
5611201;5500;Turismo;Restaurantes e similares;TRUE
5611203;5500;Turismo;Lanchonetes, casas de chá, de sucos e similares;TRUE
5611204;5500;Turismo;Bares e outros estabelecimentos especializados em servir bebidas, sem entretenimento;TRUE
5611205;5500;Turismo;Bares e outros estabelecimentos especializados em servir bebidas, com entretenimento;TRUE
5612100;5500;Turismo;Serviços ambulantes de alimentação;TRUE
5620101;5500;Turismo;Fornecimento de alimentos preparados preponderantemente para empresas;TRUE
5620102;5500;Turismo;Serviços de alimentação para eventos e recepções  bufê;TRUE
5620103;5500;Turismo;Cantinas  serviços de alimentação privativos;TRUE
5620104;5500;Turismo;Fornecimento de alimentos preparados preponderantemente para consumo domiciliar;TRUE
4929903;4900;Turismo;Organização de excursões em veículos rodoviários próprios, municipal;TRUE
4929904;4900;Turismo;Organização de excursões em veículos rodoviários próprios, intermunicipal, interestadual e internacional;TRUE
4950700;4900;Turismo;Trens turísticos, teleféricos e similares;TRUE
7911200;7800;Turismo;Agências de viagens;TRUE
7912100;7800;Turismo;Operadores turísticos;TRUE
8591100;8592;Turismo;Ensino de esportes;TRUE
9312300;9080;Turismo;Clubes sociais, esportivos e similares;TRUE
9319101;9080;Turismo;Produção e promoção de eventos esportivos;TRUE
5099801;4900;Turismo;Transporte aquaviário para passeios turísticos;TRUE
9329899;9080;Turismo;Outras atividades de recreação e lazer não especificadas anteriormente;TRUE
7721700;7800;Turismo;Aluguel de equipamentos recreativos e esportivos;TRUE"

cnaes_economia_do_mar <- readr::read_delim(
  CNAES_ECONOMIA_DO_MAR_CSV, delim = ";", col_types = "ccccl", trim_ws = TRUE
) |>
  rename(descricao_cnaes_mar = nome)

# Normaliza códigos para comparação tolerante a formatação
# (gold usa "0311-6/01"; a lista curada usa "0311601" — ambos viram dígitos).
normalizar_cod_subclasse <- function(x) gsub("[^0-9]", "", x)

# Filtra a gold consolidada pelas subclasses da lista curada.
filtrar_cnaes_mar <- function(gold_cnae) {
  cat("[CNAES_MAR] Filtrando gold pelas subclasses da lista de economia do mar...\n")
  tryCatch({
    if (!"cod_subclasse" %in% names(gold_cnae)) {
      stop("Coluna 'cod_subclasse' ausente na gold — não é possível filtrar economia do mar")
    }

    subclasses_alvo <- normalizar_cod_subclasse(cnaes_economia_do_mar$cnae)

    cnaes_mar <- gold_cnae |>
      mutate(.chave = normalizar_cod_subclasse(cod_subclasse)) |>
      filter(.chave %in% subclasses_alvo) |>
      left_join(
        cnaes_economia_do_mar |>
          mutate(.chave = normalizar_cod_subclasse(cnae)) |>
          select(.chave, scn, macrossetor, descricao_cnaes_mar, proximidade),
        by = ".chave"
      ) |>
      select(-.chave) |>
      distinct(cod_subclasse, versao_cnae, .keep_all = TRUE)

    if (nrow(cnaes_mar) == 0) {
      stop("Nenhum registro casou com a lista de economia do mar — verifique o formato de cod_subclasse")
    }

    subclasses_encontradas <- n_distinct(normalizar_cod_subclasse(cnaes_mar$cod_subclasse))
    cat(glue(
      "[CNAES_MAR] {nrow(cnaes_mar)} registros | {subclasses_encontradas} de ",
      "{nrow(cnaes_economia_do_mar)} subclasses da lista de economia do mar encontradas na gold.\n"
    ))

    if (subclasses_encontradas < nrow(cnaes_economia_do_mar)) {
      faltantes <- setdiff(subclasses_alvo, unique(normalizar_cod_subclasse(cnaes_mar$cod_subclasse)))
      cat("[CNAES_MAR] AVISO: subclasses da lista não encontradas na gold (versão de CNAE pode divergir):\n")
      print(cnaes_economia_do_mar |> filter(normalizar_cod_subclasse(cnae) %in% faltantes))
    }

    cnaes_mar
  }, error = function(e) {
    cat("[CNAES_MAR] Erro ao filtrar economia do mar:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}

# ------------------------------------------------------------------------------
# 5. SALVAR CNAES_MAR — parquet (histórico) + cnaes_mar.csv (nome fixo)
# ------------------------------------------------------------------------------
save_cnaes_mar <- function(data) {
  tryCatch({
    filepath <- sprintf("gold/base_cnae/cnaes_mar_%s.parquet", format(Sys.time(), "%Y%m%d"))
    write_parquet_to_minio(data, filepath)
    cat("[CNAES_MAR] Parquet salvo em:", filepath, "\n")

    filepath_csv <- "gold/base_cnae/cnaes_mar.csv"
    arquivo_csv_local <- tempfile(fileext = ".csv")
    readr::write_csv(data, arquivo_csv_local, na = "")
    sucesso <- upload_file_to_minio(local_file = arquivo_csv_local, path = filepath_csv)
    unlink(arquivo_csv_local)
    if (!isTRUE(sucesso)) stop("Falha ao enviar cnaes_mar.csv para o MinIO")
    cat("[CNAES_MAR] CSV salvo em:", filepath_csv, "\n")

    list(parquet = filepath, csv = filepath_csv)
  }, error = function(e) {
    cat("[CNAES_MAR] Erro ao salvar:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}

silver      <- read_silver()
gold        <- generate_products(silver)
saida_gold  <- save_gold(gold)
cnaes_mar   <- filtrar_cnaes_mar(gold)
saida_mar   <- save_cnaes_mar(cnaes_mar)
cat(glue("[GOLD] Finalizado: {nrow(gold)} registros | {n_distinct(gold$versao_cnae)} versões\n"))
cat(glue("[CNAES_MAR] Finalizado: {nrow(cnaes_mar)} registros na economia do mar\n"))