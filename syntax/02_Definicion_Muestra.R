# ------------------------------------------------------------------------------
# [CIMAT CONSULTORIA]  02_Definicion_Muestra.R
# 
# Autora      : Semiramis G. de la Cruz
# Revision    : 16.02.2025
# Descripcion : Se define la muestra de trabajo del proyecto de abastos, tomando
#               la muestra desde el lunes 2023-02-20 hasta el final.
#               
# ------------------------------------------------------------------------------

# --- 1. LIMPIEZA DEL ENTORNO ------------------------------------------------

rm(list=ls())
gc()
cat("\14") # Limpiar consola
cat("\n")

# --- 2. RUTA INICIAL Y DIRECTORIOS ------------------------------------------

# Detectar ruta del script, compatible con terminal y RStudio
args <- commandArgs(trailingOnly = FALSE)
script_path <- sub("--file=", "", args[grep("--file=", args)])

if (length(script_path) == 0) {  
  # Si se ejecuta en RStudio, usar rstudioapi
  if ("rstudioapi" %in% installed.packages()) {
    script_path <- rstudioapi::getActiveDocumentContext()$path
  } else {
    stop("⚠️ No se puede determinar la ruta del script.")
  }
}

SYNTAXDIR <- dirname(script_path)
PATH <- dirname(SYNTAXDIR)  
DATADIR <-paste0(PATH, "/data/") 
setwd(PATH)

# --- 3. INSTALACIÓN Y CARGA DE LIBRERÍAS -------------------------------------
ipak <- function(pkg){
  new.pkg <- pkg[!(pkg %in% installed.packages()[, "Package"])]
  if (length(new.pkg)) install.packages(new.pkg, dependencies = TRUE)
  sapply(pkg, require, character.only = TRUE)
}

# Paquetes necesarios
packages <- c("Hmisc", "lubridate", "dplyr", "tidyverse", "janitor",
              "readxl", "xlsx", "RCT", "openxlsx", "data.table", "rstudioapi")

ipak(packages)


# --- 4. DEFINICIÓN DE FUNCIONES --------------------------------------------

# Filtrar por rango de fechas
filtrar_por_fecha <- function(data, fecha_inicio, fecha_fin) {
  return(data[Fecha >= as.IDate(fecha_inicio) & Fecha <= as.IDate(fecha_fin)])
}

# Transformar data de fechas-fila a fechas-columna
transformar_data_completa <- function(data, fecha_inicio, fecha_fin) {
  data_filtrada <- filtrar_por_fecha(data, fecha_inicio, fecha_fin)
  todas_fechas <- seq(as.IDate(fecha_inicio), as.IDate(fecha_fin), by = "day")
  
  data_wide <- dcast(data_filtrada, Loc + Sku ~ Fecha, value.var = "Uni",
                     fun.aggregate = sum, fill = 0)
  
  # Verificar fechas faltantes
  fechas_presentes <- setdiff(names(data_wide), c("Loc", "Sku"))
  fechas_faltantes <- setdiff(as.character(todas_fechas), fechas_presentes)
  
  if (length(fechas_faltantes) > 0) {
    cat("⚠️ Faltan las siguientes fechas en la base transformada:\n", 
        fechas_faltantes, "\n")
    
    for (fecha in fechas_faltantes) {
      data_wide[[fecha]] <- 0
    }
    
    setcolorder(data_wide, c("Loc", "Sku", as.character(todas_fechas)))
  }
  
  return(data_wide)
}

# Agrupar datos diarios en semanales
agrupar_a_semanas <- function(data) {
  columnas_fechas <- setdiff(names(data), c("Loc", "Sku"))
  fechas <- as.Date(columnas_fechas, format = "%Y-%m-%d")
  semanas <- paste0("Semana_", isoweek(fechas), "_", year(fechas))
  
  data_semanal <- data[, .(Loc, Sku)]
  
  for (semana in unique(semanas)) {
    cols_semana <- columnas_fechas[semanas == semana]
    data_semanal[[semana]] <- rowSums(data[, ..cols_semana], na.rm = TRUE)
  }
  
  return(data_semanal)
}

# --- 5. FUNCIÓN PRINCIPAL --------------------------------------------------

procesar_abastos <- function() {
  cat("\n📌 Procesando datos de abastos...\n")
  
  start_total <- Sys.time()
  
  # Carga de datos
  cat("\n🔄 Cargando datos...\n")
  start <- Sys.time()
  Data_Base_Abastos <- fread(paste0(DATADIR, "00_Datos_Modelar.txt"), sep = "|")
  end <- Sys.time()
  cat("⏱ Tiempo de carga:", 
      round(difftime(end, start, units = "secs"), 2), "segundos\n")
  
  # Fechas para Train y Test
  fecha_train_inicio <- "2023-02-20"
  fecha_train_fin <- "2024-02-18"
  fecha_test_inicio <- "2024-02-19"
  fecha_test_fin <- "2024-05-12"
  
  # Transformación de fechas para Train
  cat("\n🔄 Transformando datos para Train...\n")
  start <- Sys.time()
  Data_Base_Train_Cols <- transformar_data_completa(Data_Base_Abastos, 
                                                    fecha_train_inicio, 
                                                    fecha_train_fin)
  end <- Sys.time()
  cat("⏱ Tiempo de transformación (Train):", 
      round(difftime(end, start, units = "secs"), 2), "segundos\n")
  
  # Transformación de fechas para Test
  cat("\n🔄 Transformando datos para Test...\n")
  start <- Sys.time()
  Data_Base_Test_Cols <- transformar_data_completa(Data_Base_Abastos, 
                                                   fecha_test_inicio, 
                                                   fecha_test_fin)
  end <- Sys.time()
  cat("⏱ Tiempo de transformación (Test):", 
      round(difftime(end, start, units = "secs"), 2), "segundos\n")
  
  # Agrupación en semanas para Train
  cat("\n🔄 Agrupando datos en semanas para Train...\n")
  start <- Sys.time()
  Data_Base_Train_Semanal <- agrupar_a_semanas(Data_Base_Train_Cols)
  end <- Sys.time()
  cat("⏱ Tiempo de agrupación (Train):", 
      round(difftime(end, start, units = "secs"), 2), "segundos\n")
  
  # Agrupación en semanas para Test
  cat("\n🔄 Agrupando datos en semanas para Test...\n")
  start <- Sys.time()
  Data_Base_Test_Semanal <- agrupar_a_semanas(Data_Base_Test_Cols)
  end <- Sys.time()
  cat("⏱ Tiempo de agrupación (Test):", 
      round(difftime(end, start, units = "secs"), 2), "segundos\n")
  
  # Guardado de archivos
  cat("\n💾 Guardando archivos Train y Test...\n")
  start <- Sys.time()
  output_train <- paste0(DATADIR, "Data_Base_Abastos_Train.csv")
  output_test <- paste0(DATADIR, "Data_Base_Abastos_Test.csv")
  fwrite(Data_Base_Train_Semanal, output_train)
  fwrite(Data_Base_Test_Semanal, output_test)
  end <- Sys.time()
  cat("⏱ Tiempo de guardado:", 
      round(difftime(end, start, units = "secs"), 2), "segundos\n")
  
  total_end <- Sys.time()
  cat("\n✅ Procesamiento finalizado en", 
      round(difftime(total_end, start_total, units = "secs"), 2), "segundos.\n")
  cat("📂 Archivos guardados en:\n- Train:", output_train, "\n- Test:", 
      output_test, "\n")
  
  return(list(Train = Data_Base_Train_Semanal, Test = Data_Base_Test_Semanal))
}

Data_Base_Abastos <- procesar_abastos()


