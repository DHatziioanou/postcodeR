#' Retrieve ONS postcode data and append to object
#'
#' Retrieves Office for National Statistics (ONS) data by postcode and appends it as added columns to an input file. For information on ONS databases see https://geoportal.statistics.gov.uk/datasets/national-statistics-postcode-lookup-february-2020
#'
#' @param dt data with postcodes for ONS information retrieval
#'
#' @param query_column name of column within dt containing postcodes to retrieve as a string
#'
#' @param Index index object created using index_postcodes()
#'
#' @param desired_columns name of ONS columns to retrieve. Default is "all"; note this excludes 2019 utla and ltla "stp19", "ccg19" geographies which are retrieved from a different database.
#' For UTLA and LTLA "utla", "ltla" can be listed and the 2019 geographies (names and codes) will be retrieved based on the database "laua" from  https://geoportal.statistics.gov.uk/datasets/lower-tier-local-authority-to-upper-tier-local-authority-april-2019-lookup-in-england-and-wales (2019 version).
#' For the 2020 STP and CCGs  lsoa11, "stp20", "ccg20" can be listed and the 2020 geographies (names and codes) will be retrieved based on the database "lsoa11" from https://geoportal.statistics.gov.uk/datasets/lsoa-2011-to-clinical-commissioning-groups-to-sustainability-and-transformation-partnerships-april-2020-lookup-in-england/data . Herefordshire CCG name is updated as per https://digital.nhs.uk/services/organisation-data-service/change-summary---stp-reconfiguration
#'
#' @return Returns the original dt object as a data.frame with added columns of ONS data
#'
#'
#' @author Diane Hatziioanou
#'
#' @keywords postcode, ONS, NSPL, Office for National Statistics
#'
#' @examples
#' Index <- fread(file.path("C:/.../NSPL_FEB_2019_UK/Data/Index.csv"))
#'
#' data_with_ONS <- match_postcode(data, "Postcode", Index, "all")
#'
#' data_with_ONS <- match_postcode(data, "Postcode", Index, c("laua", "ccg"))
#'
#' @export
match_postcode <- function(dt, query_column, Index, desired_columns){
  start_time <- Sys.time()
  options(datatable.fread.dec.experiment = FALSE)
  if (!require("data.table")) {
    install.packages("data.table")
    library(data.table)
  }
  if (!require("dplyr")) {
    install.packages("dplyr")
    library(dplyr)
  }
  if (!require("lubridate")) {
    install.packages("lubridate")
    library(lubridate)
  }
  
  setDF(dt)
  col_order <- names(dt)
  # Check dabatase has desired columns
  ONS_columns <- as.character(fread(Index$full_path[1], header = F, nrows = 1, stringsAsFactors = F, fill = T))

  if ("all" %in% desired_columns) {
    desired_columns <- ONS_columns
  } else {
    problem_columns <- desired_columns[!(desired_columns %in% c(ONS_columns, "utla", "ltla", "stp20", "ccg20", "PHEC19CD", "PHEREG19CD"))]
    if (length(problem_columns) != 0) {
      cat(problem_columns)
      stop(paste("The database doesn't have the desired columns above. Try with ones it has!"))
    }
  }

  # sort by first letters and first number
  setDT(dt)[,Postcode_match := toupper(gsub(" ", "", dt[,get(query_column)]))]
  dt[, Pc := (gsub('([a-zA-Z])([0-9])', '\\1_\\2', dt$Postcode_match))]
  dt[, Pc := (gsub( "_.*$", "",  dt$Pc))]
  dt[, Pc_N := (substr(gsub('\\D+','', dt[,get(query_column)]), 1,1))]
  dt[, Pc_s := paste0(dt$Pc,dt$Pc_N)]
  dt[, Pc_s := toupper(dt$Pc_s)]
  dt[, Pc_s := factor(dt$Pc_s)]
  dt[, c("Pc","Pc_N") := NULL]
  data.table::setorder(dt, cols = Pc_s, na.last = T)

  # Change any problematic Date columns to character format
  Date_columns <- (colnames(dt)[grepl("Date", sapply(dt,class))])
  if (isTRUE(length(Date_columns) > 0)){
    dt[, (Date_columns) := lapply(.SD, as.character), .SDcols = Date_columns]
  }

  # Retrieve goegraphies from postcodes
  for (i in seq_along(levels(dt$Pc_s))) {
    # Subset data by postcode
    temp_dt <- setDF(dt)[dt$Pc_s == levels(dt$Pc_s)[i],]

    # Give ONS data to postcodes in database
    if (levels(dt$Pc_s)[i] %in% Index$Pc_s) {

      # Retrieve ONS postcode geographies
      Pc_ONS <- fread(input = file.path(Index[Index$Pc_s == levels(dt$Pc_s)[i],"full_path"]),
                      header = F,
                      sep = "auto",
                      skip = as.numeric(Index[Index$Pc_s == levels(dt$Pc_s)[i],"start"] - 1),
                      nrows = as.numeric(Index[Index$Pc_s == levels(dt$Pc_s)[i],"stop"] - Index[Index$Pc_s == levels(dt$Pc_s)[i],"start"] + 1))
      # Make postcodes matchable
      Pc_ONS[,Postcode_match := toupper(gsub(" ", "", Pc_ONS$V1))]

      # Add ONS geographies to matched data
      temp_dt <- merge(temp_dt,Pc_ONS, by = "Postcode_match", all.x = T, all.y = F)

    } else {
      # Give NA to postcodes not in database
      setDT(temp_dt)[, colnames(fread(Index$full_path[1], header = F, nrows = 1, stringsAsFactors = F, fill = T)) := character(.N) ]
    }
    # IF MERGED DATASET EXISTS, MERGE
    if (exists("dt_geocoded", where = -1) & dim(temp_dt)[1] != 0 ) {
      dt_geocoded <- rbindlist(list(dt_geocoded, temp_dt), use.names = T, fill = T)
    }
    # IF MERGED DATASET DOESN'T EXIST, CREATE IT
    if (!exists("dt_geocoded", where = -1)) {
      dt_geocoded <- copy(temp_dt)
    }
    if (!exists("Pc_ONS")) {
      remove(temp_dt)
    } else{
      remove(temp_dt,Pc_ONS)
    }
  }

  setcolorder(setDT(dt_geocoded), c(col_order, paste0("V", 1:41)))

  # Add column names
  col_names <- as.character(fread(Index$full_path[1], header = F, nrows = 1, stringsAsFactors = F, fill = T))
  colnames(dt_geocoded)[((ncol(dt_geocoded) - (NROW(col_names)) - 1)):(ncol(dt_geocoded) - 2)] <- col_names

  # Remove undesired column
  setDT(dt_geocoded)[, c(ONS_columns[!(ONS_columns %in% c(desired_columns, "laua","lsoa11")) &
                                       ONS_columns %in% names(dt_geocoded)],
                         "Pc_s", "Postcode_match") := NULL]

# Convert previous Date columns back to Date format  
  if (isTRUE(length(Date_columns) > 0)) {
    dt_geocoded[, (Date_columns) := lapply(.SD, function(x)  as.Date(lubridate::parse_date_time(x, orders = c("dmy", "mdy", "ymd")))), .SDcols = Date_columns]
  }
  
  
  # Add UTLA - LTLA 2020 data if "utla", "ltla in desired_columns
  if (isTRUE(sum(c("utla", "ltla") %in% desired_columns) > 0)) {
    UTLA_LTLA <-  fread(file.path("https://opendata.arcgis.com/datasets/3e4f4af826d343349c13fb7f0aa2a307_0.csv"))
    if (sum(c("utla", "ltla") %in% desired_columns) == 2) {
      get_columns <- c( "LTLA19CD", "LTLA19NM", "UTLA19CD", "UTLA19NM")
    } else if ("ltla" %in% desired_columns) {
      get_columns <- c("LTLA19CD", "LTLA19NM")
    } else{
      get_columns <- c("LTLA19CD","UTLA19CD", "UTLA19NM")
    }
    dt_geocoded <- merge(dt_geocoded, setDF(UTLA_LTLA)[, get_columns], by.x = "laua", by.y = "LTLA19CD", all.x = T, all.y = F)
  }
  # Add CCG - STP 2020 data if "stp20", "ccg20" in desired_columns
  if (isTRUE(sum(c("stp20", "ccg20") %in% desired_columns) > 0)) {
    if (sum(c("stp20", "ccg20") %in% desired_columns) == 2) {
      get_columns <- c("LSOA11CD","CCG20CD", "CCG20NM",  "STP20CD",  "STP20NM")
    } else if ("stp20" %in% desired_columns) {
      get_columns <- c("LSOA11CD","STP20CD",  "STP20NM")
    } else{
      get_columns <- c("LSOA11CD","CCG20CD", "CCG20NM")
    }
    UTLA2011_CCG_STP2020 <-  fread(file.path("https://opendata.arcgis.com/datasets/1631beea57ff4e9fb90d75f9c764ce26_0.csv"))
    UTLA2011_CCG_STP2020$CCG20NM[grepl("Herefordshire ", UTLA2011_CCG_STP2020$CCG20NM, ignore.case = T)] <- "NHS Herefordshire and Worcestershire CCG"
    dt_geocoded <- merge(dt_geocoded, setDF(UTLA2011_CCG_STP2020)[, get_columns], by.x = c("lsoa11"), by.y = c("LSOA11CD"), all.x = T, all.y = F)
  }

  # PHE Centre data if "PHEC19CD", "PHEREG19CD" in desired_columns
  if (isTRUE(sum(c("PHEC19CD", "PHEREG19CD") %in% desired_columns) > 0)) {
    if (sum(c("PHEC19CD", "PHEREG19CD") %in% desired_columns) == 2) {
      get_columns <- c("LAD19CD","PHEC19CD","PHEC19NM", "PHEREG19CD", "PHEREG19NM")
    } else if ("PHEC19CD" %in% desired_columns) {
      get_columns <- c("LAD19CD","PHEC19CD","PHEC19NM")
    } else{
      get_columns <- c("LAD19CD", "PHEREG19CD", "PHEREG19NM")
    }
    LAD19_to_PHE19 <-  fread(file.path("C:/Users/Diane.Hatziioanou/Documents/postcodeR/LAD19_to_PHE19.csv"))
    dt_geocoded <- merge(dt_geocoded, setDF(LAD19_to_PHE19)[, get_columns], by.x = c("laua"), by.y = c("LAD19CD"), all.x = T, all.y = F)
  }

  setcolorder(setDT(dt_geocoded), c(col_order, names(dt_geocoded)[!(names(dt_geocoded) %in% col_order)]))

  end_time <- Sys.time()
  cat("Run time:")
  print(end_time - start_time)
  message(paste("Postcodes searched:",sum(!is.na((dt_geocoded[,get(query_column)])))))

  new_col <- copy(dt_geocoded[, (ncol(dt) - 1 ):ncol(dt_geocoded)])
  new_col[new_col == ""] <- NA

  not_retrieved <- sum(rowSums(is.na(new_col)) == ncol(new_col))
  found_rows <- sum(rowSums(is.na(new_col)) != ncol(new_col))
  no_postcode <- NROW(dt[is.na(dt[,(query_column)]),])
  message(paste("Rows matched:", found_rows))
  message(paste("Rows with no postcode:", no_postcode))
  message(paste("Rows with postcode not in NSPL:",not_retrieved - no_postcode))
  message(paste("Total rows not matched:",not_retrieved))

  if (isTRUE(length(Date_columns) > 0)) {
    warning("Columns changed to character format and back to Date again: ", list(Date_columns))
  }

  return(as.data.frame(dt_geocoded))
  rm(list = ls())
  gc(verbose = F, full = T)
}
                                           
