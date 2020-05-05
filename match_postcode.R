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
#' @param retrieve_columns name of ONS columns to retrieve. Default is "all"; note this excludes 2019 utla and ltla "stp19", "ccg19" geographies which are retrieved from a different database. 
#' For UTLA and LTLA "utla", "ltla" can be listed and the 2019 geographies (names and codes) will be retrieved based on the database "laua" from  https://geoportal.statistics.gov.uk/datasets/lower-tier-local-authority-to-upper-tier-local-authority-april-2019-lookup-in-england-and-wales (2019 version).
#' For the 2020 STP and CCGs  lsoa11, "stp20", "ccg20" can be listed and the 2020 geographies (names and codes) will be retrieved based on the database "lsoa11" from https://geoportal.statistics.gov.uk/datasets/lsoa-2011-to-clinical-commissioning-groups-to-sustainability-and-transformation-partnerships-april-2020-lookup-in-england/data . Herefordshire CCG name is updated as per https://digital.nhs.uk/services/organisation-data-service/change-summary---stp-reconfiguration
#'
#' @return Returns the original dt object with added columns of ONS data
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
  options(datatable.fread.dec.experiment=FALSE)
  if (!require("data.table")) {
    install.packages("data.table")
    library(data.table)
  }
  if (!require("dplyr")) {
    install.packages("dplyr")
    library(dplyr)
  }
  setDF(dt)

# Check dabatase has desired columns
ONS_columns <- as.character(fread(Index$full_path[1], header = F, nrows = 1, stringsAsFactors = F, fill = T))

if("all" %in% desired_columns){
  desired_columns <- ONS_columns
  } else {
    problem_columns <- desired_columns[!(desired_columns %in% c(ONS_columns, "utla", "ltla", "stp20", "ccg20"))]
      if(length(problem_columns) != 0){
      cat(problem_columns)
      stop(paste("The database doesn't have the desired columns above. Try with ones it has!"))
      }
    }

# sort by first letters and first number
setDT(dt)[,Pc := gsub(" ", "", dt[,get(query_column)])]
dt[, Pc := (gsub('([a-zA-Z])([0-9])', '\\1_\\2', dt$Pc))]
dt[, Pc := (gsub( "_.*$", "",  dt$Pc))]
dt[, Pc_N := (substr(gsub('\\D+','', dt[,get(query_column)]), 1,1))]
dt[, Pc_s:= paste0(dt$Pc,dt$Pc_N)]
dt[, Pc_s:= toupper(dt$Pc_s)]
dt[, Pc_s:= factor(dt$Pc_s)]
dt[, c("Pc","Pc_N"):=NULL]
setorder(dt, cols = Pc_s, na.last=T)

# Change any problematic Date columns to character format
Date_columns <- (colnames(dt)[grepl("Date", sapply(dt,class))])
if(isTRUE(length(Date_columns) >0)){
dt[,Date_columns] <- apply(setDF(dt)[,Date_columns], 2, function(x) as.character(x))
}


# Retrieve postcode geography
for(i in seq_along(levels(dt$Pc_s))){
  # Subset data by postcode
  temp_dt <- setDF(dt)[dt$Pc_s == levels(dt$Pc_s)[i],]

  # Give ONS data to postcodes in database
  if(levels(dt$Pc_s)[i] %in% Index$Pc_s){

  # Retrieve ONS postcode geographies
  Pc <- fread(input = file.path(Index[Index$Pc_s == levels(dt$Pc_s)[i],"full_path"]),
                  header = F,
                  sep = "auto",
                  skip = as.numeric(Index[Index$Pc_s == levels(dt$Pc_s)[i],"start"]-1),
                  nrows = as.numeric(Index[Index$Pc_s == levels(dt$Pc_s)[i],"stop"] -Index[Index$Pc_s == levels(dt$Pc_s)[i],"start"] +1))
  # Make postcodes matchable
  setDT(temp_dt)[, Postcode_match := toupper(gsub(" ", "", temp_dt[,get(query_column)]))]
  Pc[,Postcode_match := toupper(gsub(" ", "", Pc$V1))]

  # Add ONS geographies to matched data
  temp_dt <- merge(temp_dt,Pc, by = "Postcode_match", all.x = T, all.y = F)

  } else {
    # Give NA to postcodes not in database
    col_names <- colnames(fread(Index$full_path[1], header = F, nrows = 1, stringsAsFactors = F, fill = T))
        setDT(temp_dt)[, `:=`(c(col_names),"Not in ONS")]
  }

  # IF MERGED DATASET EXISTS, MERGE
  if (exists("dt_geocoded") & dim(temp_dt)[1] !=0 ){
    dt_geocoded <- rbindlist(list(dt_geocoded, temp_dt), use.names = T, fill = T)
  }
  # IF MERGED DATASET DOESN'T EXIST, CREATE IT
  if (!exists("dt_geocoded")){
    dt_geocoded <- temp_dt
  }
  remove(temp_dt,Pc)
}


# # Add back lines without postcodes
# dt_no_postcode <- dt[is.na(dt$postcode),]
# col_names <- colnames(fread(Index$full_path[1], header = F, nrows = 1, stringsAsFactors = F, fill = T))
# setDT(dt_no_postcode)[, `:=`(c(col_names),"No postcode")]
# dt_geocoded <- rbind(dt_geocoded, dt_no_postcode, fill=TRUE, stringsAsFactors = FALSE)

dt_geocoded <- setDF(dt_geocoded)[dt_geocoded$Postcode_match != TRUE,]
setDT(dt_geocoded)[,c("Postcode_match","Pc_s") := NULL] 

# Add column names
col_names <- as.character(fread(Index$full_path[1], header = F, nrows = 1, stringsAsFactors = F, fill = T))
colnames(dt_geocoded)[((ncol(dt_geocoded)-(NROW(col_names)))+1):(ncol(dt_geocoded))] <- col_names

# Quick fix to remove undesired column
setDT(dt_geocoded)[, (ONS_columns[!(ONS_columns %in% desired_columns)]) := NULL]

if(isTRUE(sum(c("utla", "ltla") %in% desired_columns) >0)){
  UTLA_LTLA <-  fread(file.path("https://opendata.arcgis.com/datasets/3e4f4af826d343349c13fb7f0aa2a307_0.csv"))
  dt_geocoded <- merge(dt_geocoded, UTLA_LTLA[, -"FID"], by.x = "laua", by.y = "LTLA19CD", all.x = T, all.y = F)
}

if(isTRUE(sum(c("stp20", "ccg20") %in% desired_columns) >0)){
  UTLA2011_CCG_STP2020 <-  fread(file.path("https://opendata.arcgis.com/datasets/1631beea57ff4e9fb90d75f9c764ce26_0.csv"))
  UTLA2011_CCG_STP2020$`CCG name`[grepl("Herefordshire ", UTLA2011_CCG_STP2020$`CCG name`, ignore.case = T)] <- "NHS Herefordshire and Worcestershire CCG"
  dt_geocoded <- merge(dt_geocoded, UTLA2011_CCG_STP2020, by.x = c("lsoa11"), by.y = c("LSOA11CD"), all.x = T, all.y = F)
}


  end_time <- Sys.time()
  cat("Run time:")
  print(end_time - start_time)
  message(paste("Postcodes searched:",sum(!is.na((dt_geocoded[,get(query_column)])))))
  message(paste("Postcodes matched:", NROW(dt_geocoded[!is.na(dt_geocoded[,get(desired_columns)]),])))
  message(paste("Postcodes not found:",NROW(dt_geocoded[is.na(dt_geocoded[,get(desired_columns)]),])))
  message(paste("Query lines with no postcode:",NROW(dt[is.na(dt[,(query_column)]),])))


  if(isTRUE(length(Date_columns) >0)){
  cat("Columns changed to character format:", Date_columns, "\n", sep="\n")
  }
  remove(end_time, start_time)
  gc(verbose = F, full = T)
  return(dt_geocoded)
}







