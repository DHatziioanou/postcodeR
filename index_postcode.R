#' Create an Index for an ONS NSPL postcode database
#'
#' Creates an index of a postcode database file using the first letters and first number from within the postcode. This has been designed for use with the NSPL database of UK postcodes and will not work for postcodes which do not start with letters followed by numbers.
#'
#' @param postcode_path path to folder containing multiple postcode csv files or a single csv file. Eg for ONS NSPL this could be either file.path("NSPL_FEB_2019_UK", "Data", "multi_csv") from where the ONS data was extracted if using the multi_csv files or file.path("NSPL_FEB_2019_UK", "Data", "NSPL_FEB_2019_UK.csv") if using the full database file. Using multi_csv files can overcome insufficient memory barriers.
#'
#' @param colname column name within database csv file(s) with postcode for postcode matching (optional). Default is the first column name within the database.
#'
#' @param name name to call the index (optional). Default is "Index".
#'
#' @param exclude string pattern within files to exclude when using a folder as postcode_path (optional).
#'
#' @return Returns a postcode database Index using the first letters from the file names and the first digits of the numbers within the postcodes as a data.frame object. Saves the Index file to the location of the postcode database.
#'
#'
#' @author Diane Hatziioanou
#'
#' @keywords postcode, Index
#'
#' @examples
#' Index <- index_postcodes(postcode_path)
#'
#' Index_ONS <- index_postcodes("C:/.../NSPL_FEB_2019_UK/Data/multi_csv/)
#'
#' Index_ONS <- index_postcodes(file.path(path, "NSPL_FEB_2019_UK, "Data ,"multi_csv"), exclude = "Readme.csv")
#'
#' Index_file <- index_postcodes(file.path(path, "2020 database, "Reference.csv"), colname = "pcd", name = "Reference_index")
#' 
#' # Create an index if necessary or read in if already there
#' if (!file.exists(file.path(NSPL_csvs,"Index.csv"))) {
#' Index <- index_postcodes(NSPL_csvs)
#' } else {
#'   Index <- fread(file.path(NSPL_csvs,"Index.csv"))
#'   }
#'
#' @export
index_postcodes <- function(postcode_path, colname, name, exclude){
  start_time <- Sys.time()
  if (!require("data.table")) {
    install.packages("data.table")
    library(data.table)
  }
  if (!require("dplyr")) {
    install.packages("dplyr")
    library(dplyr)
  }
  
  if (missing(name)) {
    name <- "Index"
  }
  
  # Database file path and file name(s)
  if (dir.exists(postcode_path)) { # Directory
    
    # List files
    Db_files <- list.files(path = file.path(postcode_path),
                           include.dirs = F,
                           recursive = F,
                           pattern = ".csv")
    Db_files <- as.data.frame(Db_files[!grepl("\\$", Db_files)], responseName  = "file", row.names = NULL, optional = F, stringsAsFactors = F)
    data.table::setnames(Db_files, "file")
    
    setDT(Db_files)[, full_path := file.path(postcode_path,Db_files$file)]
    Db_files <- Db_files[!file.info(Db_files$full_path)$isdir]
    Db_files <- Db_files[str_detect(Db_files$file, "^NSPL_") & str_detect(Db_files$file, ".csv"),]

    
    if (missing(exclude)) {
      message(paste("Indexing all csv files within", postcode_path, "as ONE database"))
    } else {
      
      Db_files <- Db_files[!grepl(paste(exclude, collapse = "|") , Db_files$file),]
      message(paste("Indexing", NROW(Db_files$file), "csv files within", postcode_path, "as ONE database"))
      
    }
    if (dim(Db_files)[1] == 0) {
      
      stop("No csv files found.")
      
    }
    
    
    
    # Check database files
    for(i in 1:nrow(Db_files)) {
      
      db_file <- fread(file.path(Db_files[i,"full_path"]), header = F, nrows = 1)
      if(exists("db_files")) {
        
        t <- try(db_files <- rbind(setDT(db_files), setDT(db_file), fill=TRUE), silent = T)
        ifelse(inherits(t, "try-error"), stop("Column name mismatch; csv files don't all belong to the same database."),t)
        
      } else {
        db_files <- db_file
      }
    }

    # Postcode column
    if (missing(colname)) {
      
      colname <- db_files[1,1] %>% as.character()
      
    }
    remove(db_files, db_file)
    path <- postcode_path
    
  } else if (file.exists(postcode_path) && !dir.exists(postcode_path)) { # Single file
    
    Db_files <- postcode_path
    Db_files <- as.data.frame(Db_files[!grepl("\\$", Db_files)], responseName  = "file", row.names = NULL, optional = F, stringsAsFactors =F)
    data.table::setnames(Db_files, "full_path")
    setDT(Db_files)[, file := basename(postcode_path)]
    message(paste("Indexing csv file", Db_files$file, "within", postcode_path, "as ONE database"))
    path <- dirname(postcode_path)
    
    # Postcode column
    if (missing(colname)) {
      colname <- fread(file.path(Db_files[1,"full_path"]), header = F, select = 1, nrows = 1) %>% as.character()
    }
    
  } else { # Missing database error
    
    stop("Could not find any .csv files at ", postcode_path)
    
  }
  
  
  # Get file row index positions
  for(i in 1:nrow(Db_files)) {
    
    # Get postcode letters and first number
    Pc <- fread(file.path(Db_files[i,"full_path"]), header = T)
    
    setDT(Pc)[,block := toupper(gsub(" ", "", Pc[,get(colname)]))]
    Pc[, block := (gsub('([a-zA-Z])([0-9])', '\\1_\\2', block))]
    Pc[, block := paste0(gsub( "_.*$", "",  block), substr(gsub('\\D+','', block), 1,1))]
    Pc[, block := factor(Pc$block)]
    Pc[, row := 1:nrow(Pc)]
    
    # Get file index rows
    rows <- base::split(1:nrow(Pc), Pc$block)
    index <- sapply(rows, function(rowi) {rowi[which.min(Pc$row[rowi])]})
    index <- setDT(list(index))[] %>% setnames("start")
    index$stop <-  sapply(rows, function(rowi) {rowi[which.max(Pc$row[rowi])]})
    index[, block := names(rows)]
    remove(rows)
    
    index[,start := index$start + 1] # count for header row
    index[,stop := index$stop + 1] # count for header row
    
    index[,full_path := Db_files[i,"full_path"]]
    index[,file := Db_files[i,"file"]]
    
    # IF MERGED DATASET EXISTS, MERGE
    if (exists("Index") & dim(index)[1] != 0 ) {
      Index <- rbind(setDT(Index), setDT(index), fill=TRUE, stringsAsFactors = F)
    }
    # IF MERGED DATASET DOESN'T EXIST, CREATE IT
    if (!exists("Index")) {
      Index <- index
    }
    remove(index,Pc)
  }
  Index <- Index[!is.na(Index$block),]
  # Save index
  setcolorder(Index,c("block", "start", "stop", "file", "full_path"))
  fwrite(x = Index, file.path(path, paste0(name,".csv")), row.names = F, nThread = getDTthreads())
  
  # Report
  end_time <- Sys.time()
  message(paste("Run time:",end_time - start_time))
  message(paste("Index saved at:",file.path(path, paste0(name,".csv"))))
  Index <- as.data.frame(Index)
  return(Index)
  
}
