#' Index ONS postcode database
#'
#' Creates an index of ONS postcode files ‘https://www.ons.gov.uk/‘
#'
#' @param postcode_path path to folder containing ONS postcode multi_csv files. Eg for ONS NSPL this is file.path("NSPL_FEB_2019_UK", "Data", "multi_csv") from where the ONS data was extracted
#'
#' @return Returns an index of ONS postcode files using the first letters from the file names and the first digits of the numbers within the postcodes of each file as a data.frame object. Saves the index file to the folder above the postcode file location.
#'
#'
#' @author Diane Hatziioanou
#'
#' @keywords postcode, ONS, NSPL
#'
#' @examples
#' Index <- index_postcodes(postcode_path)
#'
#' Index_ONS <- index_postcodes("C:/.../NSPL_FEB_2019_UK/Data/multi_csv/)
#'
#' Index_ONS <- index_postcodes(file.path(path, "NSPL_FEB_2019_UK, "Data ,"multi_csv"))
#'
#' @export
index_postcodes <- function(postcode_path){
start_time <- Sys.time()
if (!require("data.table")) {
  install.packages("data.table")
  library(data.table)
}
if (!require("dplyr")) {
  install.packages("dplyr")
  library(dplyr)
}

# List files
ONS_files <- list.files(path = file.path(postcode_path),
                   include.dirs = FALSE,
                   recursive = FALSE,
                   pattern = ".csv")
ONS_files <- as.data.frame(ONS_files[!grepl("\\$", ONS_files)], responseName  = "file", row.names = NULL, optional = F, stringsAsFactors =F)
data.table::setnames(ONS_files, "file")

setDT(ONS_files)[, full_path := file.path(postcode_path,ONS_files$file)]
ONS_files[, Pc := (gsub(".*UK_(.+).csv*", "\\1",ONS_files$file))]

# Get file row indexes positions
for(i in 1:nrow(ONS_files)){

# Get file index numbers
Pc <- fread(file.path(ONS_files[i,"full_path"]), header = T)
Pc[,N1 := ""]
Pc[,N1 := gsub(paste0(".*",ONS_files[i,3],"([0-9]+).*$"), "\\1", Pc$pcd)]
Pc[,N1 := factor(substr(start = 1, stop = 1, x= Pc$N1))]
Pc[,row := 1:nrow(Pc)]

# Get file index rows
rows <- base::split(1:nrow(Pc), Pc$N1)
index <- sapply(rows, function(rowi) {rowi[which.min(Pc$row[rowi])]})
index <- setDT(list(index))[] %>% setnames("start")
index$stop <-  sapply(rows, function(rowi) {rowi[which.max(Pc$row[rowi])]})
index[, Pc_N := names(rows)]
remove(rows)
index[, Pc := ONS_files[i,3]]

setDT(index)[, Pc_s := paste0(Pc,Pc_N)]
index[,start := index$start + 1] # count for header row
index[,stop := index$stop + 1] # count for header row

# IF MERGED DATASET EXISTS, MERGE
if (exists("Index") & dim(index)[1] !=0 ){
  Index <-rbind(Index, index, fill=TRUE, stringsAsFactors = FALSE)
}
# IF MERGED DATASET DOESN'T EXIST, CREATE IT
if (!exists("Index")){
  Index <- index
}
remove(index,Pc)

}

setDT(Index)[,Pc:=factor(Index$Pc)]


# Add Index to file list
Index <- merge(ONS_files,Index, all =T, by = "Pc")

Index[,c("Pc", "Pc_N"):= NULL]
setcolorder(Index,c("Pc_s", "start", "stop", "file", "full_path"))


# Save index
fwrite(x = Index, file.path(dirname(postcode_path), "Index.csv"), row.names = F, nThread = getDTthreads())
end_time <- Sys.time()
message(paste("Run time:",end_time - start_time))
message(paste("Index saved at:",file.path(dirname(postcode_path), "Index.csv")))
return(Index)
}








