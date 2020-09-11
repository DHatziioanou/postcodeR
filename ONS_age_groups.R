
# # Calculate ONS populations by age group for any age grouping using the ONS populations file
# #
# # Import ONS population data
# # Age groups input
#
#
# # Import population data
# ONS_pop <- readxl::read_xlsx(file.path(...,  "SAPE21DT1a-mid-2018-on-2019-LA-lsoa-syoa-estimates-formatted.xlsx"), sheet = "Mid-2018 Persons",  col_names  =  T, skip = 4)
#
# Create age grouping
# age_groups_3cat <- data.frame(
#   Population_under18 = c(0,17),
#   Population_workingage = c(18,65),
#   Population_over65 = c(66,"90+"),
#   stringsAsFactors = F)
#
# age_groups_9cat <- data.frame(
#   `Population_0-4yrs` = c(0,4),
#   `Population_5-10yrs` = c(5,10),
#   `Population_11-17yrs` = c(11,17),
#   `Population_18-29yrs` = c(18,29),
#   `Population_30-39yrs` = c(30,39),
#   `Population_40-49yrs` = c(40,49),
#   `Population_50-59yrs` = c(50,59),
#   `Population_60-69yrs` = c(60,69),
#   `Population_70+yrs` = c(70,"90+"),
#   stringsAsFactors = F, check.names = T, fix.empty.names = T)


#
# ONS_populations_3categories <- ONS_age_groups(ONS_pop, age_groups_3cat)
# ONS_populations_9categories <- ONS_age_groups(ONS_pop, age_groups_9cat)


ONS_age_groups <- function(populations, age_groups){

  populations <- as.data.frame(populations)

  # Add age group populations
  for(i in names(age_groups)){

    # Populations inclusing 90+
    if(age_groups[2,i] == "90+"){

      populations[[i]] <- rowSums(populations[,c(which(names(populations) %in% c(as.character(as.numeric(age_groups[1,i]):89), "90+")))], na.rm = T)

      # Populations under 90
    } else {

      populations[[i]] <- rowSums(populations[,c(which(names(populations) %in% c(as.character(as.numeric(age_groups[1,i]):as.numeric(age_groups[2,i])))))], na.rm = T)

    }

  }

  # Shave unnecessary columns off
  try(
    populations <- populations[, c(colnames(populations)[1:grep("All", colnames(populations), ignore.case = T)], names(age_groups))]
    )
  
  
  
  if(inherits(populations, "try-error")){
    populations <- populations[, c(colnames(populations)[1:grep("Population", colnames(populations), ignore.case = T)], names(age_groups))]
  }

  names(populations) <- c(colnames(populations)[1:(grep("All", colnames(populations), ignore.case = T)-1)], "Population all ages", names(age_groups))

  # Return agre group populations
  return(populations)
}






