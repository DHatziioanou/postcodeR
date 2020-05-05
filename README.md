# postcodeR
Retrieve ONS NSPL data by postcode

Database indexing

Step 1. Retrieve the NSPL database from https://geoportal.statistics.gov.uk/datasets/national-statistics-postcode-lookup-february-2020 and unzip it.

Step 2. Create an index:

Give the file path to the "multi_csv" folder within the NSPL database to the index_postcodes command.

Index <- index_postcodes(file.path(path, "NSPL_FEB_2019_UK, "Data ,"multi_csv"))

Postcode geography retrieval:

Step 1. If not in your environment retrieve your Index file. Index <- fread(file.path("C:/.../NSPL_FEB_2019_UK/Data/Index.csv"))

Step 2. Make a vector of the geographies for retrieval:

desired_columns <- c("laua", "lsoa11", "ccg", "stp20", "ccg20")

Step 3. Retrieve the geographies for your postcodes

data_with_geographies <- match_postcode(data, "Postcode", Index, desired_columns)

Summary stats of the mapping will be printed in the Console.
