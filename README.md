# postcodeR
Retrieve ONS NSPL data by postcode


Database indexing

Step 1. Retrieve the latest NSPL database from http://geoportal.statistics.gov.uk/search?q=NSPL and unzip it.

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


Retrieval of geographies not in ONS NSPL database:

The NSPL database does not have up to date LTLA, ULTA, CCG and STP geographies. The function can retrieve 2019 LTLA and UTLA and 2020 CCG and STP geographies directly from ONS based on the NSPL LAUA and LSOA 2011 geographies respectively. 

To retrieve LTLA or UTLA assign c("laua", "utla", "ltla") to desired_columns.
To retrieve CCG or STP assign c("lsoa11", "stp20", "ccg20") to desired_columns.
