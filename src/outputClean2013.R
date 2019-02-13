source("src/functions.R")
source("src/cleaningFunctions.R")

df2013 <- get_clean2013()
write_csv_gridPointSurvey(df2013, 2013)