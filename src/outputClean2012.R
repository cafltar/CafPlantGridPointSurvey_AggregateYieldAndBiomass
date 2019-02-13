source("src/functions.R")
source("src/cleaningFunctions.R")

df2012 <- get_clean2012()
write_csv_gridPointSurvey(df2012, 2012)