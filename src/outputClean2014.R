source("src/functions.R")
source("src/cleaningFunctions.R")

df2014 <- get_clean2014()
write_csv_gridPointSurvey(df2014, 2014)