source("src/functions.R")
source("src/cleaningFunctions.R")

df2015 <- get_clean2015()
write_csv_gridPointSurvey(df2015, 2015)