source("src/functions.R")
source("src/cleaningFunctions.R")

df2016 <- get_clean2016()
write_csv_gridPointSurvey(df2016, 2016)