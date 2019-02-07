source("src/functions.R")
source("src/cleaningFunctions.R")

df2010 <- get_clean2010()
write_csv_gridPointSurvey(df2010, 2010)