source("src/functions.R")
source("src/cleaningFunctions.R")

df2011 <- get_clean2011()
write_csv_gridPointSurvey(df2011, 2011)