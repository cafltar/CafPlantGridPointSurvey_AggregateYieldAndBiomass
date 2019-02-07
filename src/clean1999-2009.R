source("src/functions.R")
source("src/cleaningFunctions.R")

df1999_2009 <- get_clean1999_2009()

write_csv_gridPointSurvey(df1999_2009, "1999-2009")