source("src/functions.R")
source("src/cleaningFunctions.R")

df1999_2016.rm.outliers <- get_clean1999_2016()
df1999_2016 <- get_clean1999_2016(FALSE)

write_csv_gridPointSurvey(df1999_2016.rm.outliers, "1999-2016_rmOutliers")
write_csv_gridPointSurvey(df1999_2016, "1999-2016")
