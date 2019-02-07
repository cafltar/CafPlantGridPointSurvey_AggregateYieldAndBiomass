source("src/functions.R")
source("src/cleaningFunctions.R")

df1999_2009 <- get_clean1999_2009()
df2010 <- get_clean2010()
df2011 <- get_clean2011()
df2012 <- get_clean2012()
df2013 <- get_clean2013()
df2014 <- get_clean2014()
df2015 <- get_clean2015()
df2016 <- get_clean2016()

df <- bind_rows(df1999_2009,
                df2010,
                df2011,
                df2012,
                df2013,
                df2014,
                df2015,
                df2016)

write_csv_gridPointSurvey(df, "1999-2016")
