source("src/functions.R")
source("src/cleaningFunctions.R")

df <- get_clean1999_2016()



df %>% 
  group_by(HarvestYear, Crop) %>% 
  summarize(mean(GrainYieldDryPerArea, na.rm = TRUE),
            mean(ResidueMassDryPerArea, na.rm = TRUE))

#write_csv_gridPointSurvey(df, "1999-2016")