library(tidyverse)
library(readxl)

source("src/cleaningFunctions.R")

# read data
df <- read_csv("output/HY1999-2016_20190723_Q01P02.csv")

#df <- get_clean1999_2016()

years <- seq(1999, 2009, 1)

for(year in years) {
  sheet = substr(toString(year), 3, 4)

  df.kadar <- read_excel("input/StripbyStripAvg2.xlsx", sheet = sheet) %>% 
    select(1, 2, 3, 4) %>% 
    rename("Crop.Kadar" = 1, 
           "ID2" = 2,
           "Field" = 3,
           "Strip" = 4) %>% 
    mutate(Crop.Kadar = str_remove(Crop.Kadar, " ")) 
  df.merge <- df %>% 
    filter(HarvestYear == year) %>% 
    select(HarvestYear, Longitude, Latitude, ID2, Crop) %>% 
    left_join(df.kadar, by = "ID2")
  
  diff <- df.merge %>% 
    filter(Crop != Crop.Kadar)
  
  if(!exists("df.issues")) {
    df.issues <- diff
  } else {
    df.issues <- bind_rows(df.issues, diff)
  }
}

date.today <- format(Sys.Date(), "%Y%m%d")
write_csv(df.issues, 
          file.path("working", paste("cropDiffCleanVsKadar_",
                                     date.today,
                                     ".csv",
                                     sep = "")))
