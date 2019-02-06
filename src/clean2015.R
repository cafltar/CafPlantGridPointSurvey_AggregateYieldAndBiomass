library(tidyverse)
library(readxl)
library(sf)

source("src/functions.R")

df2015 <- read_excel("input/HY2015GP_171018.xlsx", 
                     "Clean")

df.calc <- df2015 %>% 
  mutate(GrainYieldDryPerArea = case_when(Crop == "SC" ~ `GrainNetWt (g)` / 2,
                                          TRUE ~ `GrainNetWt (g)` / 2.4384),
         ResidueMassDryPerArea = case_when(Crop == "SC" ~ (`NetWt (g)` - `GrainNetWt (g)`) / 2,
                                           TRUE ~ (`NetWt (g)` - `GrainNetWt (g)`) / 2.4384))

df.clean <- df.calc %>% 
  rename(SampleID = Barcode,
         GrainOilDM = `Oil (DM)`,
         GrainProtein = `Protein (%)`,
         GrainMoisture = `Moisture (%)`,
         GrainStarch = `Starch (%)`,
         GrainWGlutDM = WGlutDM,
         Comments = Notes) %>% 
  select(HarvestYear,
         Crop,
         SampleID,
         Longitude,
         Latitude,
         ID2,
         GrainYieldDryPerArea,
         GrainOilDM,
         GrainProtein,
         GrainMoisture,
         GrainStarch,
         GrainWGlutDM,
         ResidueMassDryPerArea,
         Comments)

write_csv_gridPointSurvey(df.clean, 2015)
