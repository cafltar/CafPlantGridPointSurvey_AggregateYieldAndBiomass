library(tidyverse)
library(readxl)
library(sf)

source("src/functions.R")

df2012 <- read_excel("input/Yields and Residue 2012 011513.xlsx", 
                           "Grid Points",
                           skip = 1) %>% 
  filter(!is.na(ID2) | (!is.na(ROW2) & !is.na(COLUMN))) %>% 
  mutate(SampleID = toupper(Barcode)) %>% 
  arrange(ID2)

# Merge the georef data based on col and row2
df <- append_georef_to_df(df2012, "ROW2", "COLUMN")

# Check that ID2 values are ok after merging with row2 and col
df.id2.check <- df %>% 
  filter(ID2.x != ID2.y)
if(nrow(df.id2.check) > 0) { stop("Error in ID2, Row2, Col") }

# TODO: Calculate residue dry and per area
df.calcs <- df %>% 
  mutate(GrainYieldDryPerArea = `Grain Weight Dry (g)` / `Area (m2)`) %>% 
  mutate(Comments = case_when((!is.na(df$`Test Weight`) & is.na(as.numeric(df$`Test Weight`))) ~ paste("TestWeight note: ", df$`Test Weight`, sep = ""), TRUE ~ "")) %>% 
  mutate(Comments = case_when(!is.na(df$..24) ~ paste(Comments, " | Sample note: ", df$..24, sep = ""), TRUE ~ Comments)) %>% 
  mutate(HarvestYear = 2012)

df.clean <- df.calcs %>% 
  rename(Longitude = X,
         Latitude = Y,
         ID2 = ID2.x,
         GrainProtein = Protein,
         GrainMoisture = Moisture,
         GrainStarch = Starch,
         GrainWGlutDM = Gluten) %>% 
  select(HarvestYear,
         Crop,
         SampleID,
         Longitude,
         Latitude,
         ID2,
         GrainYieldDryPerArea,
         GrainProtein,
         GrainMoisture,
         GrainStarch,
         GrainWGlutDM,
         Comments)
  
write_csv_gridPointSurvey(df.clean, 2012)
