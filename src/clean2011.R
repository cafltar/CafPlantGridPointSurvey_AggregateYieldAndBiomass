library(tidyverse)
library(readxl)
library(sf)

source("src/functions.R")

df2011 <- read_excel("input/Yields and Residue HY2011 112311.xls", 
                           "Grid Point Yields Only") %>% 
  filter(!is.na(UID) | (!is.na(Row) & !is.na(Column))) %>% 
  arrange(UID)

# Merge the georef data based on col and row2
df <- append_georef_to_df(df2011, "Row", "Column")

# Check that ID2 values are ok after merging with row2 and col (it's not)
df.id2.check <- df %>% 
  filter(df$UID != df$ID2)
if(nrow(df.id2.check) > 0) { stop("Error in ID2, Row2, Col") }

# There are values where ID2, Row2, and Col do not mesh, after review, chose ID2, not UID

# TODO: Incomplete, need dry weights, need to calc dry yield, need dry residue, need C, N values... ahhhh!!!
df.calcs <- df %>% 
  mutate(Comments = case_when(is.na(as.numeric(df$`Total Residue and Grain Wet (g)`)) ~ paste("Residue note: ", df$`Total Residue and Grain Wet (g)`, sep = ""), TRUE ~ "")) %>% 
  mutate(Comments = case_when(is.na(as.numeric(df$`Total Grain Wet (g)`)) ~ paste(Comments, " | Grain note: ", df$`Total Grain Wet (g)`, sep = ""), TRUE ~ Comments)) %>% 
  mutate(BiomassWet = as.numeric(`Total Residue and Grain Wet (g)`)) %>% 
  mutate(GrainMassWet = as.numeric(`Total Grain Wet (g)`)) %>% 
  mutate(ResidueMassWetPerArea = BiomassWet - GrainMassWet / `Area (m2)`) %>% 
  mutate(GrainYieldWetPerArea = GrainMassWet / `Area (m2)`)

# TODO: Figure out if "wet" here means dry, or if there are missing data somewhere...
#df.clean <- df %>% 
#  select(Year,
#         `Current Crop`,
#         Barcode,
#         X,
#         Y,
#         ID2,
#         )