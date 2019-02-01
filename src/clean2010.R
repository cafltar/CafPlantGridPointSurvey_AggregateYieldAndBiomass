library(tidyverse)
library(readxl)
library(sf)

source("src/functions.R")

df2010 <- read_excel("input/Grid Points Yields and Residue 2010.xls") %>% 
  mutate(`Bag Barcode` = toupper(`Bag Barcode`))
df.res <- read_excel("input/Yields and Residue 2010 Final.xls", 
                     "All Data", 
                     skip = 1, 
                     n_max = 1157) %>% 
  filter(Project == "GP") %>% 
  mutate(`Bag Barcode` = toupper(`Bag Barcode`))

# Check that grain mass match
df.grain.check <- df2010 %>% 
  full_join(df.res, by = c("Bag Barcode", 
                   "Farm", 
                   "Year", 
                   "Project", 
                   "Grain Type")) %>% 
  filter(`Total Grain Dry (g).x` != `Total Grain Dry (g).y`)
if(nrow(grain.check) > 0) { stop("Grain masses don't match; review grain.check") }

# Merge the georef data based on col and row2
df <- append_georef_to_df(df2010, "Row", "Column")

# Check that ID2 values are ok after merging with row2 and col (it's not)
df.id2.check <- df %>% 
  filter(df$UID != df$ID2)
if(nrow(df.id2.check) > 0) { stop("Error in ID2, Row2, Col") }

# There are values where ID2, Row2, and Col do not mesh, after review, choose ID2, not UID

df.res.slim <- df.res %>% 
  filter(!is.na(`Residue Sub Wet (g)`),
         !is.na(`Total Biomass Wet (g)`),
         !is.na(`Total Grain Wet (g)`),
         `Residue Sub Wet (g)` > 0) %>% 
  mutate(ResidueWetMass = `Total Biomass Wet (g)` - as.numeric(`Total Grain Wet (g)`)) %>% 
  mutate(ResidueMoistureProportion = (`Residue Sub Wet (g)` - `Sub Res Dry (g)`) / `Residue Sub Wet (g)`) %>% 
  mutate(ResidueDry = ResidueWetMass * (1 - ResidueMoistureProportion)) %>% 
  select(`Bag Barcode`, ResidueDry)

# Merge residue data and calc values
df <- df %>% 
  full_join(df.res.slim, by = c("Bag Barcode")) %>%
  mutate(GrainYieldDryPerArea = `Total Grain Dry (g)` / `Area (m2)`) %>% 
  mutate(ResidueDryPerArea = ResidueDry / `Area (m2)`) %>%
  mutate(HarvestIndex = GrainYieldDryPerArea / (ResidueDryPerArea + GrainYieldDryPerArea))

df.clean <- df %>% 
  select(Year,
         `Grain Type`,
         `Bag Barcode`,
         X,
         Y,
         ID2,
         GrainYieldDryPerArea,
         ResidueDryPerArea,
         Comments) %>% 
  rename("HarvestYear" = Year,
         "Crop" = `Grain Type`,
         "SampleID" = `Bag Barcode`,
         "Latitude" = X,
         "Longitude" = Y,
         "Notes" = Comments) %>% 
  filter(!is.na(ID2)) %>% 
  arrange(HarvestYear, ID2)

write_csv_gridPointSurvey(df.clean, 2010)
