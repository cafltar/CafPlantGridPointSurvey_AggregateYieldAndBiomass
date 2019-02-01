library(tidyverse)
library(readxl)
library(sf)
library(readODS)

source("src/functions.R")

# Read yield data
df2013 <- read_excel("input/HY2013GP_171010.xlsx", 
                     "CleanAndCalcYield",
                     skip = 1) %>% 
  arrange(ID2) %>% 
  mutate(SampleID = toupper(SampleID))

# Read grain analysis
#dfCN <- read_ods("input/Cook Farm HY2013 GP WW Grain.ods",
#                 sheet = "Alternative", range = "A1:I129")

# Read grain analysis
dfGrainCN1 <- read_excel("input/CF13GPGB.xlsx",
                       "Sheet1",
                       range = "A1:F31") %>% 
  mutate(SampleID = toupper(Project)) %>% 
  rename(GrainNitrogen = `N%`,
         GrainCarbon = `C%`) %>% 
  select(SampleID,
         GrainNitrogen,
         GrainCarbon)
dfGrainCN2 <- read_excel("input/CF13GPGB Part2.xlsx",
                       "Sheet1",
                       range = "A1:F49") %>% 
  mutate(SampleID = toupper(Project)) %>% 
  rename(GrainNitrogen = `N%`,
         GrainCarbon = `C%`) %>% 
  select(SampleID,
         GrainNitrogen,
         GrainCarbon)
dfGrainCN3 <- read_excel("input/Cook Farm HY2013 GP SW_SB_GB Weights (2).xlsx",
                         "GB",
                         range = "A1:G79") %>% 
  mutate(SampleID = toupper(Barcode)) %>% 
  rename(GrainNitrogen = `N%`,
         GrainCarbon = `C%`) %>% 
  select(SampleID,
         GrainNitrogen,
         GrainCarbon)

dfGrainCN <- bind_rows(dfGrainCN1, dfGrainCN2, dfGrainCN3) %>% 
  distinct() %>% 
  arrange(SampleID)

# Read residue analysis
dfResidue1 <- read_ods("input/Cook Farm HY2013 GP WW Grain (2).ods",
                      sheet = "Alternative", range = "A133:F137",
                      col_names = FALSE) %>% 
  mutate(SampleID = toupper(A),
         ResidueMassDry = as.numeric(C),
         ResidueNitrogen = as.numeric(E),
         ResidueCarbon = as.numeric(`F`)) %>%
  select(SampleID,
         ResidueMassDry,
         ResidueNitrogen,
         ResidueCarbon)
dfResidue2 <- read_excel("input/Cook FarmHY13_GP_GB_Res_STJohnHY13WW_Res.xlsx",
                          "Sheet1",
                          range = "A1:F5") %>% 
  mutate(SampleID = toupper(str_remove(Project, "_Res"))) %>% 
  select(SampleID, `N%`, `C%`) %>% 
  rename(ResidueNitrogen = `N%`,
         ResidueCarbon = `C%`)
dfResidue3 <- read_excel("input/Cook Farm HY2013 GP SW_SB_GB Weights (2).xlsx",
                         "SW_SB",
                         range = "A132:E135",
                         col_names = c("A", "B", "C", "D", "E")) %>% 
  mutate(SampleID = toupper(str_remove(A, "_Re")),
         ResidueMassDry = as.numeric(C),
         ResidueNitrogen = as.numeric(str_remove(D, " !")),
         ResidueCarbon = as.numeric(str_remove(E, " !"))) %>% 
  select(SampleID,
         ResidueMassDry,
         ResidueNitrogen,
         ResidueCarbon)
dfResidue4 <- read_excel("input/Cook Farm HY2013 GP SW_SB_GB Weights (2).xlsx",
                         "GB",
                         range = "A83:G86",
                         col_names = c("A", "B", "C", "D", "E", "F", "G")) %>% 
  mutate(SampleID = toupper(str_remove(A, "_Res")),
         ResidueNitrogen = as.numeric(`F`),
         ResidueCarbon = as.numeric(G)) %>% 
  select(SampleID,
         ResidueNitrogen,
         ResidueCarbon)

dfResidue <- bind_rows(dfResidue1, dfResidue2, dfResidue3, dfResidue4) %>% 
  distinct() %>% 
  arrange(SampleID)

# --- Double checking that previous merge (via Excel) of ID2 and lat/lon are consistent with current
## Merge the georef data based on col and row2
#df.check <- append_georef_to_df(df2013, "Row2", "Column")
## Check that ID2 values are ok after merging with row2 and col
#df.id2.check <- df.check %>% 
#  filter(ID2.x != ID2.y)
#if(nrow(df.id2.check) > 0) { stop("Error in ID2, Row2, Col") }
#df.Long.check <- df.check %>% 
#  filter(abs(Longitude - X) > 0.00001)
#df.lat.check <- df.check %>% 
#  filter(abs(Latitude - Y) > 0.00001)

# Append residue and grain analysis
df <- df2013 %>% 
  left_join(dfGrainCN, by = c("SampleID")) %>% 
  left_join(dfResidue, by = c("SampleID"))

# GrainWeightWet is actually dry weight, when I named that column I considered greenhouse dried samples as still "wet" (it still has moisture).  Our lab considers it dry.  
df.calcs <- df %>% 
  mutate(GrainYieldDryPerArea = GrainWeightWet / Area,
         ResidueMassDryPerArea = ResidueMassDry / Area)

df.clean <- df.calcs %>% 
  filter(!is.na(SampleID)) %>% 
  rename(HarvestYear = Year,
         GrainProtein = Protein,
         GrainMoisture = Moisture,
         GrainStarch = Starch,
         GrainWGlutDM = WGlutDM,
         Comments = Notes) %>% 
  select(HarvestYear,
         Crop,
         SampleID,
         Longitude,
         Latitude,
         ID2,
         GrainYieldDryPerArea,
         GrainCarbon,
         GrainNitrogen,
         GrainProtein,
         GrainMoisture,
         GrainStarch,
         GrainWGlutDM,
         ResidueMassDryPerArea,
         ResidueCarbon,
         ResidueNitrogen,
         Comments)

write_csv_gridPointSurvey(df.clean, 2013)