library(tidyverse)
library(readxl)
library(sf)

source("src/functions.R")

# Functions
check_yields <- function(df.in, sheet.name, year, yield.column) {
  yield.check <- read_excel("input/StripbyStripAvg2.xlsx", sheet = sheet.name)
  yield.check <- yield.check %>% 
    select("UID", yield.column) %>% 
    rename("ID2" = 1, "Yield" = 2)
  yield.total.diff <- df.in %>% 
    filter(Year == year) %>% 
    select(Year, ID2, `total grain yield dry(grams/M2)`) %>% 
    full_join(yield.check, by = "ID2") %>% 
    mutate(yield.diff = `total grain yield dry(grams/M2)` - Yield) %>% 
    summarize(total.diff = sum(yield.diff, na.rm = TRUE))
  if(yield.total.diff[[1]] > 0.01) { stop("Yield check failed") }
}

# Load data
legacy <- read_excel(
  "input/Cook Farm All years all points.xlsx", 
  na = c("-99999", "-77777"))


# Keep only values from grid point project
legacy <- legacy %>% filter(Project == "grid points" | Project == "Grid Points")

# Merge the data based on col and row2
df <- append_georef_to_df(legacy, "Row Letter", "Column")

# Check that merged ID2 matches with Sample Location ID (it does)
id.errors <- df %>% 
  mutate(`Sample Location ID` = as.numeric(`Sample Location ID`)) %>% 
  mutate(`ID2` = as.numeric(`ID2`)) %>% 
  filter(`Sample Location ID` != `ID2`) %>% 
  select(`Sample Location ID`, `ID2`)
if(nrow(id.errors) > 0) { stop("Merge check failed") }

# Remove ID2 values with NA (fields north of current CookEast used to be sampled)
df <- df %>% filter(!is.na(ID2))

# Compare yields with those that Kadar used for relative yield project (which is last known (to me) yield data that Dave looked at)
check_yields(df, "99", 1999, "Yield _1999")
check_yields(df, "00", 2000, "Yield _2000")
check_yields(df, "01", 2001, "Yield _2001")
check_yields(df, "02", 2002, "Yield _2002")
check_yields(df, "03", 2003, "Yield_2003")
check_yields(df, "04", 2004, "Yield_2004")
check_yields(df, "05", 2005, "Yield_2005")
check_yields(df, "06", 2006, "Yield_2006")
check_yields(df, "07", 2007, "Yield_2007")
check_yields(df, "08", 2008, "Yield_2008")
check_yields(df, "09", 2009, "Yield_2009")

# Check for differences between duplicated column names
compare_cols(df, "Grain Carbon %", "Grain Carbon %__1")
compare_cols(df, "Grain Sulfur %", "Grain Sulfur %__1")
compare_cols(df, "Grain Nitrogen %", "Grain Nitrogen %__1")
compare_cols(df, "Crop", "Crop__1")

# Check that residue was calculated correctly (11 obs had error)
residue.check <- df %>% 
  select(Year,
         ID2, 
         `Residue plus Grain Wet Weight (grams)`,
         `Residue sample Grain Wet Weight (grams)`,
         `Total Residue Wet Weight (grams)`,
         `Non-Residue Grain Wet Weight (grams)`) %>% 
  mutate(ResidueWetWeightCalc = `Residue plus Grain Wet Weight (grams)` - `Residue sample Grain Wet Weight (grams)`) %>% 
  filter(abs(ResidueWetWeightCalc - `Total Residue Wet Weight (grams)`) > 0.01)
if(nrow(residue.check) > 0) { stop("Residue column is not trustworthy; review dataframe 'residue.check'") }

# No need to check yield, the calculation is in the excel sheet and passes muster

# Calculate residue values, don't use supplied since it seems to have errors
# Also calc HI for review
df <- df %>% 
  mutate(ResidueWetMass = `Residue plus Grain Wet Weight (grams)` - `Residue sample Grain Wet Weight (grams)`) %>% 
  mutate(ResidueMoistureProportion = (`Residue Sub-Sample Wet Weight (grams)` - `Residue Sub-Sample Dry Weight (grams)`) / `Residue Sub-Sample Wet Weight (grams)`) %>% 
  mutate(ResidueDry = ResidueWetMass * (1 - ResidueMoistureProportion)) %>% 
  mutate(ResidueDryPerArea = ResidueDry / `Residue Sample Area (square meters)`) %>% 
  mutate(HarvestIndex = `total grain yield dry(grams/M2)` / (ResidueDryPerArea + `total grain yield dry(grams/M2)`)) 

# TODO: Need to rename columns, finalize ones to include
df.clean <- df %>% 
  mutate(Comments = coalesce(`Grain Harvest Comments`, `Residue Sample Comments`)) %>% 
  select(Year, 
         Crop, X, Y, ID2,
         `total grain yield dry(grams/M2)`,
         `Grain Carbon %`, `Grain Nitrogen %`, `Grain Sulfur %`,
         ResidueDryPerArea, 
         `Residue Carbon %`,`Residue Nitrogen %`, `Residue Sulfur %`, 
         Comments) %>% 
  rename(HarvestYear = Year,
         Crop = Crop,
         Latitude = Y,
         Longitude = X,
         ID2 = ID2,
         GrainYieldDry = `total grain yield dry(grams/M2)`,
         GrainCarbon = `Grain Carbon %`,
         GrainNitrogen = `Grain Nitrogen %`,
         GrainSulfur = `Grain Sulfur %`,
         ResidueMassDry = ResidueDryPerArea,
         ResidueCarbon = `Residue Carbon %`,
         ResidueNitrogen = `Residue Nitrogen %`,
         ResidueSulfur = `Residue Sulfur %`,
         Comments = Comments) %>% 
  replace(. == "winter wheat", "WW") %>% 
  replace(. == "spring wheat", "SW") %>% 
  replace(. == "spring barley", "SB") %>% 
  replace(. == "spring canola", "SC") %>% 
  replace(. == "spring pea", "SP") %>% 
  replace(. == "winter barley", "WB") %>% 
  replace(. == "winter pea", "WP") %>% 
  replace(. == "winter canola", "WC") %>% 
  replace(. == "Winter Canola", "WC") %>% 
  replace(. == "winter lentil", "WL") %>% 
  replace(. == "Garbonzo Beans", "GB") %>% 
  mutate(HarvestYear = as.integer(HarvestYear)) %>% 
  filter(!is.na(ID2), Crop != "Fallow") %>% 
  arrange(HarvestYear, ID2)

write_csv_gridPointSurvey(df.clean, "1999-2009")
