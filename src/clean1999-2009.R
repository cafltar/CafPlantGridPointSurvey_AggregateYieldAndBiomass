library(tidyverse)
library(readxl)
library(sf)

# Functions
check.yields <- function(df.in, sheet.name, year, yield.column) {
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
compare.cols <- function(df.in, column1, column2) {
  #sum <- sum(df.in[column1] - df.in[column2], na.rm = TRUE)
  #if(sum > 0) { stop("Columns don't match") }
  compare <- df.in[column1] == df.in[column2]
  if(nrow(compare) < nrow(df.in[column1])) { stop("Columns do not match") }
}

# Load data
legacy <- read_excel(
  "input/Cook Farm All years all points.xlsx", 
  na = "-99999")
georef <- st_read("input/CookEast_GeoreferencePoints_171127.json")

# Keep only values from grid point project
legacy <- legacy %>% filter(Project == "grid points" | Project == "Grid Points")

# Merge the data based on col and row2
df <- legacy %>% 
  mutate(Column = as.integer(Column)) %>% 
  select(-Strip, -Field) %>% 
  rename("Row2" = "Row Letter") %>% 
  full_join(data.frame(st_coordinates(georef),
                       st_set_geometry(georef, NULL)),
            by = c("Column", "Row2"))

# Check that merged ID2 matches with Sample Location ID (it does)
id.errors <- df %>% 
  mutate(`Sample Location ID` = as.numeric(`Sample Location ID`)) %>% 
  mutate(`ID2` = as.numeric(`ID2`)) %>% 
  filter(`Sample Location ID` != `ID2`) %>% 
  select(`Sample Location ID`, `ID2`)
if(nrow(id.errors) > 0) { stop("Merge check failed") }

# Compare yields with those that Kadar used for relative yield project (which is last known (to me) yield data that Dave looked at)
check.yields(df, "99", 1999, "Yield _1999")
check.yields(df, "00", 2000, "Yield _2000")
check.yields(df, "01", 2001, "Yield _2001")
check.yields(df, "02", 2002, "Yield _2002")
check.yields(df, "03", 2003, "Yield_2003")
check.yields(df, "04", 2004, "Yield_2004")
check.yields(df, "05", 2005, "Yield_2005")
check.yields(df, "06", 2006, "Yield_2006")
check.yields(df, "07", 2007, "Yield_2007")
check.yields(df, "08", 2008, "Yield_2008")
check.yields(df, "09", 2009, "Yield_2009")

# Check for differences between duplicated column names
compare.cols(df, "Grain Carbon %", "Grain Carbon %__1")
compare.cols(df, "Grain Sulfur %", "Grain Sulfur %__1")
compare.cols(df, "Grain Nitrogen %", "Grain Nitrogen %__1")
compare.cols(df, "Crop", "Crop__1")

# TODO: Need to rename columns, finalize ones to include
df.clean <- df %>% 
  select(Year, `Grain Carbon %`, `Grain Sulfur %`, `Grain Nitrogen %`,
         `Total Residue Wet Weight (grams)`, 
         `Residue Sub-Sample Wet Weight (grams)`,
         `Residue Sub-Sample Dry Weight (grams)`,
         `Residue Nitrogen %`, `Residue Sulfur %`, `Residue Carbon %`,
         `Grain Harvest Comments`, `Residue Sample Comments`,
         `total grain yield dry(grams/M2)`,
         X, Y, ID2, Crop) %>% 
  filter(!is.na(ID2))


date.today <- format(Sys.Date(), "%y%m%d")
filePath.all <- paste(
  "working/HY1999-HY2009_AllColumns_",
  date.today,
  ".csv",
  sep = "")
write.csv(df, filePath.all, row.names=FALSE, na = "")

filePath.clean <- paste(
  "working/HY1999-HY2009_cleanedData_",
  date.today,
  ".csv",
  sep = "")
write.csv(df.clean, filePath.clean, row.names=FALSE, na = "")
