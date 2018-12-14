# Author: Bryan Carlson
# Contact: bryan.carlson@usda.gov
# Purpose: Select "best" data from various conflicting values from HY2014 data.  Derived from [GPHY2017selectBestData.R](https://github.com/cafltar/CookEastPlantHandHarvest_R/blob/master/cleaningHY2014/GPHY2017selectBestData.R) (note, filename is misnomer, should be HY2014)

# ---- Setup ----
library(rgdal)
library(geojsonio)
library(tidyverse)

hy2014 <- read.csv("input/HY2014GB_aggregate_all_data_171122.csv")
hy2014$ShoudKeep <- NULL
georef <- geojson_read("input/CookEast_GeoreferencePoints_171127.json",
                       what = "sp")
# Merge the data based on col and row2 because John Morse said he focused on
# row and columns, not ID2 values (this is important because ID2 values are not
# consistent with row/column values)
#df <- merge(hy2014, georef,
#            by.x = c("Col", "Row2"),
#            by.y = c("Column", "Row2"),
#            all.x = TRUE)
df <- hy2014 %>% 
  rename("Column" = Col, "ID2Bad" = "ID2") %>% 
  join(as.data.frame(georef), by = c("Column", "Row2"))

# Remove missing data
dfc <- df[!is.na(df$ID2),]

# Create dataframe with "good" values
dfg <- subset(dfc, FALSE)

for (id in 1:max(dfc$ID2, na.rm = TRUE)) {
  reps <- dfc[dfc$ID2 == id,]
  
  # Keep if is only value
  if (nrow(reps) == 1) {
    dfg <- rbind(dfg, reps)
    next
  }
  
  # Keep if NIR data, next iteration if found
  rep.withProtein <- reps[which(!is.na(reps$protein)),]
  if (nrow(rep.withProtein) == 1) {
    dfg <- rbind(dfg, rep.withProtein)
    next
  }
  else if (nrow(rep.withProtein) > 1) {
    # If all rows are duplicates, then keep first row
    if (nrow(rep.withProtein[duplicated(rep.withProtein), ]) == 
        (nrow(rep.withProtein) / 2)) {
      dfg <- rbind(dfg, rep.withProtein[1,])
      next
    }
    else if (length(unique(rep.withProtein$TotalGrain.g.))==1) {
      # If grain mass are all the same, just take first row
      dfg <- rbind(dfg, rep.withProtein[1,])
      next
    }
    else {
      stop(paste("id:", id, "- something wrong with protein"))
    }
  }
  
  # Keep if weighed in 2015, next iteration if so
  rep.weighed2015 <- reps[
    which(
      as.numeric(format(
        as.Date(reps$LastSaved, origin = '1900-1-1'), "%Y")) >= 2015),]
  if (nrow(rep.weighed2015) == 1) {
    dfg <- rbind(dfg, rep.weighed2015)
    next
  }
  else if (nrow(rep.weighed2015) > 1) {
    # If all rows are duplicates, then keep first row
    if (nrow(rep.weighed2015[duplicated(rep.weighed2015), ]) ==
        (nrow(rep.weighed2015) / 2)) {
      dfg <- rbind(dfg, rep.weighed2015[1,])
      next
    }
    else if (length(unique(rep.weighed2015$TotalGrain.g.)) == 1) {
      # If grain mass are all the same, just take first row
      dfg <- rbind(dfg, rep.weighed2015[1,])
      next
    }
    else {
      stop(paste("id:", id, "something wrong checking weighed in 2015"))
    }
  }
  
  # Keep if has grain+bag value
  rep.withGrainBagWeight <- reps[which(!is.na(reps$TareBag.g..1)),]
  if (nrow(rep.withGrainBagWeight) == 1) {
    dfg <- rbind(dfg, rep.withGrainBagWeight)
    next
  }
  else if (nrow(rep.withGrainBagWeight) > 0) {
    # If all rows are duplicates, then keep first row
    if (nrow(rep.withGrainBagWeight[duplicated(rep.withGrainBagWeight), ]) ==
        (nrow(rep.withGrainBagWeight) / 2)) {
      dfg <- rbind(dfg, rep.withGrainBagWeight[1,])
      next
    }
    else if (length(unique(rep.withGrainBagWeight$TotalGrain.g.)) == 1) {
      # If grain mass are all the same, just take first row
      dfg <- rbind(dfg, rep.withGrainBagWeight[1,])
      next
    }
    else {
      stop(paste("id:", id, "something wrong with grain+bag weight"))
    }
    
  }
}

# Test output to compare selected values with manual selection ----
#filePath <- paste(
#                  "Output/selectValueTest_",
#                  format(Sys.Date(), "%y%m%d"),
#                  ".csv",
#                  sep = "")
#write.csv(dfg, filePath)

# ========== TODO: Update for new biomass ============

## Dump messy file before cleaning (this does not have biomass values)
date.today <- format(Sys.Date(), "%y%m%d")
filePath.all <- paste(
  "working/HY2014_selectedDataAllColumns_",
  date.today,
  ".csv",
  sep = "")
write.csv(dfg, filePath.all, na = "")

# Calculate biomass if value does not exist then keep only rows with biomass
dfcBiomass <- dfc %>% 
  mutate(biomass = case_when(
    is.na(TotalBiomass.g.) ~ Tota.Biomass.g..bag - TareBag.g..1,
    TRUE ~ TotalBiomass.g.)) %>% 
  filter(!is.na(biomass)) %>% 
  select(biomass, ID2)

# Merge biomass with main df
dfdBiomass <- left_join(dfg, dfcBiomass, by = c("ID2"))

## Clean data
df.clean <- dfdBiomass %>% 
  select("Column", "Row2", "BarcodeFinal", "Crop", "TotalGrain.g.",
         "protein", "moisture", "starch", "gluten", "testWeight",
         "NOTES", "Notes2", "Notes3", "ID2", "coords.x1", "coords.x2",
         "biomass")

df.clean <- within(df.clean, NotesKeep <- paste(NOTES, Notes2, Notes3,
                                                sep = " | "))

df.clean <- df.clean %>% 
  mutate(NotesKeep = paste(NOTES, Notes2, Notes3,
                           sep = " | ")) %>% 
  select(-NOTES, -Notes2, -Notes3) %>% 
  rename("SampleID" = "BarcodeFinal",
         "GrainWeight" = TotalGrain.g.,
         "Protein" = protein,
         "Moisture" = moisture,
         "Starch" = starch,
         "WGlutDM" = "gluten",
         "TestWeight" = testWeight,
         "ID2" = ID2,
         "Logitude" = coords.x1,
         "Latitude" = coords.x2,
         "Notes" = NotesKeep,
         "ResidueWeight" = biomass) %>% 
  mutate(Year = 2014, FieldID = "CookEast")

filePath.clean <- paste(
  "working/HY2014_cleanedData_",
  date.today,
  ".csv",
  sep = "")
write.csv(df.clean, filePath.clean, row.names=FALSE, na = "")
