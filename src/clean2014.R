# Author: Bryan Carlson
# Contact: bryan.carlson@usda.gov
# Purpose: Select "best" data from various conflicting values from HY2014 data.  Derived from [GPHY2017selectBestData.R](https://github.com/cafltar/CookEastPlantHandHarvest_R/blob/master/cleaningHY2014/GPHY2017selectBestData.R) (note, filename is misnomer, should be HY2014)

# ---- Setup ----
library(rgdal)
library(geojsonio)
library(plyr)

hy2014 <- read.csv("input/HY2014GB_aggregate_all_data_171122.csv")
hy2014$ShoudKeep <- NULL
georef <- geojson_read("input/CookEast_GeoreferencePoints_171127.json",
                       what = "sp")
# Merge the data based on col and row2 because John Morse said he focused on
# row and columns, not ID2 values (this is important because ID2 values are not
# consistent with row/column values)
df <- merge(hy2014, georef,
            by.x = c("Col", "Row2"),
            by.y = c("Column", "Row2"),
            all.x = TRUE)

# Remove missing data
dfc <- df[!is.na(df$ID2.y),]

# Create dataframe with "good" values
dfg <- subset(dfc, FALSE)

for (id in 1:max(dfc$ID2.y, na.rm = TRUE)) {
  reps <- dfc[dfc$ID2.y == id,]
  
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

## Dump messy file before cleaning
#date.today <- format(Sys.Date(), "%y%m%d")
#filePath.all <- paste(
#  "Output/selectedDataAllColumns_",
#  date.today,
#  ".csv",
#  sep = "")
#write.csv(dfg, filePath.all)
#
## Clean data
#col.keep <- c("Col", "Row2", "BarcodeFinal", "Crop", "TotalGrain.g.",
#              "protein", "moisture", "starch", "gluten", "testWeight",
#              "NOTES", "Notes2", "Notes3", "ID2.y", "coords.x1", "coords.x2")
#dfg.slim <- dfg[, col.keep]
#dfg.slim <- within(dfg.slim, NotesKeep <- paste(NOTES, Notes2, Notes3,
#                                                sep = " | "))
##col.keep$Notes <- paste(dfg.slim$NOTES, dfg.slim$Notes2, dfg.slim$Notes3,
##                        sep = " | ")
#dfg.slim$NOTES <- NULL
#dfg.slim$Notes2 <- NULL
#dfg.slim$Notes3 <- NULL
#names(dfg.slim) <- c("Column", "Row2", "SampleID", "Crop", "GrainWeightWet",
#                     "Protein", "Moisture", "Starch", "WGlutDM", "TestWeight", "ID2",
#                     "Longitude", "Latitude", "Notes")
#dfg.slim$FieldID <- "CookEast"
#dfg.slim$Year <- 2014
#filePath.slim <- paste(
#  "Output/selectedDataSlimColumns_",
#  date.today,
#  ".csv",
#  sep = "")
#write.csv(dfg.slim, filePath.slim, row.names=FALSE)#