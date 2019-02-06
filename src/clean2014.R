# Author: Bryan Carlson
# Contact: bryan.carlson@usda.gov
# Purpose: Select "best" data from various conflicting values from HY2014 data.  Derived from [GPHY2017selectBestData.R](https://github.com/cafltar/CookEastPlantHandHarvest_R/blob/master/cleaningHY2014/GPHY2017selectBestData.R) (note, filename is misnomer, should be HY2014)

# ---- Setup ----
library(tidyverse)

source("src/functions.R")

df2014 <- read.csv("input/HY2014GB_aggregate_all_data_171122.csv") %>% 
  mutate(ShouldKeep = NULL)

df <- append_georef_to_df(df2014, "Row2", "Col") %>% 
  rename(ID2 = ID2.y)

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

# Calculate biomass if value does not exist then keep only rows with biomass
dfcBiomass <- dfc %>% 
  mutate(biomass = case_when(is.na(TotalBiomass.g.) ~ Tota.Biomass.g..bag - TareBag.g..1,
    TRUE ~ TotalBiomass.g.)) %>% 
  filter(!is.na(biomass)) %>% 
  select(biomass, ID2)

# Merge biomass with main df
dfdBiomass <- left_join(dfg, dfcBiomass, by = c("ID2"))

# Calc per area
# NOTE: The actual area harvested was not recorded for 2014. It is likely that confusion over SOP caused area to be 1 m x 2 m or 2 m2 (harvesting within area inside pvc pipes instead of four rows of 2 m lenght - as was previous methods).  After mapping yields and comparing between years, Dave Huggins decided that 2014 was likely harvested at 2 m2. See notes in "CookEastPlantHandHarvest" OneNote (Bryan's WSU account)
df.calc <- dfdBiomass %>% 
  mutate(GrainYieldDryPerArea = TotalGrain.g. / 2,
         ResidueMassDryPerArea = (biomass - TotalGrain.g.) / 2,
         Comments = paste(NOTES, Notes2, Notes3,
                          sep = " | "),
         HarvestYear = 2014)
  
## Clean data
df.clean <- df.calc %>% 
  rename("SampleID" = BarcodeFinal,
         "GrainMassDryPerArea" = TotalGrain.g.,
         "GrainProtein" = protein,
         "GrainMoisture" = moisture,
         "GrainStarch" = starch,
         "GrainWGlutDM" = gluten,
         "Longitude" = X,
         "Latitude" = Y) %>% 
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
         ResidueMassDryPerArea,
         Comments)

write_csv_gridPointSurvey(df.clean, 2014)
