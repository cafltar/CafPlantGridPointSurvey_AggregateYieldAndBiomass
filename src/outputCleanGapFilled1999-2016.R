source("src/functions.R")
source("src/cleaningFunctions.R")
source("src/gapFillingFunctions.R")
source("src/qualityControlChecks.R")

# ==== Cleaning and QA functions ====
# Get cleaned data, keep all extreme outliers
# Processing level = 2 (calculated), accuracy level = 1 (only QA checks)
df <- get_clean1999_2016(FALSE)

# Some values were found to be outliers, remove them here
# Processing level = 2 (calculated), accuracy = 4 (outlier check operated on whole dataset)
df.rm.outliers <- remove_calculated_values_manually(df)

# Fill missing dry values by estimating from wet values
# Processing level = 2 (these are calculation similar to how it is done in DET; agreed upon standard), accuracy = 4 (from previous)
dfConvertWetToDryByMoistureProportion <- estimateResidueMassDryXByResidueMoistureProportion(df.rm.outliers)

# ==== Gap filling functions ====
# Fill missing yield values
# Processing level = 3 (modeled), accuracy = 1 (many values have not been checked)
dfGapFillYield <- estimateYieldByAvgYieldAndRelativeYield(dfConvertWetToDryByMoistureProportion)

# Fill missing residue values
# Processing level = 3 (modeled), accuracy = 3 (bounds check on derived variable (harvest index))
dfGapFillByGrainMass <- estimateResidueMassDryPerAreaByGrainYieldDryPerArea(dfGapFillYield)

# ==== Clean up for output ====
# Clean up columns
# Note that even though we calculate a parameter to convert wet residue mass to dry this is not considered a "modeled" result since this is similar to what is done in the lab, thus meets criteria for a "calculated" result so is included in the "Processing Level 2" dataset
dfClean <- dfConvertWetToDryByMoistureProportion %>% 
  select(HarvestYear,
         ID2,
         Longitude,
         Latitude,
         SampleID,
         Crop,
         GrainYieldDryPerArea,
         GrainCarbon,
         GrainNitrogen,
         GrainProtein,
         GrainMoisture,
         GrainStarch,
         GrainWGlutDM,
         GrainOilDM,
         ResidueMassDryPerArea,
         ResidueCarbon,
         ResidueNitrogen,
         Comments)
dfCleanGapFilled <- dfGapFillByGrainMass %>% 
  select(HarvestYear,
         ID2,
         Longitude,
         Latitude,
         SampleID,
         Crop,
         GrainYieldDryPerArea,
         GrainYieldDryPerArea_P,
         GrainCarbon,
         GrainNitrogen,
         GrainProtein,
         GrainMoisture,
         GrainStarch,
         GrainWGlutDM,
         GrainOilDM,
         ResidueMassDryPerArea,
         ResidueMassDryPerArea_P,
         ResidueCarbon,
         ResidueNitrogen,
         CropExists,
         Comments)

varNames.P2 <- c("HarvestYear",
              "ID2",
              "Longitude",	
              "Latitude",
              "SampleID",
              "Crop",	
              "GrainYieldDryPerArea",	
              "GrainCarbon",	
              "GrainNitrogen",
              "GrainProtein",	
              "GrainMoisture",	
              "GrainStarch",	
              "GrainWGlutDM",	
              "GrainOilDM",
              "ResidueMassDryPerArea",	
              "ResidueCarbon",	
              "ResidueNitrogen",	
              "Comments")

varUnits.P2 <- c("unitless",
              "unitless",
              "dd",
              "dd",
              "unitless",
              "unitless",
              "g/m2",
              "%",
              "%",
              "%",
              "%",
              "%",
              "%",
              "%",
              "g/m2",
              "%",
              "%",
              "unitless")

varDesc.P2 <- c("Year sample was collected",
             "Number ID of georeference point near sample collection",
             "Longitude of georeference point near where sample was collected",
             "Latitude of georeference point near where sample was collected",
             "ID of sample",
             "Crop abbreviation where: Spring wheat = SW, Winter wheat = WW, Spring canola = SC, Winter canola = WC, Spring barley = SB, Spring pea = SP, Winter barley = WB, Winter pea = WP, Winter triticale = WT, Winter lentil = WL, Garbonzo Beans = GB, Alfalfa = AL",
             "Dry grain yield on a per area basis. Sample dried in greenhouse or oven, threshed, then weighed. Some moisture likely still present",
             "Percent carbon of dry grain mass",
             "Percent nitrogen of dry gain mass",
             "Percent of protein in grain",
             "Percent of moisture in dried grain",
             "Percent of starch in dried grain",
             "Percent of gluten in dried grain",
             "Percent of oil in dried grain",
             "Residue mass on a per area basis. Sample dried in greenhouse or oven. Residue = (biomass - grain mass) / area",
             "Percent carbon of dry residue mass",
             "Percent nitrogen of dry residue mass",
             "Comments, aggregated from various columns. '|' or ',' separates source")

varTypes.P2 <- c("Int",
              "Int",
              "Double",
              "Double",
              "String",
              "String",
              "Double",
              "Double",
              "Double",
              "Double",
              "Double",
              "Double",
              "Double",
              "Double",
              "Double",
              "Double",
              "Double",
              "String")

dictionary.P2 <- data.frame(varNames.P2, varUnits.P2, varDesc.P2, varTypes.P2) %>% 
  rename("FieldName" = varNames.P2,
         "Units" = varUnits.P2,
         "Description" = varDesc.P2,
         "DataType" = varTypes.P2)

varNames.P3 <- c("HarvestYear",
                 "ID2",
                 "Longitude",	
                 "Latitude",
                 "SampleID",
                 "Crop",	
                 "GrainYieldDryPerArea",
                 "GrainYieldDryPerArea_P",
                 "GrainCarbon",	
                 "GrainNitrogen",
                 "GrainProtein",	
                 "GrainMoisture",	
                 "GrainStarch",	
                 "GrainWGlutDM",	
                 "GrainOilDM",
                 "ResidueMassDryPerArea",
                 "ResidueMassDryPerArea_P",
                 "ResidueCarbon",	
                 "ResidueNitrogen",	
                 "CropExists",
                 "Comments")

varUnits.P3 <- c("unitless",
                 "unitless",
                 "dd",
                 "dd",
                 "unitless",
                 "unitless",
                 "g/m2",
                 "unitless",
                 "%",
                 "%",
                 "%",
                 "%",
                 "%",
                 "%",
                 "%",
                 "g/m2",
                 "unitless",
                 "%",
                 "%",
                 "unitless",
                 "unitless")

varDesc.P3 <- c("Year sample was collected",
                "Number ID of georeference point near sample collection",
                "Longitude of georeference point near where sample was collected",
                "Latitude of georeference point near where sample was collected",
                "ID of sample",
                "Crop abbreviation where: Spring wheat = SW, Winter wheat = WW, Spring canola = SC, Winter canola = WC, Spring barley = SB, Spring pea = SP, Winter barley = WB, Winter pea = WP, Winter triticale = WT, Winter lentil = WL, Garbonzo Beans = GB, Alfalfa = AL",
                "Dry grain yield on a per area basis. Sample dried in greenhouse or oven, threshed, then weighed. Some moisture likely still present",
                "Processing level of 'GrainYieldDryPerArea' values where: 1 = measured value, 2 = calculated values, 3 = modeled value",
                "Percent carbon of dry grain mass",
                "Percent nitrogen of dry gain mass",
                "Percent of protein in grain",
                "Percent of moisture in dried grain",
                "Percent of starch in dried grain",
                "Percent of gluten in dried grain",
                "Percent of oil in dried grain",
                "Residue mass on a per area basis. Sample dried in greenhouse or oven. Residue = (biomass - grain mass) / area",
                "Processing level of 'ResidueMassDryPerArea' values where: 1 = measured value, 2 = calculated values, 3 = modeled value",
                "Percent carbon of dry residue mass",
                "Percent nitrogen of dry residue mass",
                "Indication whether or not a crop was present at location: 1 = crop present, 2 = not present. Crop not present due to planting error, failed germination, weeds, etc. A value of 1 without data indicates missing sample",
                "Comments, aggregated from various columns. '|' or ',' separates source")

varTypes.P3 <- c("Int",
                 "Int",
                 "Double",
                 "Double",
                 "String",
                 "String",
                 "Double",
                 "Int",
                 "Double",
                 "Double",
                 "Double",
                 "Double",
                 "Double",
                 "Double",
                 "Double",
                 "Double",
                 "Int",
                 "Double",
                 "Double",
                 "Boolean",
                 "String")

dictionary.P3 <- data.frame(varNames.P3, varUnits.P3, varDesc.P3, varTypes.P3) %>% 
  rename("FieldName" = varNames.P3,
         "Units" = varUnits.P3,
         "Description" = varDesc.P3,
         "DataType" = varTypes.P3)

# Write dataset with only calculated values
write_csv_gridPointSurvey(dfClean, dictionary.P2, "1999-2016", "output", 4, 2)

# Write dataset with gap-filled (modeled) values
write_csv_gridPointSurvey(dfCleanGapFilled, dictionary.P3, "1999-2016", "output", 3, 3)

