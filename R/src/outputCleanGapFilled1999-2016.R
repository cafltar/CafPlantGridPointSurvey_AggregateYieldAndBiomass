source("src/functions.R")
source("src/cleaningFunctions.R")
source("src/gapFillingFunctions.R")
source("src/qualityControlChecks.R")

# ==== Cleaning and QA functions ====
# Get cleaned data, keep all extreme outliers
# Processing level = 2 (calculated), accuracy level = 1 (only QA checks)
df <- get_clean1999_2016(FALSE)

df.cropExists <- append_cropexists(df)

# Some values were found to be outliers, remove them here
# Processing level = 2 (calculated), accuracy = 1,4 (outlier check operated of HI on whole dataset) for residue and yield, but 1 for others
#df.rm.outliers <- remove_calculated_values_manually(df)

# Fill missing dry values by estimating from wet values
# Processing level = 2 (these are calculation similar to how it is done in DET; agreed upon standard), accuracy = 1,4 (from previous)
#dfConvertWetToDryByMoistureProportion <- estimateResidueMassDryXByResidueMoistureProportion(df.rm.outliers)

# ==== Gap filling functions ====
# Fill missing yield values
# Processing level = 3 (modeled), accuracy = 1 (many values have not been checked)
#dfGapFillYield <- estimateYieldByAvgYieldAndRelativeYield(dfConvertWetToDryByMoistureProportion)

# Fill missing residue values
# Processing level = 3 (modeled), accuracy = 1,3 (bounds check on derived variable (harvest index)), but 1 for others
#dfGapFillByGrainMass <- estimateResidueMassDryPerAreaByGrainYieldDryPerArea(dfGapFillYield)

# ==== Output ====
# Clean up columns
dfClean_P1 <- df.cropExists %>%
  select(HarvestYear,
         ID2,
         Longitude,
         Latitude,
         SampleID,
         Crop,
         GrainSampleArea,
         GrainMassWet,
         GrainMassWetInGrainSample,
         GrainMassOvenDryInGrainSample,
         GrainMassAirDry,
         GrainMassOvenDry,
         GrainMoisture,
         GrainProtein,
         GrainStarch,
         GrainWGlutDM,
         GrainOilDM,
         GrainTestWeight,
         GrainCarbon,
         GrainNitrogen,
         GrainSulfur,
         BiomassSampleArea,
         BiomassWet,
         BiomassAirDry,
         GrainMassWetInBiomassSample,
         GrainMassOvenDryInBiomassSample,
         ResidueMassWetSubsample,
         ResidueMassOvenDrySubsample,
         ResidueCarbon,
         ResidueNitrogen,
         ResidueSulfur,
         CropExists,
         Comments) %>% 
  arrange(HarvestYear, ID2)

write_csv_gridPointSurvey(dfClean_P1, NULL, "1999-2016", "output", 0, 1)

# Note that even though we calculate a parameter to convert wet residue mass to dry this is not considered a "modeled" result since this is similar to what is done in the lab, thus meets criteria for a "calculated" result so is included in the "Processing Level 2" dataset
#dfClean_P2 <- dfGapFillByGrainMass %>% 
#  mutate(ResidueMassDryPerArea = ResidueMassDryPerArea_P2) %>% 
#  select(HarvestYear,
#         ID2,
#         Longitude,
#         Latitude,
#         SampleID,
#         Crop,
#         GrainYieldDryPerArea,
#         GrainCarbon,
#         GrainNitrogen,
#         GrainProtein,
#         GrainMoisture,
#         GrainStarch,
#         GrainWGlutDM,
#         GrainOilDM,
#         GrainTestWeight,
#         ResidueMassDryPerArea,
#         ResidueCarbon,
#         ResidueNitrogen,
#         CropExists,
#         Comments)
#
#dfClean_P3 <- dfGapFillByGrainMass %>% 
#  mutate(ResidueMassDryPerArea = ResidueMassDryPerArea_P3,
#         GrainYieldDryPerArea = GrainYieldDryPerArea_P3) %>% 
#  select(HarvestYear,
#         ID2,
#         Longitude,
#         Latitude,
#         SampleID,
#         Crop,
#         GrainYieldDryPerArea,
#         GrainCarbon,
#         GrainNitrogen,
#         GrainProtein,
#         GrainMoisture,
#         GrainStarch,
#         GrainWGlutDM,
#         GrainOilDM,
#         GrainTestWeight,
#         ResidueMassDryPerArea,
#         ResidueCarbon,
#         ResidueNitrogen,
#         CropExists,
#         Comments)
#
#varNames <- c("HarvestYear",
#              "ID2",
#              "Longitude",	
#              "Latitude",
#              "SampleID",
#              "Crop",	
#              "GrainYieldDryPerArea",	
#              "GrainCarbon",	
#              "GrainNitrogen",
#              "GrainProtein",	
#              "GrainMoisture",	
#              "GrainStarch",	
#              "GrainWGlutDM",	
#              "GrainOilDM",
#              "GrainTestWeight",
#              "ResidueMassDryPerArea",	
#              "ResidueCarbon",	
#              "ResidueNitrogen",
#              "CropExists",
#              "Comments")
#
#varUnits <- c("unitless",
#              "unitless",
#              "dd",
#              "dd",
#              "unitless",
#              "unitless",
#              "g/m2",
#              "%",
#              "%",
#              "%",
#              "%",
#              "%",
#              "%",
#              "%",
#              "lb/bushel",
#              "g/m2",
#              "%",
#              "%",
#              "unitless",
#              "unitless")
#
#varDesc <- c("Year sample was collected",
#             "Number ID of georeference point near sample collection",
#             "Longitude of georeference point near where sample was collected",
#             "Latitude of georeference point near where sample was collected",
#             "ID of sample",
#             "Crop abbreviation where: Spring wheat = SW, Winter wheat = WW, Spring canola = SC, Winter canola = WC, Spring barley = SB, Spring pea = SP, Winter barley = WB, Winter pea = WP, Winter triticale = WT, Winter lentil = WL, Garbonzo Beans = GB, Alfalfa = AL",
#             "Dry grain yield on a per area basis. Sample dried in greenhouse or oven, threshed, then weighed. Some moisture likely still present",
#             "Percent carbon of dry grain mass",
#             "Percent nitrogen of dry gain mass",
#             "Percent of protein in grain",
#             "Percent of moisture in dried grain",
#             "Percent of starch in dried grain",
#             "Percent of gluten in dried grain",
#             "Percent of oil in dried grain",
#             "Test weight of grain, as an indicator of grain quality",
#             "Residue mass on a per area basis. Sample dried in greenhouse or oven. Residue = (biomass - grain mass) / area",
#             "Percent carbon of dry residue mass",
#             "Percent nitrogen of dry residue mass",
#             "Indication whether or not a crop was present at location: 1 = crop present, 0 = not present. Crop not present due to planting error, failed germination, weeds, etc. A value of 1 without data indicates missing sample",
#             "Comments, aggregated from various columns. '|' or ',' separates source")
#
#varTypes <- c("Int",
#              "Int",
#              "Double",
#              "Double",
#              "String",
#              "String",
#              "Double",
#              "Double",
#              "Double",
#              "Double",
#              "Double",
#              "Double",
#              "Double",
#              "Double",
#              "Double",
#              "Double",
#              "Double",
#              "Double",
#              "Int",
#              "String")
#
#dictionary <- data.frame(varNames, varUnits, varDesc, varTypes) %>% 
#  rename("FieldName" = varNames,
#         "Units" = varUnits,
#         "Description" = varDesc,
#         "DataType" = varTypes)
#
## Write dataset with only calculated values
## processing = 2 because calculations of residue, yield per area; accuracy = 1 because has data that have not been vetted (even though yield/residue are at higher acc)
#write_csv_gridPointSurvey(dfClean_P2, dictionary, "1999-2016", "output", 1, 2)
#
## Write dataset with gap-filled (modeled) values
#write_csv_gridPointSurvey(dfClean_P3, dictionary, "1999-2016", "output", 1, 3)
#
## Dump all data, with intermediate calculations
#write_csv_gridPointSurvey(dfGapFillByGrainMass, NULL, "1999-2016", "output", 1, 3, "IntermediateValues")

