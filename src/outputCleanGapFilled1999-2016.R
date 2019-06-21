source("src/functions.R")
source("src/cleaningFunctions.R")
source("src/gapFillingFunctions.R")

df1999_2016 <- get_clean1999_2016(FALSE)
dfGapFillByMoistureProportion <- estimateResidueMassDryXByResidueMoistureProportion(df1999_2016)
dfGapFillByGrainMass <- estimateResidueMassDryPerAreaByGrainYieldDryPerArea(dfGapFillByMoistureProportion)
df1999_2016GapFilled <- dfGapFillByGrainMass #%>% 
  #select(-GrainSampleArea, -ResidueSampleArea, -ResidueMassWet, -ResidueMassDry, -ResidueMoistureProportion, -InterceptEstimate, -XEstimate)

varNames <- c("HarvestYear",	
              "Crop",	
              "Longitude",	
              "Latitude",	
              "ID2",	
              "GrainYieldDryPerArea",	
              "GrainCarbon",	
              "GrainNitrogen",	
              "ResidueMassDryPerArea",	
              "ResidueCarbon",	
              "ResidueNitrogen",	
              "Comments",	
              "SampleID",	
              "GrainProtein",	
              "GrainMoisture",	
              "GrainStarch",	
              "GrainWGlutDM",	
              "GrainOilDM")

varUnits <- c("unitless",
              "unitless",
              "dd",
              "dd",
              "unitless",
              "g/m2",
              "%",
              "%",
              "g/m2",
              "%",
              "%",
              "unitless",
              "unitless",
              "%",
              "%",
              "%",
              "%",
              "%")

varDesc <- c("Year sample was collected",
             "Crop abbreviation where: Spring wheat = SW, Winter wheat = WW, Spring canola = SC, Winter canola = WC, Spring barley = SB, Spring pea = SP, Winter barley = WB, Winter pea = WP, Winter triticale = WT, Winter lentil = WL, Garbonzo Beans = GB, Alfalfa = AL",
             "Longitude of georeference point near where sample was collected",
             "Latitude of georeference point near where sample was collected",
             "Number ID of georeference point near sample collection",
             "Dry grain yield on a per area basis. Sample dried in greenhouse or oven, threshed, then weighed",
             "Percent carbon of dry grain mass",
             "Percent nitrogen of dry gain mass",
             "Residue mass on a per area basis. Residue = (biomass - grain mass) / area. Some values calculated from a model.",
             "Percent carbon of dry residue mass",
             "Percent nitrogen of dry residue mass",
             "Comments, aggregated from various columns. '|' or ',' separates source",
             "ID of sample",
             "Percent of protein in grain",
             "Percent of moisture in dried grain",
             "Percent of starch in dried grain",
             "Percent of gluten in dried grain",
             "Percent of oil in dried grain")

varTypes <- c("Int",
              "String",
              "Double",
              "Double",
              "Int",
              "Double",
              "Double",
              "Double",
              "Double",
              "Double",
              "Double",
              "String",
              "String",
              "Double",
              "Double",
              "Double",
              "Double",
              "Double")

data.frame(varNames, varUnits, varDesc, varTypes) %>% 
  rename("FieldName" = varNames,
         "Units" = varUnits,
         "Description" = varDesc,
         "DataType" = varTypes) %>% 
  write_csv_gridPointSurvey("1999-2016_DataDictionary")

write_csv_gridPointSurvey(df1999_2016GapFilled, "1999-2016")

