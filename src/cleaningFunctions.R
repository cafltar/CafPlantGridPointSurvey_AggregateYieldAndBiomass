get_dirty1999_2009 <- function() {
  # WARNING: This function is depricated and only here to run legacy code
  #
  # Note that quality/verification checks are scrubbed out of this function. See "checkUnger1999-2009.R" for such tests.
  require(tidyverse)
  require(readxl)
  require(sf)
  
  source("src/functions.R")
  
  # Load data
  df.ubbie <- read_excel(
    "input/Cook Farm All years all points.xlsx",
    na = c("-99999", "-77777")) %>% 
    filter(Project == "grid points" | Project == "Grid Points")
  
  # Get all data from Unger, merge together
  df.unger <- bind_rows(
    getUngerDF("1999"),
    getUngerDF("2000"),
    getUngerDF("2001"),
    getUngerDF("2002"),
    getUngerDF("2003"),
    getUngerDF("2004"),
    getUngerDF("2005"),
    getUngerDF("2006"),
    getUngerDF("2007"),
    getUngerDF("2008"),
    getUngerDF("2009"))
  
  df.merged <- df.ubbie %>% 
    mutate(Column = as.integer(Column)) %>% 
    left_join(df.unger, by = c("Year", "Column" , "Row Letter" = "Row2"))
  
  # This does not remove outliers and accepts Ungers %C and %N over Ubbies, and Ubbies grain mass over Unger's
  df.merged.rm.zeros <- df.merged %>% 
    group_by(Year, Crop) %>% 
    mutate(GrainNUbbie = case_when(`Grain Nitrogen %...19` > 0 & `Grain Nitrogen %...19` <= 100 ~ `Grain Nitrogen %...19`),
           GrainNUnger = case_when(GrainNitrogen > 0 & GrainNitrogen <= 100 ~ GrainNitrogen,
                                   Year == 1999 & Crop == "SW" ~ NA_real_,
                                   Year == 2001 & Crop == "WW" ~ NA_real_)) %>% 
    mutate(GrainNFinal = case_when(!is.na(GrainNUbbie) & !is.na(GrainNUnger) ~ GrainNUnger,
                                   is.na(GrainNUbbie) & !is.na(GrainNUnger) ~ GrainNUnger,
                                   !is.na(GrainNUbbie) & is.na(GrainNUnger) ~ GrainNUbbie)) %>% 
    mutate(GrainCUbbie = case_when(`Grain Carbon %...21` > 0 & `Grain Carbon %...21` <= 100 ~ `Grain Carbon %...21`),
           GrainCUnger = case_when(GrainCarbon > 0 & GrainCarbon <= 100 ~ GrainCarbon,
                                   Year == 1999 & Crop == "SW" ~ NA_real_,
                                   Year == 2001 & Crop == "WW" ~ NA_real_)) %>% 
    mutate(GrainCFinal = case_when(!is.na(GrainCUbbie) & !is.na(GrainCUnger) ~ GrainCUnger,
                                   is.na(GrainCUbbie) & !is.na(GrainCUnger) ~ GrainCUnger,
                                   !is.na(GrainCUbbie) & is.na(GrainCUnger) ~ GrainCUbbie)) %>% 
    mutate(GrainMassUbbie = case_when(`total grain yield dry (grams)` > 0 ~ `total grain yield dry (grams)`)) %>% 
    mutate(`total area harvested (M2)` = na_if(`total area harvested (M2)`, 0)) %>% 
    mutate(`Residue plus Grain Wet Weight (grams)` = na_if(`Residue plus Grain Wet Weight (grams)`, 0)) %>% 
    mutate(`Residue Sub-Sample Dry Weight (grams)` = na_if(`Residue Sub-Sample Dry Weight (grams)`, 0)) %>% 
    mutate(`Residue Sub-Sample Wet Weight (grams)` = na_if(`Residue Sub-Sample Wet Weight (grams)`, 0)) %>% 
    mutate(`Residue Sample Area (square meters)` = na_if(`Residue Sample Area (square meters)`, 0)) %>% 
    mutate(GrainMassFinal = GrainMassUbbie) %>% 
    ungroup()
  
  # Merge lat/lon data based on col and row2
  df <- append_georef_to_df(df.merged.rm.zeros, 
                            "Row Letter", 
                            "Column")
  
  # Remove ID2 values with NA (fields north of current CookEast used to be sampled)
  # Calculate residue values, don't use supplied since it seems to have errors
  df.calc <- df %>%
    mutate(GrainYieldDryPerArea = GrainMassFinal / `total area harvested (M2)`) %>% 
    mutate(ResidueMassWet = `Residue plus Grain Wet Weight (grams)` - `Residue sample Grain Wet Weight (grams)`) %>%
    mutate(ResidueMoistureProportion = (`Residue Sub-Sample Wet Weight (grams)` - `Residue Sub-Sample Dry Weight (grams)`) / `Residue Sub-Sample Wet Weight (grams)`) %>%
    mutate(ResidueMassDry = ResidueMassWet * (1 - ResidueMoistureProportion)) %>%
    mutate(ResidueMassDryPerArea = ResidueMassDry / `Residue Sample Area (square meters)`)
  
  return(df.calc)
}

get_clean1999_2009 <- function() {
  # Loads legecy data from one or more datasets and outputs cleaned dataframe
  # Note that quality/verification checks are scrubbed out of this function. See "checkUnger1999-2009.R" for such tests.
  
  require(tidyverse)
  require(readxl)
  require(sf)
  
  source("src/functions.R")
  
  # Load data, set nulls and filter for only samples from georef points
  df.ubbie <- read_excel(
    "input/Cook Farm All years all points.xlsx",
    na = c("-99999", "-77777")) %>% 
    filter(Project == "grid points" | Project == "Grid Points")
  
  # Get all data from Unger, merge together
  df.unger <- bind_rows(
    getUngerDF("1999"),
    getUngerDF("2000"),
    getUngerDF("2001"),
    getUngerDF("2002"),
    getUngerDF("2003"),
    getUngerDF("2004"),
    getUngerDF("2005"),
    getUngerDF("2006"),
    getUngerDF("2007"),
    getUngerDF("2008"),
    getUngerDF("2009"))
  
  df.merged <- df.ubbie %>% 
    mutate(Column = as.integer(Column)) %>% 
    left_join(df.unger, by = c("Year", "Column" , "Row Letter" = "Row2"))
  
  # Set variables to best available data given various conflicting datasets
  # This does not remove outliers and accepts Ungers %C and %N over Ubbies, and Ubbies grain mass over Unger's
  df.merged.rm.zeros <- df.merged %>% 
    group_by(Year, Crop) %>% 
    mutate(GrainNUbbie = case_when(`Grain Nitrogen %...19` > 0 & `Grain Nitrogen %...19` <= 100 ~ `Grain Nitrogen %...19`),
           GrainNUnger = case_when(GrainNitrogen > 0 & GrainNitrogen <= 100 ~ GrainNitrogen,
                                   Year == 1999 & Crop == "SW" ~ NA_real_,
                                   Year == 2001 & Crop == "WW" ~ NA_real_)) %>% 
    mutate(GrainNFinal = case_when(!is.na(GrainNUbbie) & !is.na(GrainNUnger) ~ GrainNUnger,
                                   is.na(GrainNUbbie) & !is.na(GrainNUnger) ~ GrainNUnger,
                                   !is.na(GrainNUbbie) & is.na(GrainNUnger) ~ GrainNUbbie)) %>% 
    mutate(GrainCUbbie = case_when(`Grain Carbon %...21` > 0 & `Grain Carbon %...21` <= 100 ~ `Grain Carbon %...21`),
           GrainCUnger = case_when(GrainCarbon > 0 & GrainCarbon <= 100 ~ GrainCarbon,
                                   Year == 1999 & Crop == "SW" ~ NA_real_,
                                   Year == 2001 & Crop == "WW" ~ NA_real_)) %>% 
    mutate(GrainCFinal = case_when(!is.na(GrainCUbbie) & !is.na(GrainCUnger) ~ GrainCUnger,
                                   is.na(GrainCUbbie) & !is.na(GrainCUnger) ~ GrainCUnger,
                                   !is.na(GrainCUbbie) & is.na(GrainCUnger) ~ GrainCUbbie)) %>% 
    #mutate(GrainMassUbbie = case_when(`total grain yield dry (grams)` > 0 ~ `total grain yield dry (grams)`)) %>% 
    #mutate(GrainMassWetUbbie = case_when(`total grain yield wet(grams)` > 0 ~ `total grain yield wet(grams)`)) %>% 
    #mutate(`total area harvested (M2)` = na_if(`total area harvested (M2)`, 0)) %>% 
    #mutate(`Residue plus Grain Wet Weight (grams)` = na_if(`Residue plus Grain Wet Weight (grams)`, 0)) %>% 
    #mutate(`Residue Sub-Sample Dry Weight (grams)` = na_if(`Residue Sub-Sample Dry Weight (grams)`, 0)) %>% 
    #mutate(`Residue Sub-Sample Wet Weight (grams)` = na_if(`Residue Sub-Sample Wet Weight (grams)`, 0)) %>% 
    #mutate(`Residue Sample Area (square meters)` = na_if(`Residue Sample Area (square meters)`, 0)) %>% 
    #mutate(GrainMassFinal = GrainMassUbbie) %>% 
    mutate(GrainSampleArea = `Non-Residue Grain Sample Area (square meters)`) %>%
    mutate(BiomassSampleArea =  `Residue Sample Area (square meters)`) %>%
    mutate(GrainMassWetInGrainSample = case_when(GrainSampleArea > 0 & BiomassSampleArea > 0 ~ NA_real_,
                                               TRUE ~ `Non-Residue Grain Wet Weight (grams)`)) %>%
    mutate(GrainMassDryInGrainSample = case_when(GrainSampleArea > 0 & BiomassSampleArea > 0 ~ NA_real_,
                                               TRUE ~ `Non-Residue grain dry weight (grams)`)) %>%
    mutate(BiomassWet = `Residue plus Grain Wet Weight (grams)`) %>%
    mutate(GrainMassWetInBiomassSample = `Residue sample Grain Wet Weight (grams)`) %>%
    mutate(GrainMassOvenDryInBiomassSample = case_when(GrainSampleArea > 0 & BiomassSampleArea > 0 ~ NA_real_,
                                                       TRUE ~ `Residue sample Grain Dry Weight (grams)`)) %>%
    mutate(ResidueMassWetSubsample = `Residue Sub-Sample Wet Weight (grams)`) %>%
    mutate(ResidueMassOvenDrySubsample = `Residue Sub-Sample Dry Weight (grams)`) %>%
    mutate(GrainMassWet = case_when(GrainSampleArea > 0 & BiomassSampleArea > 0 ~ `total grain yield wet(grams)`,
                                    TRUE ~ NA_real_)) %>%
    mutate(GrainMassOvenDry = case_when(GrainSampleArea > 0 & BiomassSampleArea > 0 ~ `total grain yield dry (grams)`,
                                    TRUE ~ NA_real_)) %>%
    
    ungroup()
  
  
  # After reviewing raw dataset, found that harvest data was assigned to wrong georef point, reassigned here
  df.fixed.points <- df.merged.rm.zeros %>% 
    mutate(`Row Letter` = case_when(Year == 2008 & `Sample Location ID` == 338 ~ "H",
                                    Year == 2008 & `Sample Location ID` == 192 ~ NA_character_,
                                    TRUE ~ `Row Letter`),
           Column = case_when((Year == 2008 & `Sample Location ID` == 338) ~ as.integer(23),
                                (Year == 2008 & `Sample Location ID` == 192) ~ NA_integer_,
                                TRUE ~ as.integer(Column)),
           `Strip` = case_when(Year == 2008 & `Sample Location ID` == 338 ~ 2,
                               TRUE ~ as.numeric(`Strip`)))
  
  # Merge lat/lon data based on col and row2
  df <- append_georef_to_df(df.fixed.points, 
                            "Row Letter", 
                            "Column")
  
  # Convert all crop designations to standards, remove measurements outside of CE
  df.standard.crop.abbriv <- df %>% 
    filter(!is.na(ID2), Crop != "FALLOW") %>%
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
    replace(. == "Alfalfa", "AL")
  
  # After reviewing crop management files, found that WC failed in 2001, 2004, and 2008 and was planted as SC, reassigned here
  df.wc.to.sc <- df.standard.crop.abbriv %>% 
    mutate(Crop = case_when(Year == 2001 & Field == "A" & Strip == 3 ~ "SC",
                            Year == 2004 & Field == "A" & Strip == 3 ~ "SC",
                            Year == 2008 & Field == "A" & Strip == 4 ~ "SC",
                            TRUE ~ Crop))
  
  # Converting A3 to SC converts a mis-planted SP sample, so reassignign it here
  # Also note that there's a misplanted SW in C1 (ID2 = 110), WB in A2 (ID2 = 394), SP in A4 (398), WB in A2 (419) see comments for details
  df.fix.planting.mistakes <- df.wc.to.sc %>% 
    mutate(Crop = case_when(Year == 2001 & ID2 == 227 ~ "WP",
                            TRUE ~ Crop))
  
  # Remove ID2 values with NA (fields north of current CookEast used to be sampled)
  # Calculate residue values, don't use supplied since it seems to have errors
#  df.calc <- df.fix.planting.mistakes %>%
#    mutate(GrainYieldOvenDryPerArea = GrainMassFinal / `total area harvested (M2)`) %>% 
#    mutate(ResidueMassWet = `Residue plus Grain Wet Weight (grams)` - `Residue sample Grain Wet Weight (grams)`) %>%
#    mutate(ResidueMoistureProportion = (`Residue Sub-Sample Wet Weight (grams)` - `Residue Sub-Sample Dry Weight (grams)`) / `Residue Sub-Sample Wet Weight (grams)`) %>%
#    mutate(ResidueMassOvenDry = ResidueMassWet * (1 - ResidueMoistureProportion)) %>%
#    mutate(ResidueMassOvenDryPerArea = ResidueMassOvenDry / `Residue Sample Area (square meters)`)

  # Remove values with no ID and if fallow
  # Aggregate comments, rename crops for consistency
  # Accept Unger's crop abbreviations over Ubbies (Dave and I reviewed)
  # Add missing residue using crop averages of moisture proportion (2023-05-04: I think this is not being done here)
  df.clean <- df.fix.planting.mistakes %>% #df.calc %>% 
    mutate(Comments = coalesce(`Grain Harvest Comments`, `Residue Sample Comments`)) %>%
    mutate(HarvestYear = as.integer(Year)) %>%
    rename(GrainCarbonUnger = GrainCarbon,
           GrainNitrogenUnger = GrainNitrogen,
           Latitude = Y,
           Longitude = X,
           GrainCarbon = GrainCFinal,
           GrainNitrogen = GrainNFinal,
           ResidueCarbon = `Residue Carbon %`,
           ResidueNitrogen = `Residue Nitrogen %`,
           #GrainSampleArea = `total area harvested (M2)`,
           #GrainSampleArea = `Non-Residue Grain Sample Area (square meters)`,
           #GrainMassWet = GrainMassWetUbbie,
           #GrainMassWet = `Non-Residue Grain Wet Weight (grams)`,
           #BiomassSampleArea = `Residue Sample Area (square meters)`,
           #GrainMassOvenDry = GrainMassFinal,
           #GrainMassOvenDry = `Non-Residue grain dry weight (grams)`,
           #BiomassWet = `Residue plus Grain Wet Weight (grams)`,
           #GrainMassWetInBiomassSample = `Residue sample Grain Wet Weight (grams)`,
           #GrainMassOvenDryInBiomassSample = `Residue sample Grain Dry Weight (grams)`,
           #ResidueMassWetSubsample = `Residue Sub-Sample Wet Weight (grams)`,
           #ResidueMassOvenDrySubsample = `Residue Sub-Sample Dry Weight (grams)`
           ) %>% 
    select(HarvestYear,
           Crop,
           Longitude,
           Latitude,
           ID2,
           GrainSampleArea,
           GrainMassWet,
           GrainMassOvenDry,
           #GrainYieldOvenDryPerArea,
           GrainCarbon,
           GrainNitrogen,
           BiomassSampleArea,
           BiomassWet,
           GrainMassWetInBiomassSample,
           GrainMassOvenDryInBiomassSample,
           #ResidueMassWet,
           ResidueMassWetSubsample,
           ResidueMassOvenDrySubsample,
           #ResidueMassOvenDry,
           #ResidueMoistureProportion,
           #ResidueMassOvenDryPerArea,
           ResidueCarbon,
           ResidueNitrogen,
           Comments) %>% 
    arrange(HarvestYear, ID2)
  
  return(df.clean)
}

get_clean2010 <- function() {
  # Loads legacy data from one or more datasets and outputs cleaned dataframe
  
  require(tidyverse)
  require(readxl)
  require(sf)
  
  source("src/functions.R")
  
  # Read data, harmonize barcode labels to be uppercase
  df2010 <- read_excel("input/Grid Points Yields and Residue 2010.xls") %>% 
    mutate(`Bag Barcode` = toupper(`Bag Barcode`))
  df.res <- read_excel("input/Yields and Residue 2010 Final.xls", 
                       "All Data", 
                       skip = 1, 
                       n_max = 1157) %>% 
    filter(Project == "GP") %>% 
    mutate(`Bag Barcode` = toupper(`Bag Barcode`))
  
  # Check that grain mass match
  #df.grain.check <- df2010 %>% 
  #  full_join(df.res, by = c("Bag Barcode", 
  #                           "Farm", 
  #                           "Year", 
  #                           "Project", 
  #                           "Grain Type")) %>% 
  #  filter(`Total Grain Dry (g).x` != `Total Grain Dry (g).y`)
  #if(nrow(df.grain.check) > 0) { warning("Grain masses don't match; review grain.check") }
  
  # Merge the georef data based on col and row2
  df <- append_georef_to_df(df2010, "Row", "Column")
  
  # Check that ID2 values are ok after merging with row2 and col (it's not)
  #df.id2.check <- df %>% 
  #  filter(df$UID != df$ID2)
  #if(nrow(df.id2.check) > 0) { warning("Error in ID2, Row2, Col") }
  
  # There are values where ID2, Row2, and Col do not mesh, after review, choose ID2, not UID
  
  # Calculate some missing values and trim columns
  df.res.slim <- df.res %>% 
    filter(!is.na(`Residue Sub Wet (g)`),
           !is.na(`Total Biomass Wet (g)`),
           !is.na(`Total Grain Wet (g)`),
           `Residue Sub Wet (g)` > 0) %>% 
    #mutate(ResidueMassWet = `Total Biomass Wet (g)` - as.numeric(`Total Grain Wet (g)`)) %>% 
    #mutate(ResidueMoistureProportion = (`Residue Sub Wet (g)` - `Sub Res Dry (g)`) / `Residue Sub Wet (g)`) %>% 
    #mutate(ResidueMassOvenDry = ResidueMassWet * (1 - ResidueMoistureProportion)) %>% 
    rename(ResidueMassWetSubsample = `Residue Sub Wet (g)`,
           ResidueMassOvenDrySubsample = `Sub Res Dry (g)`,
           BiomassWet = `Total Biomass Wet (g)`) %>%
    select(`Bag Barcode`, 
           #ResidueMassOvenDry, 
           #ResidueMassWet, 
           BiomassWet,
           ResidueMassWetSubsample,
           ResidueMassOvenDrySubsample)
           #ResidueMoistureProportion)
  
  # Merge residue data and calc values
  df.merge <- df %>% 
    full_join(df.res.slim, by = c("Bag Barcode")) #%>%
    #mutate(GrainYieldOvenDryPerArea = `Total Grain Dry (g)` / `Area (m2)`) %>% 
    #mutate(ResidueMassOvenDryPerArea = ResidueMassOvenDry / `Area (m2)`)
  
  # After reviewing management data, determined that strip A1 should be WW, not SW
  df.sw.to.ww <- df.merge %>% 
    mutate(`Grain Type` = case_when(Field == "A" & Strip == 1 ~ "WW",
                                    TRUE ~ `Grain Type`))
  
  # Rename columns to standard and drop unneeded columns
  df.clean <- df.sw.to.ww %>% 
    rename(HarvestYear = Year,
           Crop = `Grain Type`,
           SampleID = `Bag Barcode`,
           Latitude = Y,
           Longitude = X,
           GrainSampleArea = `Area (m2)`,
           GrainMassWet = `Total Grain Wet (g)`,
           GrainMassOvenDry = `Total Grain Dry (g)`) %>% 
    mutate(BiomassSampleArea = GrainSampleArea) %>% 
    select(HarvestYear,
           Crop,
           SampleID,
           Latitude,
           Longitude,
           ID2,
           GrainSampleArea,
           GrainMassWet,
           GrainMassOvenDry,
           #GrainYieldOvenDryPerArea,
           BiomassSampleArea,
           BiomassWet,
           #ResidueMassWet,
           ResidueMassWetSubsample,
           ResidueMassOvenDrySubsample,
           #ResidueMassOvenDry,
           #ResidueMoistureProportion,
           #ResidueMassOvenDryPerArea,
           Comments) %>% 
    filter(!is.na(ID2)) %>% 
    arrange(HarvestYear, ID2)
  
  return(df.clean)
}

get_clean2011 <- function() {
  # Loads legacy data from one or more datasets and outputs cleaned dataframe
  
  require(tidyverse)
  require(readxl)
  require(sf)
  
  source("src/functions.R")
  
  # Read data, filter out NAs, make SampleID to uppercase, arrange values
  df2011 <- read_excel("input/Yields and Residue HY2011 112311.xls", 
                       "Grid Point Yields Only") %>% 
    filter(!is.na(UID) | (!is.na(Row) & !is.na(Column))) %>% 
    mutate(SampleID = toupper(Barcode)) %>% 
    arrange(UID)
  
  # Merge the georef data based on col and row2
  df <- append_georef_to_df(df2011, "Row", "Column")
  
  # Check that ID2 values are ok after merging with row2 and col (it's not)
  #df.id2.check <- df %>% 
  #  filter(df$UID != df$ID2)
  #if(nrow(df.id2.check) > 0) { warning("Error in ID2, Row2, Col") }
  
  # There are values where ID2, Row2, and Col do not mesh, after review, chose ID2, not UID
  
  # Calc missing values
  df.calcs <- df %>% 
    mutate(Comments = case_when(is.na(as.numeric(df$`Total Residue and Grain Wet (g)`)) ~ paste("Residue note: ", df$`Total Residue and Grain Wet (g)`, sep = ""), TRUE ~ "")) %>% 
    mutate(Comments = case_when(is.na(as.numeric(df$`Total Grain Wet (g)`)) ~ paste(Comments, " | Grain note: ", df$`Total Grain Wet (g)`, sep = ""), TRUE ~ Comments)) %>% 
    mutate(BiomassWet = as.numeric(`Total Residue and Grain Wet (g)`)) %>% 
    mutate(GrainMassWet = as.numeric(`Total Grain Wet (g)`)) #%>% 
    #mutate(ResidueMassWet = BiomassWet - GrainMassWet) #%>% 
    #mutate(GrainYieldWetPerArea = GrainMassWet / `Area (m2)`)
  
  # Rename columns to standard, drop unwanted columns
  df.clean <- df.calcs %>% 
    rename(HarvestYear = Year,
           Crop = `Current Crop`,
           Longitude = X,
           Latitude = Y,
           GrainSampleArea = `Area (m2)`) %>% 
    mutate(BiomassSampleArea = GrainSampleArea) %>% 
    select(HarvestYear,
           Crop,
           SampleID,
           Longitude,
           Latitude,
           ID2,
           GrainSampleArea,
           GrainMassWet,
           #GrainYieldWetPerArea,
           BiomassSampleArea,
           BiomassWet,
           #ResidueMassWet,
           Comments)
  
  return(df.clean)
}

get_clean2012 <- function() {
  # Loads legecy data from one or more datasets and outputs cleaned dataframe
  
  require(tidyverse)
  require(readxl)
  require(sf)
  
  source("src/functions.R")
  
  # Read data, filter out NAs, make SampleID to uppercase, arrange values 
  df2012 <- read_excel("input/Yields and Residue 2012 011513.xlsx", 
                       "Grid Points",
                       skip = 1) %>% 
    filter(!is.na(ID2) | (!is.na(ROW2) & !is.na(COLUMN))) %>% 
    mutate(SampleID = toupper(Barcode)) %>% 
    arrange(ID2)
  
  # Merge the georef data based on col and row2
  df <- append_georef_to_df(df2012, "ROW2", "COLUMN")
  
  # Check that ID2 values are ok after merging with row2 and col
  #df.id2.check <- df %>% 
  #  filter(ID2.x != ID2.y)
  #if(nrow(df.id2.check) > 0) { warning("Error in ID2, Row2, Col") }
  
  # Calc missing values
  df.calcs <- df %>% 
    mutate(GrainMassWet = `Grain Weight Wet (g)`) %>%
    #mutate(GrainYieldWetPerArea = `Grain Weight Wet (g)` / `Area (m2)`) %>% 
    mutate(Comments = case_when((!is.na(df$`Test Weight`) & is.na(as.numeric(df$`Test Weight`))) ~ paste("TestWeight note: ", df$`Test Weight`, sep = ""), TRUE ~ "")) %>% 
    mutate(Comments = case_when(!is.na(df$...24) ~ paste(Comments, " | Sample note: ", df$...24, sep = ""), TRUE ~ Comments)) %>% 
    mutate(HarvestYear = 2012) %>% 
    #mutate(ResidueMassWet = `Total Biomass Wet (g)` - `Grain Weight Wet (g)`) %>% 
    mutate(GrainTestWeight = as.numeric(`Test Weight`))
  
  # Calc grain at 0 % moisture
  #df.calcs <- df.calcs %>%
  #  mutate(GrainMassOvenDry = case_when(
  #    !is.na(df.calcs$`Grain Weight Dry (g)`) ~ df.calcs$`Grain Weight Dry (g)`, 
  #    TRUE ~ (df.calcs$`Grain Weight Wet (g)` - (df.calcs$`Grain Weight Wet (g)` * (df.calcs$`Moisture` / 100)))))

  
  # Calc yield at 0% moisture
  #df.calcs <- df.calcs %>%
  #  mutate(GrainYieldOvenDryPerArea = df.calcs$GrainMassOvenDry / df.calcs$`Area (m2)`)
  
  # Clean and output
  df.clean <- df.calcs %>% 
    rename(Longitude = X,
           Latitude = Y,
           ID2 = ID2.x,
           GrainProtein = Protein,
           GrainMoisture = Moisture,
           GrainStarch = Starch,
           GrainWGlutDM = Gluten,
           GrainSampleArea = `Area (m2)`,
           BiomassWet = `Total Biomass Wet (g)`,
           GrainMassOvenDry = `Grain Weight Dry (g)`) %>% 
    mutate(BiomassSampleArea = GrainSampleArea) %>% 
    select(HarvestYear,
           Crop,
           SampleID,
           Longitude,
           Latitude,
           ID2,
           GrainSampleArea,
           GrainMassWet,
           GrainMassOvenDry,
           #GrainYieldOvenDryPerArea,
           GrainProtein,
           GrainMoisture,
           GrainStarch,
           GrainWGlutDM,
           BiomassSampleArea,
           #ResidueMassWet,
           BiomassWet,
           Comments) %>% 
    replace(. == "SL", "GB")
  
  return(df.clean)
}

get_clean2013 <- function() {
  # Loads legacy data from one or more datasets and outputs cleaned dataframe
  
  require(tidyverse)
  require(readxl)
  require(sf)
  require(readODS)
  
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
           BiomassAirDry = as.numeric(C),
           ResidueNitrogen = as.numeric(E),
           ResidueCarbon = as.numeric(`F`)) %>%
    select(SampleID,
           BiomassAirDry,
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
           BiomassAirDry = as.numeric(C),
           ResidueNitrogen = as.numeric(str_remove(D, " !")),
           ResidueCarbon = as.numeric(str_remove(E, " !"))) %>% 
    select(SampleID,
           BiomassAirDry,
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
  
  # Combine residue data
  dfResidue <- bind_rows(dfResidue1, dfResidue2, dfResidue3, dfResidue4) %>% 
    distinct() %>% 
    arrange(SampleID)
  
  # --- Double checking that previous merge (via Excel) of ID2 and lat/lon are consistent with current
  ## Merge the georef data based on col and row2
  #df.check <- append_georef_to_df(df2013, "Row2", "Column")
  ## Check that ID2 values are ok after merging with row2 and col
  #df.id2.check <- df.check %>% 
  #  filter(ID2.x != ID2.y)
  #if(nrow(df.id2.check) > 0) { warning("Error in ID2, Row2, Col") }
  #df.Long.check <- df.check %>% 
  #  filter(abs(Longitude - X) > 0.00001)
  #df.lat.check <- df.check %>% 
  #  filter(abs(Latitude - Y) > 0.00001)
  
  # Append residue and grain analysis
  df <- df2013 %>% 
    left_join(dfGrainCN, by = c("SampleID")) %>% 
    left_join(dfResidue, by = c("SampleID"))
  
  # GrainWeightWet is actually dry weight, when I named that column I considered greenhouse dried samples as still "wet" (it still has moisture).  Our lab considers it dry.  
  #df.calcs <- df %>% 
  #  mutate(ResidueMassAirDry = BiomassAirDry - GrainWeightWet) %>% 
  #  mutate(GrainYieldAirDryPerArea = GrainWeightWet / Area,
  #         ResidueMassAirDryPerArea = ResidueMassAirDry / Area)
  
  # Calculate oven dry using NIR data
  #df.calcs <- df.calcs %>%
  #  mutate(GrainMassOvenDry = GrainWeightWet - (GrainWeightWet * (Moisture / 100))) %>%
  #  mutate(GrainYieldOvenDryPerArea = GrainMassOvenDry / Area)
  
  # Clean
  df.clean <- df %>% 
    filter(!is.na(SampleID)) %>% 
    rename(HarvestYear = Year,
           GrainProtein = Protein,
           GrainMoisture = Moisture,
           GrainStarch = Starch,
           GrainWGlutDM = WGlutDM,
           Comments = Notes,
           GrainSampleArea = Area,
           GrainTestWeight = TestWeight,
           GrainMassAirDry = GrainWeightWet) %>% 
    mutate(BiomassSampleArea = GrainSampleArea) %>% 
    select(HarvestYear,
           Crop,
           SampleID,
           Longitude,
           Latitude,
           ID2,
           GrainSampleArea,
           GrainMassAirDry,
           #GrainMassOvenDry,
           #GrainYieldOvenDryPerArea,
           GrainCarbon,
           GrainNitrogen,
           GrainProtein,
           GrainMoisture,
           GrainStarch,
           GrainWGlutDM,
           GrainTestWeight,
           BiomassSampleArea,
           #ResidueMassAirDry,
           #ResidueMassAirDryPerArea,
           ResidueCarbon,
           ResidueNitrogen,
           BiomassAirDry,
           Comments)
  
  return(df.clean)
}

get_clean2014_prioritizeNirData <- function() {
  # Loads legecy data from one or more datasets and outputs cleaned dataframe
  # Cleaning step that prioritizes yield data accompanied with NIR data (and thus grain moisture content). The other 2014 function accepts a dataset Dr. Dave Huggins said is better quality.
  
  require(tidyverse)
  
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
        warning(paste("id:", id, "- something wrong with protein"))
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
        warning(paste("id:", id, "something wrong checking weighed in 2015"))
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
        warning(paste("id:", id, "something wrong with grain+bag weight"))
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
    mutate(GrainSampleArea = 2,
           BiomassSampleArea = 2) %>% 
    mutate(#GrainYieldAirDryPerArea = TotalGrain.g. / GrainSampleArea,
           #ResidueMassAirDry = biomass - TotalGrain.g.,
           #ResidueMassAirDryPerArea = (biomass - TotalGrain.g.) / ResidueSampleArea,
           Comments = paste(NOTES, Notes2, Notes3,
                            sep = " | "),
           HarvestYear = 2014)
  
  ## Clean data
  df.clean <- df.calc %>% 
    rename(SampleID = BarcodeFinal,
           GrainMassAirDry = TotalGrain.g.,
           GrainProtein = protein,
           GrainMoisture = moisture,
           GrainStarch = starch,
           GrainWGlutDM = gluten,
           Longitude = X,
           Latitude = Y,
           GrainTestWeight = testWeight,
           BiomassAirDry = biomass) %>% 
    select(HarvestYear,
           Crop,
           SampleID,
           Longitude,
           Latitude,
           ID2,
           GrainSampleArea,
           GrainMassAirDry,
           #GrainYieldAirDryPerArea,
           GrainProtein,
           GrainMoisture,
           GrainStarch,
           GrainWGlutDM,
           GrainTestWeight,
           BiomassSampleArea,
           #ResidueMassAirDry,
           #ResidueMassAirDryPerArea,
           BiomassAirDry,
           Comments)
  
  return(df.clean)
}

get_clean2014 <- function() {
  # Loads legecy data from one or more datasets and outputs cleaned dataframe
  
  require(tidyverse)
  require(readxl)
  source("src/functions.R")
  
  # Data are from various files, specific to crop, provided by Dr. Huggins.
  # Read in the data, do some cleaning as we go
  gb.cols <- c(rep("skip", 4),
               "numeric",
               "text",
               rep("skip",2),
               "text",
               rep("skip", 2),
               "numeric",
               rep("skip", 2),
               "numeric",
               rep("skip", 6),
               "text")
  gb.names <- c("Column",
                "Row2",
                "SampleID",
                "BiomassAirDry",
                "GrainMassAirDry",
                "Comments")
  df.gb <- read_excel("input/GPHY14_GB20141221.xlsx",
                      sheet = "Sheet1",
                      skip = 9,
                      col_types = gb.cols,
                      col_names = gb.names) %>% 
    mutate(Crop = "GB",
           HarvestYear = 2014)
  
  sb.cols <- c(rep("skip", 4),
               "numeric",
               "text",
               rep("skip",2),
               "text",
               rep("skip", 2),
               "numeric",
               "numeric",
               rep("skip", 6),
               rep("numeric", 4),
               "text")
  sb.names <- c("Column",
                "Row2",
                "SampleID",
                "GrainMassAirDry",
                "BiomassAirDry",
                "GrainProtein",
                "GrainMoisture",
                "GrainStarch",
                "GrainTestWeight",
                "Comments")
  df.sb <- read_excel("input/GPHY14SB_20190621.xlsx",
                      skip = 8,
                      col_types = sb.cols,
                      col_names = sb.names) %>% 
    mutate(Crop = "SB",
           HarvestYear = 2014)
  
  ww.cols <- c("text",
               "skip",
               "numeric",
               "numeric",
               rep("skip", 9),
               "text")
  ww.names <- c("SampleID",
                "GrainMassAirDry",
                "BiomassAirDry",
                "Comments")
  df.ww <- read_excel("input/Cook Farm GP HY2014 Field B C4 WW_20190621.xlsx",
                      skip = 8,
                      col_types = ww.cols,
                      col_names = ww.names) %>% 
    mutate(SampleSep = SampleID) %>% 
    separate(SampleSep, sep = "_", into = c("SamplePrefix", "ID", "ColRow2", "IsRes")) %>% 
    separate(ColRow2, sep = "-", into = c("Column", "Row2")) %>% 
    mutate(Column = as.numeric(Column)) %>% 
    select(-SamplePrefix, -ID, -IsRes) %>% 
    mutate(Crop = "WW",
           HarvestYear = 2014)
  
  sw.cols <- c(rep("skip", 4),
               "text",
               "numeric",
               rep("skip", 2),
               "text",
               "skip",
               rep("numeric", 2),
               rep("skip", 11),
               "text")
  sw.names <- c("Row2",
                "Column",
                "Sample",
                "GrainMassAirDry",
                "BiomassAirDry",
                "Comments")
  df.sw <- read_excel("input/CFGPSW_HY2014_9.4.2014.xlsx",
                      skip = 8,
                      col_types = sw.cols,
                      col_names = sw.names) %>% 
    mutate(SampleID = paste(Sample, paste(Column, Row2, sep = "-"), sep = "_")) %>% 
    mutate(Crop = "SW",
           HarvestYear = 2014)
  
  # Append rows from all dataframes
  df.bind <- bind_rows(df.gb, df.sb, df.sw, df.ww) %>% 
    # NOTE: The actual area harvested was not recorded for 2014. It is likely that confusion over SOP caused area to be 1 m x 2 m or 2 m2 (harvesting within area inside pvc pipes instead of four rows of 2 m lenght - as was previous methods).  After mapping yields and comparing between years, Dave Huggins decided that 2014 was likely harvested at 2 m2. See notes in "CookEastPlantHandHarvest" OneNote (Bryan's WSU account)
    mutate(GrainSampleArea = 2,
           BiomassSampleArea = 2) %>% 
    arrange(SampleID)
  
  # Some WW samples have a observation for residue (_RES) samples and one for grain, these need to be combined
  df.coalesce <- df.bind %>% 
    mutate(SampleID = str_replace(SampleID, "_RES", "")) %>% 
    group_by(SampleID) %>% 
    fill(everything(), .direction = "down") %>% 
    fill(everything(), .direction = "up") %>% 
    slice(1)
  
  # Add georef data
  df <- append_georef_to_df(df.coalesce, "Row2", "Column") %>% 
    rename(Latitude = Y,
           Longitude = X)
  
  #df.calc <- df %>% 
  #  mutate(GrainYieldAirDryPerArea = GrainMassAirDry / GrainSampleArea,
  #         ResidueMassAirDry = BiomassAirDry - GrainMassAirDry,
  #         ResidueMassAirDryPerArea = ResidueMassAirDry / ResidueSampleArea)
  
  # Load alternate dataset that prioritized yield values accompanied with NIR data
  df.nir <- get_clean2014_prioritizeNirData()
  
  #df.merge.test <- df.nir %>% 
  #  left_join(df, by = "ID2")
  
  # Note: manually compared following values from two dataframes
  #   * Longitude: 0 rows
  #     * df.merge.test %>% filter(Longitude.x != Longitude.y)
  #   * Latitude: 0 rows
  #     * df.merge.test %>% filter(Latitude.x != Latitude.y)
  #   * GrainProtein: 0 rows
  #     * df.merge.test %>% filter(!is.na(GrainProtein.y)) %>% filter(GrainProtein.y != GrainProtein.x)
  #   * Crop: 0 rows
  #     * df.merge.test %>% filter(Crop.x != Crop.y)
  #   * SampleID: 118 rows [using SampleID (from get_clean2014())]
  #     * df.merge.test %>% filter(toupper(SampleID.x) != toupper(SampleID.y)) %>% select(ID2, SampleID.x, SampleID.y) %>% tibble()
  #   * GrainProtein 0 rows
  #     * df.merge.test %>% filter(!is.na(GrainProtein.y)) %>% filter(GrainProtein.x != GrainProtein.y)
  #   * GrainMoisture: 0 rows
  #     * df.merge.test %>% filter(!is.na(GrainMoisture.y)) %>% filter(GrainMoisture.x != GrainMoisture.y)
  
  # NIR dataset has some values missing from this dataset, so merge then select best values
  df.merge <- df.nir %>% 
    #mutate(ResidueMassAirDry = ResidueMassAirDryPerArea * ResidueSampleArea) %>% 
    select(ID2, 
           GrainProtein, 
           GrainMoisture, 
           GrainStarch, 
           GrainWGlutDM,
           GrainTestWeight,
           BiomassAirDry,
           GrainMassAirDry,
           #ResidueMassAirDry,
           #ResidueMassAirDryPerArea,
           Comments) %>% 
    left_join(df, by = "ID2")
  
  # .x suffix = NIR data, .y suffix = Huggin's approved data
  # Accept Residue and grain data from Huggin's above NIR's
  # Accept NAIR's grain protein, moisture, startch, gluten, test weight
  # Merge comments
  df.clean <- df.merge %>%
    mutate(GrainProtein = GrainProtein.x,
           GrainMoisture = GrainMoisture.x,
           GrainStarch = GrainStarch.x,
           GrainTestWeight = GrainTestWeight.x,
           #ResidueMassAirDry = case_when(!is.na(ResidueMassAirDry.y) ~ ResidueMassAirDry.y,
           #                           TRUE ~ ResidueMassAirDry.x),
           BiomassAirDry = case_when(!is.na(BiomassAirDry.y) ~ BiomassAirDry.y,
                                         TRUE ~ BiomassAirDry.x),
           GrainMassAirDry = case_when(!is.na(GrainMassAirDry.y) ~ GrainMassAirDry.y,
                                       TRUE ~ GrainMassAirDry.x),
           #ResidueMassAirDryPerArea = case_when(!is.na(ResidueMassAirDryPerArea.y) ~ ResidueMassAirDryPerArea.y,
           #                                  TRUE ~ ResidueMassAirDryPerArea.x),
           Comments = paste(Comments.x, Comments.y, sep = " | ")) %>% 
    select(HarvestYear,
           Crop,
           SampleID,
           Longitude,
           Latitude,
           ID2,
           GrainSampleArea,
           GrainMassAirDry,
           GrainProtein,
           GrainMoisture,
           GrainStarch,
           GrainWGlutDM,
           GrainTestWeight,
           BiomassAirDry,
           BiomassSampleArea,
           #ResidueMassAirDry,
           #ResidueMassAirDryPerArea,
           Comments)
  
  # Now that data are cleaned and merged, calculate yield at 0 % moisture
  #df.clean.calc <- df.clean %>%
  #  mutate(GrainMassOvenDry = GrainMassAirDry - (GrainMassAirDry * (GrainMoisture/100))) %>%
  #  mutate(GrainYieldOvenDryPerArea = GrainMassOvenDry/GrainSampleArea)

  return(df.clean)
}

get_clean2015 <- function() {
  # Loads legacy data from one or more datasets and outputs cleaned dataframe
  
  require(tidyverse)
  require(readxl)
  require(sf)
  
  source("src/functions.R")
  
  # Read data. This dataset was manually cleaned previously using Excel.
  df2015 <- read_excel("input/HY2015GP_171018.xlsx", 
                       "Clean")
  
  # Fill area values and calc missing
  df.calc <- df2015 %>% 
    # Ellen mapped data, ID2 of 258 is within SC strip
    mutate(Crop = case_when(ID2 == 258 ~ "SC",
                            TRUE ~ Crop)) %>% 
    # SC was harvested using 1 m x 2 m pipes as guide due to difficulties seeing drill rows
    # All others were harvested at 4 rows of 1 m length
    mutate(GrainSampleArea = case_when(Crop == "SC" ~ 2,
                                       TRUE ~ 2.4384),
           BiomassSampleArea = GrainSampleArea) #%>% 
    #mutate(GrainYieldAirDryPerArea = `GrainNetWt (g)` / GrainSampleArea,
    #       ResidueMassAirDry = `NetWt (g)` - `GrainNetWt (g)`)
  
  # Calc yield at 0% moisture
  #df.calc <- df.calc %>%
  #  mutate(GrainMassOvenDry = `GrainNetWt (g)` - (`GrainNetWt (g)` * (`Moisture (%)` / 100))) %>%
  #  mutate(GrainYieldOvenDryPerArea = GrainMassOvenDry / GrainSampleArea)
  
  # Rename and select columns
  df.clean <- df.calc %>% 
    rename(SampleID = Barcode,
           GrainOilDM = `Oil (DM)`,
           GrainProtein = `Protein (%)`,
           GrainMoisture = `Moisture (%)`,
           GrainStarch = `Starch (%)`,
           GrainWGlutDM = WGlutDM,
           GrainTestWeight = `TestWeight (g)`,
           Comments = Notes,
           GrainMassAirDry = `GrainNetWt (g)`,
           BiomassAirDry = `NetWt (g)`) %>% 
    select(HarvestYear,
           Crop,
           SampleID,
           Longitude,
           Latitude,
           ID2,
           GrainSampleArea,
           GrainMassAirDry,
           #GrainMassOvenDry,
           #GrainYieldOvenDryPerArea,
           GrainOilDM,
           GrainProtein,
           GrainMoisture,
           GrainStarch,
           GrainWGlutDM,
           GrainTestWeight,
           BiomassSampleArea,
           #ResidueMassAirDry,
           #ResidueMassAirDryPerArea,
           BiomassAirDry,
           Comments)
  
  return(df.clean)
}

get_clean2016 <- function() {
  # Loads legacy data from one or more datasets and outputs cleaned dataframe
  
  require(tidyverse)
  require(readxl)
  require(sf)
  
  source("src/functions.R")
  
  # Read data. This dataset was manually cleaned previously using Excel.
  df2016 <- read_excel("input/HY2016GP_171019.xlsx", 
                       "Clean") %>% 
    filter(!is.na(ID2))
  
  # Fill area values and calc missing
  df.calc <- df2016 %>% 
    mutate(GrainSampleArea = case_when(Crop == "SC" ~ 2,
                                       TRUE ~ 2.4384),
           BiomassSampleArea = GrainSampleArea) %>% 
    mutate(#GrainYieldAirDryPerArea = `GrainNetWt (g)` / GrainSampleArea,
           #ResidueMassAirDry = `NetWt (g)` - `GrainNetWt (g)`,
           Longitude = as.numeric(Longitude))
  
  # Calc yield at 0% moisture
  #df.calc <- df.calc %>%
  #  mutate(GrainMassOvenDry = `GrainNetWt (g)` - (`GrainNetWt (g)` * (`Moisture (%)` / 100))) %>%
  #  mutate(GrainYieldOvenDryPerArea = GrainMassOvenDry / GrainSampleArea)
  
  # Rename and select columns
  df.clean <- df.calc %>% 
    rename(SampleID = Barcode,
           GrainOilDM = `Oil (DM)`,
           GrainProtein = `Protein (%)`,
           GrainMoisture = `Moisture (%)`,
           GrainStarch = `Starch (%)`,
           GrainWGlutDM = WGlutDM,
           GrainTestWeight = `TestWeight (g)`,
           Comments = NotesValue,
           GrainMassAirDry = `GrainNetWt (g)`,
           BiomassAirDry = `NetWt (g)`) %>% 
    select(HarvestYear,
           Crop,
           SampleID,
           Longitude,
           Latitude,
           ID2,
           GrainSampleArea,
           GrainMassAirDry,
           #GrainMassOvenDry,
           #GrainYieldOvenDryPerArea,
           GrainOilDM,
           GrainProtein,
           GrainMoisture,
           GrainStarch,
           GrainWGlutDM,
           GrainTestWeight,
           BiomassSampleArea,
           #ResidueMassAirDry,
           #ResidueMassAirDryPerArea,
           BiomassAirDry,
           Comments)
  
  return(df.clean)
}

get_clean1999_2016 <- function(rm.outliers = F) {
  # Gets all data and combines. Alternatively removes extreme outliers.
  #
  # Args:
  #   rm.outliers: if TRUE, removes extreme outliers from subset of columns
  
  df1999_2009 <- get_clean1999_2009()
  df2010 <- get_clean2010()
  df2011 <- get_clean2011()
  df2012 <- get_clean2012()
  df2013 <- get_clean2013()
  df2014 <- get_clean2014()
  df2015 <- get_clean2015()
  df2016 <- get_clean2016()
  
  df <- bind_rows(df1999_2009,
                  df2010,
                  df2011,
                  df2012,
                  df2013,
                  df2014,
                  df2015,
                  df2016)
  
  
  
  # TODO: 2023-05-03 - Need to redo this (nevermind, it's not being used)
  if(rm.outliers == T) {
    print("rm.outliers is not implemented")
    #df.rm.outliers <- df %>% 
    #  group_by(HarvestYear, Crop) %>% 
    #  mutate(GrainYieldDryPerArea = removeOutliers(GrainYieldDryPerArea),
    #         GrainCarbon = removeOutliers(GrainCarbon),
    #         GrainNitrogen = removeOutliers(GrainNitrogen),
    #         ResidueMassDryPerArea = removeOutliers(ResidueMassDryPerArea),
    #         ResidueCarbon = removeOutliers(ResidueCarbon),
    #         ResidueNitrogen = removeOutliers(ResidueNitrogen),
    #         GrainProtein = removeOutliers(GrainProtein),
    #         GrainMoisture = removeOutliers(GrainMoisture),
    #         GrainStarch = removeOutliers(GrainStarch),
    #         GrainWGlutDM = removeOutliers(GrainWGlutDM),
    #         GrainOilDM = removeOutliers(GrainOilDM))
    #
    #return(df.rm.outliers)
  } else {
    return(df);
  }
}