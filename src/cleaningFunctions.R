get_dirty1999_2009 <- function() {
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
  
  df.merged.rm.zeros.outliers <- df.merged %>% 
    group_by(Year, Crop) %>% 
    mutate(GrainNUbbie = case_when(`Grain Nitrogen %...19` > 0 ~ `Grain Nitrogen %...19`),
           GrainNUnger = case_when(GrainNitrogen > 0 ~ GrainNitrogen,
                                   Year == 1999 & Crop == "SW" ~ NA_real_,
                                   Year == 2001 & Crop == "WW" ~ NA_real_)) %>% 
    mutate(GrainNUbbieRmOutlier = removeOutliers(GrainNUbbie),
           GrainNUngerRmOutlier = removeOutliers(GrainNUnger)) %>% 
    mutate(GrainNFinal = case_when(!is.na(GrainNUbbieRmOutlier) & !is.na(GrainNUngerRmOutlier) ~ (GrainNUbbieRmOutlier + GrainNUngerRmOutlier) / 2,
                                   is.na(GrainNUbbieRmOutlier) & !is.na(GrainNUngerRmOutlier) ~ GrainNUngerRmOutlier,
                                   !is.na(GrainNUbbieRmOutlier) & is.na(GrainNUngerRmOutlier) ~ GrainNUbbieRmOutlier)) %>% 
    mutate(GrainCUbbie = case_when(`Grain Carbon %...21` > 0 ~ `Grain Carbon %...21`),
           GrainCUnger = case_when(GrainCarbon > 0 ~ GrainCarbon,
                                   Year == 1999 & Crop == "SW" ~ NA_real_,
                                   Year == 2001 & Crop == "WW" ~ NA_real_)) %>% 
    mutate(GrainCUbbieRmOutlier = removeOutliers(GrainCUbbie),
           GrainCUngerRmOutlier = removeOutliers(GrainCUnger)) %>% 
    mutate(GrainCFinal = case_when(!is.na(GrainCUbbieRmOutlier) & !is.na(GrainCUngerRmOutlier) ~ (GrainCUbbieRmOutlier + GrainCUngerRmOutlier) / 2,
                                   is.na(GrainCUbbieRmOutlier) & !is.na(GrainCUngerRmOutlier) ~ GrainCUngerRmOutlier,
                                   !is.na(GrainCUbbieRmOutlier) & is.na(GrainCUngerRmOutlier) ~ GrainCUbbieRmOutlier)) %>% 
    mutate(GrainMassUbbie = case_when(`total grain yield dry (grams)` > 0 ~ `total grain yield dry (grams)`)) %>% 
    mutate(GrainMassUbbieRmOutlier = removeOutliers(GrainMassUbbie)) %>% 
    mutate(GrainMassFinal = GrainMassUbbieRmOutlier) %>% 
    ungroup()
  
  
  # Merge lat/lon data based on col and row2
  df <- append_georef_to_df(df.merged.rm.zeros.outliers, 
                            "Row Letter", 
                            "Column")
  
  # Remove ID2 values with NA (fields north of current CookEast used to be sampled)
  # Calculate residue values, don't use supplied since it seems to have errors
  # Also calc HI for review
  df.calc <- df %>%
    mutate(GrainYieldDryPerArea = GrainMassFinal / `total area harvested (M2)`) %>% 
    mutate(ResidueWetMass = `Residue plus Grain Wet Weight (grams)` - `Residue sample Grain Wet Weight (grams)`) %>%
    mutate(ResidueMoistureProportion = (`Residue Sub-Sample Wet Weight (grams)` - `Residue Sub-Sample Dry Weight (grams)`) / `Residue Sub-Sample Wet Weight (grams)`) %>%
    mutate(ResidueDry = ResidueWetMass * (1 - ResidueMoistureProportion)) %>%
    mutate(ResidueMassDryPerArea = ResidueDry / `Residue Sample Area (square meters)`)
  
  #--- Aside ----
  # TODO: Unger's and Ubbie's data don't match - taking Ubbies, but need to verify
  df.calc %>% 
    filter(!is.na(ID2), `Crop...3` != "Fallow") %>% 
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
    filter(`Crop...3` != Crop) %>% 
    select(Year, ID2, `Crop...3`, Crop) %>% 
    print(n = 100)
  # ----
  
  return(df.calc)
}
get_clean1999_2009 <- function() {
  df.calc <- get_dirty1999_2009()
  
  df.clean <- df.calc %>% 
    filter(!is.na(ID2), `Crop...3` != "Fallow") %>% 
    mutate(Comments = coalesce(`Grain Harvest Comments`, `Residue Sample Comments`)) %>%
    mutate(HarvestYear = as.integer(Year)) %>%
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
    rename(CropUnger = Crop,
           GrainCarbonUnger = GrainCarbon,
           GrainNitrogenUnger = GrainNitrogen,
           Crop = Crop...3,
           Latitude = Y,
           Longitude = X,
           GrainCarbon = GrainCFinal,
           GrainNitrogen = GrainNFinal,
           ResidueCarbon = `Residue Carbon %`,
           ResidueNitrogen = `Residue Nitrogen %`) %>% 
    select(HarvestYear,
           Crop,
           Longitude,
           Latitude,
           ID2,
           GrainYieldDryPerArea,
           GrainCarbon,
           GrainNitrogen,
           ResidueMassDryPerArea,
           ResidueCarbon,
           ResidueNitrogen,
           Comments) %>% 
    arrange(HarvestYear, ID2)
  
  return(df.clean)
  
}
get_clean1999_2009_deprecated <- function() {
  require(tidyverse)
  require(readxl)
  require(sf)
  
  source("src/functions.R")
  
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
  if(nrow(id.errors) > 0) { warning("Merge check failed") }
  
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
  compare_cols(df, "Grain Carbon %...11", "Grain Carbon %...21")
  compare_cols(df, "Grain Sulfur %...12", "Grain Sulfur %...20")
  compare_cols(df, "Grain Nitrogen %...13", "Grain Nitrogen %...19")
  compare_cols(df, "Crop...3", "Crop...22")
  
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
  if(nrow(residue.check) > 0) { warning("Residue column is not trustworthy; review dataframe 'residue.check'") }
  
  # No need to check yield, the calculation is in the excel sheet and passes muster
  
  # Calculate residue values, don't use supplied since it seems to have errors
  # Also calc HI for review
  df <- df %>%
    mutate(ResidueWetMass = `Residue plus Grain Wet Weight (grams)` - `Residue sample Grain Wet Weight (grams)`) %>%
    mutate(ResidueMoistureProportion = (`Residue Sub-Sample Wet Weight (grams)` - `Residue Sub-Sample Dry Weight (grams)`) / `Residue Sub-Sample Wet Weight (grams)`) %>%
    mutate(ResidueDry = ResidueWetMass * (1 - ResidueMoistureProportion)) %>%
    mutate(ResidueMassDryPerArea = ResidueDry / `Residue Sample Area (square meters)`) %>%
    mutate(HarvestIndex = `total grain yield dry(grams/M2)` / (ResidueMassDryPerArea + `total grain yield dry(grams/M2)`))
  
  # TODO: Need to rename columns, finalize ones to include
  df.clean <- df %>%
    mutate(Comments = coalesce(`Grain Harvest Comments`, `Residue Sample Comments`)) %>%
    select(Year,
           Crop...3, X, Y, ID2,
           `total grain yield dry(grams/M2)`,
           `Grain Carbon %...11`, `Grain Nitrogen %...13`, `Grain Sulfur %...12`,
           ResidueMassDryPerArea,
           `Residue Carbon %`,`Residue Nitrogen %`, `Residue Sulfur %`,
           Comments) %>%
    rename(HarvestYear = Year,
           Crop = Crop...3,
           Latitude = Y,
           Longitude = X,
           ID2 = ID2,
           GrainYieldDryPerArea = `total grain yield dry(grams/M2)`,
           GrainCarbon = `Grain Carbon %...11`,
           GrainNitrogen = `Grain Nitrogen %...13`,
           GrainSulfur = `Grain Sulfur %...12`,
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
}

get_clean2010 <- function() {
  require(tidyverse)
  require(readxl)
  require(sf)
  
  source("src/functions.R")
  
  df2010 <- read_excel("input/Grid Points Yields and Residue 2010.xls") %>% 
    mutate(`Bag Barcode` = toupper(`Bag Barcode`))
  df.res <- read_excel("input/Yields and Residue 2010 Final.xls", 
                       "All Data", 
                       skip = 1, 
                       n_max = 1157) %>% 
    filter(Project == "GP") %>% 
    mutate(`Bag Barcode` = toupper(`Bag Barcode`))
  
  # Check that grain mass match
  df.grain.check <- df2010 %>% 
    full_join(df.res, by = c("Bag Barcode", 
                             "Farm", 
                             "Year", 
                             "Project", 
                             "Grain Type")) %>% 
    filter(`Total Grain Dry (g).x` != `Total Grain Dry (g).y`)
  if(nrow(df.grain.check) > 0) { warning("Grain masses don't match; review grain.check") }
  
  # Merge the georef data based on col and row2
  df <- append_georef_to_df(df2010, "Row", "Column")
  
  # Check that ID2 values are ok after merging with row2 and col (it's not)
  df.id2.check <- df %>% 
    filter(df$UID != df$ID2)
  if(nrow(df.id2.check) > 0) { warning("Error in ID2, Row2, Col") }
  
  # There are values where ID2, Row2, and Col do not mesh, after review, choose ID2, not UID
  
  df.res.slim <- df.res %>% 
    filter(!is.na(`Residue Sub Wet (g)`),
           !is.na(`Total Biomass Wet (g)`),
           !is.na(`Total Grain Wet (g)`),
           `Residue Sub Wet (g)` > 0) %>% 
    mutate(ResidueWetMass = `Total Biomass Wet (g)` - as.numeric(`Total Grain Wet (g)`)) %>% 
    mutate(ResidueMoistureProportion = (`Residue Sub Wet (g)` - `Sub Res Dry (g)`) / `Residue Sub Wet (g)`) %>% 
    mutate(ResidueDry = ResidueWetMass * (1 - ResidueMoistureProportion)) %>% 
    select(`Bag Barcode`, ResidueDry)
  
  # Merge residue data and calc values
  df <- df %>% 
    full_join(df.res.slim, by = c("Bag Barcode")) %>%
    mutate(GrainYieldDryPerArea = `Total Grain Dry (g)` / `Area (m2)`) %>% 
    mutate(ResidueMassDryPerArea = ResidueDry / `Area (m2)`) %>%
    mutate(HarvestIndex = GrainYieldDryPerArea / (ResidueMassDryPerArea + GrainYieldDryPerArea))
  
  df.clean <- df %>% 
    select(Year,
           `Grain Type`,
           `Bag Barcode`,
           X,
           Y,
           ID2,
           GrainYieldDryPerArea,
           ResidueMassDryPerArea,
           Comments) %>% 
    rename("HarvestYear" = Year,
           "Crop" = `Grain Type`,
           "SampleID" = `Bag Barcode`,
           "Latitude" = Y,
           "Longitude" = X) %>% 
    filter(!is.na(ID2)) %>% 
    arrange(HarvestYear, ID2)
}
get_clean2011 <- function() {
  require(tidyverse)
  require(readxl)
  require(sf)
  
  source("src/functions.R")
  
  df2011 <- read_excel("input/Yields and Residue HY2011 112311.xls", 
                       "Grid Point Yields Only") %>% 
    filter(!is.na(UID) | (!is.na(Row) & !is.na(Column))) %>% 
    mutate(SampleID = toupper(Barcode)) %>% 
    arrange(UID)
  
  # Merge the georef data based on col and row2
  df <- append_georef_to_df(df2011, "Row", "Column")
  
  # Check that ID2 values are ok after merging with row2 and col (it's not)
  df.id2.check <- df %>% 
    filter(df$UID != df$ID2)
  if(nrow(df.id2.check) > 0) { warning("Error in ID2, Row2, Col") }
  
  # There are values where ID2, Row2, and Col do not mesh, after review, chose ID2, not UID
  
  # TODO: Incomplete, need dry weights, need to calc dry yield, need dry residue, need C, N values... ahhhh!!!
  df.calcs <- df %>% 
    mutate(Comments = case_when(is.na(as.numeric(df$`Total Residue and Grain Wet (g)`)) ~ paste("Residue note: ", df$`Total Residue and Grain Wet (g)`, sep = ""), TRUE ~ "")) %>% 
    mutate(Comments = case_when(is.na(as.numeric(df$`Total Grain Wet (g)`)) ~ paste(Comments, " | Grain note: ", df$`Total Grain Wet (g)`, sep = ""), TRUE ~ Comments)) %>% 
    mutate(BiomassWet = as.numeric(`Total Residue and Grain Wet (g)`)) %>% 
    mutate(GrainMassWet = as.numeric(`Total Grain Wet (g)`)) %>% 
    mutate(ResidueMassWetPerArea = (BiomassWet - GrainMassWet) / `Area (m2)`) %>% 
    mutate(GrainYieldWetPerArea = GrainMassWet / `Area (m2)`)
  
  # TODO: Figure out if "wet" here means dry, or if there are missing data somewhere...
  df.clean <- df.calcs %>% 
    rename(HarvestYear = Year,
           Crop = `Current Crop`,
           Longitude = X,
           Latitude = Y,
           GrainYieldDryPerArea = GrainYieldWetPerArea,
           ResidueMassDryPerArea = ResidueMassWetPerArea) %>% 
    select(HarvestYear,
           Crop,
           SampleID,
           Longitude,
           Latitude,
           ID2,
           GrainYieldDryPerArea,
           ResidueMassDryPerArea,
           Comments)
}
get_clean2012 <- function() {
  require(tidyverse)
  require(readxl)
  require(sf)
  
  source("src/functions.R")
  
  df2012 <- read_excel("input/Yields and Residue 2012 011513.xlsx", 
                       "Grid Points",
                       skip = 1) %>% 
    filter(!is.na(ID2) | (!is.na(ROW2) & !is.na(COLUMN))) %>% 
    mutate(SampleID = toupper(Barcode)) %>% 
    
    arrange(ID2)
  
  # Merge the georef data based on col and row2
  df <- append_georef_to_df(df2012, "ROW2", "COLUMN")
  
  # Check that ID2 values are ok after merging with row2 and col
  df.id2.check <- df %>% 
    filter(ID2.x != ID2.y)
  if(nrow(df.id2.check) > 0) { warning("Error in ID2, Row2, Col") }
  
  df.calcs <- df %>% 
    mutate(GrainYieldDryPerArea = `Grain Weight Dry (g)` / `Area (m2)`) %>% 
    mutate(Comments = case_when((!is.na(df$`Test Weight`) & is.na(as.numeric(df$`Test Weight`))) ~ paste("TestWeight note: ", df$`Test Weight`, sep = ""), TRUE ~ "")) %>% 
    mutate(Comments = case_when(!is.na(df$...24) ~ paste(Comments, " | Sample note: ", df$...24, sep = ""), TRUE ~ Comments)) %>% 
    mutate(HarvestYear = 2012)
  
  # Wet masses are considered dry, per Dave Huggin's decision
  df.clean <- df.calcs %>% 
    rename(Longitude = X,
           Latitude = Y,
           ID2 = ID2.x,
           GrainProtein = Protein,
           GrainMoisture = Moisture,
           GrainStarch = Starch,
           GrainWGlutDM = Gluten) %>% 
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
           Comments) %>% 
    replace(. == "SL", "GB")
}
get_clean2013 <- function() {
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
}
get_clean2014 <- function() {
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
}
get_clean2015 <- function() {
  require(tidyverse)
  require(readxl)
  require(sf)
  
  source("src/functions.R")
  
  df2015 <- read_excel("input/HY2015GP_171018.xlsx", 
                       "Clean")
  
  df.calc <- df2015 %>% 
    mutate(GrainYieldDryPerArea = case_when(Crop == "SC" ~ `GrainNetWt (g)` / 2,
                                            TRUE ~ `GrainNetWt (g)` / 2.4384),
           ResidueMassDryPerArea = case_when(Crop == "SC" ~ (`NetWt (g)` - `GrainNetWt (g)`) / 2,
                                             TRUE ~ (`NetWt (g)` - `GrainNetWt (g)`) / 2.4384))
  
  df.clean <- df.calc %>% 
    rename(SampleID = Barcode,
           GrainOilDM = `Oil (DM)`,
           GrainProtein = `Protein (%)`,
           GrainMoisture = `Moisture (%)`,
           GrainStarch = `Starch (%)`,
           GrainWGlutDM = WGlutDM,
           Comments = Notes) %>% 
    select(HarvestYear,
           Crop,
           SampleID,
           Longitude,
           Latitude,
           ID2,
           GrainYieldDryPerArea,
           GrainOilDM,
           GrainProtein,
           GrainMoisture,
           GrainStarch,
           GrainWGlutDM,
           ResidueMassDryPerArea,
           Comments)
}
get_clean2016 <- function() {
  require(tidyverse)
  require(readxl)
  require(sf)
  
  source("src/functions.R")
  
  df2016 <- read_excel("input/HY2016GP_171019.xlsx", 
                       "Clean") %>% 
    filter(!is.na(ID2))
  
  df.calc <- df2016 %>% 
    mutate(GrainYieldDryPerArea = case_when(Crop == "SC" ~ `GrainNetWt (g)` / 2,
                                            TRUE ~ `GrainNetWt (g)` / 2.4384),
           ResidueMassDryPerArea = case_when(Crop == "SC" ~ (`NetWt (g)` - `GrainNetWt (g)`) / 2,
                                             TRUE ~ (`NetWt (g)` - `GrainNetWt (g)`) / 2.4384),
           Longitude = as.numeric(Longitude))
  
  df.clean <- df.calc %>% 
    rename(SampleID = Barcode,
           GrainOilDM = `Oil (DM)`,
           GrainProtein = `Protein (%)`,
           GrainMoisture = `Moisture (%)`,
           GrainStarch = `Starch (%)`,
           GrainWGlutDM = WGlutDM,
           Comments = NotesValue) %>% 
    select(HarvestYear,
           Crop,
           SampleID,
           Longitude,
           Latitude,
           ID2,
           GrainYieldDryPerArea,
           GrainOilDM,
           GrainProtein,
           GrainMoisture,
           GrainStarch,
           GrainWGlutDM,
           ResidueMassDryPerArea,
           Comments)
}

get_clean1999_2016 <- function(rm.outliers = T) {
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
  
  if(rm.outliers == T) {
    df.clean <- df %>% 
      group_by(HarvestYear, Crop) %>% 
      mutate(GrainYieldDryPerArea = removeOutliers(GrainYieldDryPerArea),
             GrainCarbon = removeOutliers(GrainCarbon),
             GrainNitrogen = removeOutliers(GrainNitrogen),
             ResidueMassDryPerArea = removeOutliers(ResidueMassDryPerArea),
             ResidueCarbon = removeOutliers(ResidueCarbon),
             ResidueNitrogen = removeOutliers(ResidueNitrogen),
             GrainProtein = removeOutliers(GrainProtein),
             GrainMoisture = removeOutliers(GrainMoisture),
             GrainStarch = removeOutliers(GrainStarch),
             GrainWGlutDM = removeOutliers(GrainWGlutDM),
             GrainOilDM = removeOutliers(GrainOilDM))
    
    return(df.clean)
  } else {
    return(df);
  }
}