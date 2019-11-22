estimateResidueMassDryXByResidueMoistureProportion <- function(df) {
  # Calculate average moisture proportion of residue by crop and use it to calculate the dry mass of residue for missing values
  #
  # Args:
  #   df: dataframe containing harvest data as generated from get_clean1999_2016()
  
  df.gapFill <- df %>% 
    group_by(Crop) %>% 
    mutate(AvgResidueMoistureProportionByCrop = mean(ResidueMoistureProportion, na.rm = TRUE)) %>% 
    ungroup() %>% 
    mutate(ResidueMassDry_P2 = case_when(!is.na(ResidueMassDry) ~ ResidueMassDry,
                                      is.na(ResidueMassDry) ~ ResidueMassWet * (1 - AvgResidueMoistureProportionByCrop))) %>%
    mutate(ResidueMassDryPerArea_P2 = case_when(!is.na(ResidueMassDryPerArea) ~ ResidueMassDryPerArea,
                                             is.na(ResidueMassDryPerArea) ~ ResidueMassDry_P2 / ResidueSampleArea))
  
  return(df.gapFill)
}

estimateResidueMassDryPerAreaByGrainYieldDryPerArea <- function(df) {
  # Create a linear model that relates grain yield to dry residue mass for a given crop in a given year.
  # 
  # Args:
  #   df: dataframe containing harvest data as generated from get_clean1999_2016()
  #
  # Results:
  #   DataFrame with gap-filled values and new column "ResidueMassDryPerArea_P" that indicates whether original data (2) or gap-filled (3)
  
  require(purrr)
  require(broom)
  require(tidyverse)
  
  # Remove AL (alfalfa) since we don't actually have data
  df.clean <- df %>% 
    filter(Crop != "AL", !is.na(Crop),
           !is.na(GrainYieldDryPerArea_P3),
           !is.na(ResidueMassDryPerArea_P2))
  
  modelByCropYear <- df.clean %>% 
    nest(-HarvestYear, -Crop) %>%
    mutate(
      fit = map(data, ~ lm(ResidueMassDryPerArea_P2 ~ GrainYieldDryPerArea_P3, data = .x))
    )
  
  summaryByCropYear <- modelByCropYear %>% 
    mutate(summaries = map(fit, broom::glance)) %>% 
    unnest(summaries) %>% 
    select(HarvestYear, Crop, adj.r.squared, df.residual) %>% 
    rename(AdjRSquared = adj.r.squared,
           DfResidual = df.residual)
  
  regressionByCropYear <- modelByCropYear %>% 
    mutate(tidied = map(fit, tidy)) %>% 
    unnest(tidied) %>% 
    select(HarvestYear, Crop, term, estimate) %>% 
    spread(term, estimate) %>% 
    rename(InterceptEstimate = `(Intercept)`,
           XEstimate = GrainYieldDryPerArea_P3)
  
  # Join data, use df to retain NaN and NA values
  df.linear <- df %>% 
    select(HarvestYear, Crop) %>% 
    left_join(summaryByCropYear, by = c("HarvestYear", "Crop")) %>% 
    left_join(regressionByCropYear, by = c("HarvestYear", "Crop")) %>% 
    unique()
  
  # Calculate average harvest index
  # Hardcoded min/max range of accepted HI values, as determined by Qiuping Peng using HI values from this dataset (after cleaning outliers) and from literature values
  hi.bounds <- read_csv("input/crop-harvest-index-bounds.csv")
  df.hi <- df %>% 
    mutate(HarvestIndex = GrainYieldDryPerArea_P3 / (GrainYieldDryPerArea_P3 + ResidueMassDryPerArea_P2)) %>% 
    left_join(hi.bounds, by = ("Crop")) %>% 
    group_by(HarvestYear, Crop) %>% 
    mutate(HarvestIndexAvg = mean(HarvestIndex, na.rm = TRUE)) %>% 
    ungroup() %>% 
    group_by(Crop) %>% 
    mutate(HarvestIndexAvg = case_when(is.na(HarvestIndexAvg) ~ mean(HarvestIndex, na.rm = TRUE),
                                 TRUE ~ HarvestIndexAvg)) %>% 
    ungroup()
  
  # Estimate those values!
  #   1. Use linear model
  #   2. Calculate harvest index using residue estimated by linear model
  #   3. Check that harvest index is within bounds
  #   4. Calculate residue from average HI if outside of bounds
  df.estResidue <- df.hi %>% 
    full_join(df.linear, by = c("HarvestYear","Crop")) %>%
    mutate(ResidueMassDryPerArea.linear = case_when(is.na(ResidueMassDryPerArea_P2) ~ InterceptEstimate + XEstimate * GrainYieldDryPerArea_P3)) %>% 
    mutate(HarvestIndex.linear = case_when(!is.na(ResidueMassDryPerArea.linear) ~ GrainYieldDryPerArea_P3 / (GrainYieldDryPerArea_P3 + ResidueMassDryPerArea.linear))) %>% 
    mutate(ResidueMassDryPerArea.HI = case_when(((HarvestIndex.linear < HIMin) | (HarvestIndex.linear > HIMax)) ~ (GrainYieldDryPerArea_P3/HarvestIndexAvg) - GrainYieldDryPerArea_P3,
                                                is.na(HarvestIndex.linear) & is.na(ResidueMassDryPerArea_P2) ~ (GrainYieldDryPerArea_P3/HarvestIndexAvg) - GrainYieldDryPerArea_P3))
  
  df.gapFill <- df.estResidue %>% 
    mutate(ResidueMassDryPerArea_P = case_when(!is.na(ResidueMassDryPerArea_P2) ~ 2,
                                               is.na(ResidueMassDryPerArea_P2) & (!is.na(ResidueMassDryPerArea.HI) | !is.na(ResidueMassDryPerArea.linear)) ~ 3))
  
  df.result <- df.gapFill %>% 
    mutate(ResidueMassDryPerArea_P3 = case_when(!is.na(ResidueMassDryPerArea_P2) ~ ResidueMassDryPerArea_P2,
                                             is.na(ResidueMassDryPerArea_P2) & !is.na(ResidueMassDryPerArea.HI) ~ ResidueMassDryPerArea.HI,
                                             is.na(ResidueMassDryPerArea_P2) & is.na(ResidueMassDryPerArea.HI) & !is.na(ResidueMassDryPerArea.linear) ~ ResidueMassDryPerArea.linear))
  
  return(df.result)
}

estimateYieldByAvgYieldAndRelativeYield <- function(df) {
  # Estimates yield value by calculating two forms of average yield,
  #   1: by crop & year
  #   2: by crop (all years)
  # then using that average to multiple by 11-year average relative yield at that sample's location
  # 
  # Option 2 is used only if Option 1 cannot be calculated
  #
  # Args:
  #   df: dataframe containing harvest data as generated from get_clean1999_2016()
  #
  # Returns:
  #   dataframe with gap-filled values and new column "GrainYieldDryPerArea_P" that indicates whether original data (2) or gap-filled (3)
  
  # Load data for relative yields
  df.relYields <- read_xlsx("input/StripbyStripAvg2.xlsx",
                     sheet = "NEW_RY") %>% 
    select(UID, Field, Strip, `11Years Avg_Strip`) %>% 
    rename(RelativeYield = `11Years Avg_Strip`,
           ID2 = UID)
  # Load data to assess whether or not yield should be estimated
  df.cropPresent <- read_xlsx("input/HY1999-2016_20190708_NOGapfill with updated cropexistcode.xlsx",
                             sheet = "HY1999-2016_20190708_NOGap") %>% 
    select(HarvestYear, ID2, updateCropwaspresentcode) %>% 
    rename(CropExists = updateCropwaspresentcode)
  
  # Merge datasets
  df.merge <- df %>% 
    left_join(df.relYields, by = "ID2") %>% 
    left_join(df.cropPresent, by = c("HarvestYear", "ID2"))
  
  # Calculate average yield
  # First calc by crop and year
  # Second calc by crop (all years) if first fails
  df.calc <- df.merge %>% 
    group_by(HarvestYear, Crop) %>% 
    mutate(GrainYieldDryPerAreaAvg = mean(GrainYieldDryPerArea, na.rm = T)) %>% 
    ungroup() %>% 
    group_by(Crop) %>% 
    mutate(GrainYieldDryPerAreaAvg = case_when(is.na(GrainYieldDryPerAreaAvg) ~ mean(GrainYieldDryPerArea, na.rm = T),
                                               TRUE ~ GrainYieldDryPerAreaAvg)) %>% 
    ungroup()
  
  # If yield is null and crop exists, estimate: YieldAvg * RelativeYield
  df.gapFill <- df.calc %>% 
    mutate(GrainYieldDryPerArea_P3 = case_when((is.na(GrainYieldDryPerArea) & CropExists == 1) ~ GrainYieldDryPerAreaAvg * RelativeYield,
                                            TRUE ~ GrainYieldDryPerArea))
  
  df.result <- df.gapFill %>% 
    mutate(GrainYieldDryPerArea_P = case_when(!is.na(GrainYieldDryPerArea) ~ 2,
                                              is.na(GrainYieldDryPerArea) & !is.na(GrainYieldDryPerArea_P3) ~ 3))
  
  return(df.result)
}

writeGapFillResidueStatistics <- function(df) {
  # A one off function that writes some parameters used for gap filling
  
  df %>% 
    filter(!is.na(ResidueMoistureProportion)) %>% 
    group_by(Crop) %>% 
    #mutate(AvgResidueMoistureProportionByCrop = mean(ResidueMoistureProportion, na.rm = TRUE)) %>% 
    #mutate(SEResidueMoistureProportionByCrop = se(ResidueMoistureProportion, na.rm = TRUE)) %>%
    summarize(AvgResidueMoistureProportionByCrop = mean(ResidueMoistureProportion),
              SDResidueMoistureProportionByCrop = sd(ResidueMoistureProportion),
              SEResidueMoistureProportionByCrop = sd(ResidueMoistureProportion)/sqrt(n()),
              n_samples = n()) %>% 
    ungroup() %>%
    distinct() %>% 
    write_csv("Working/residueGapFillStatsByCrop_20190617.csv")
}
