estimateResidueMassDryXByResidueMoistureProportion <- function(df) {
  # Calculate average moisture proportion of residue by crop and use it to calculate the dry mass of residue for missing values
  #
  # Args:
  #   df: dataframe containing harvest data as generated from get_clean1999_2016()
  
  df.gapFill <- df %>% 
    group_by(Crop) %>% 
    mutate(AvgResidueMoistureProportionByCrop = mean(ResidueMoistureProportion, na.rm = TRUE)) %>% 
    ungroup() %>% 
    mutate(ResidueMassDry = case_when(!is.na(ResidueMassDry) ~ ResidueMassDry,
                                      is.na(ResidueMassDry) ~ ResidueMassWet * (1 - AvgResidueMoistureProportionByCrop))) %>%
    mutate(ResidueMassDryPerArea = case_when(!is.na(ResidueMassDryPerArea) ~ ResidueMassDryPerArea,
                                             is.na(ResidueMassDryPerArea) ~ ResidueMassDry / ResidueSampleArea)) %>% 
    select(-AvgResidueMoistureProportionByCrop)
  
  return(df.gapFill)
}

estimateResidueMassDryPerAreaByGrainYieldDryPerArea <- function(df) {
  # Create a linear model that relates grain yield to dry residue mass. Two linear models are calculated:
  #   1: using data from all years of a single crop
  #   2: using data from single crop in a single year
  # Option 1 is used if available data for option 2 is n < 10.
  #
  # Args:
  #   df: dataframe containing harvest data as generated from get_clean1999_2016()
  
  require(purrr)
  require(broom)
  require(tidyverse)
  
  # Remove AL (alfalfa) since we don't actually have data
  df.clean <- df %>% 
    filter(Crop != "AL", !is.na(Crop),
           !is.na(GrainYieldDryPerArea),
           !is.na(ResidueMassDryPerArea))
  
  modelByCropYear <- df.clean %>% 
    nest(-HarvestYear, -Crop) %>%
    mutate(
      fit = map(data, ~ lm(ResidueMassDryPerArea ~ GrainYieldDryPerArea, data = .x))
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
           XEstimate = GrainYieldDryPerArea)
  
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
    mutate(HarvestIndex = GrainYieldDryPerArea / (GrainYieldDryPerArea + ResidueMassDryPerArea)) %>% 
    left_join(hi.bounds, by = ("Crop")) %>% 
    group_by(HarvestYear, Crop) %>% 
    mutate(HarvestIndexAvg = mean(HarvestIndex, na.rm = TRUE)) %>% 
    ungroup() %>% 
    group_by(Crop) %>% 
    mutate(HarvestIndexAvg = case_when(is.na(HarvestIndexAvg) ~ mean(HarvestIndex, na.rm = TRUE),
                                 TRUE ~ HarvestIndexAvg)) %>% 
    ungroup()
  
  # This should be a function since I'm copy/pasting
  #modelByCrop <- df.clean %>% 
  #  nest(-Crop) %>%
  #  mutate(
  #    fit = map(data, ~ lm(ResidueMassDryPerArea ~ GrainYieldDryPerArea, data = .x))
  #  )
  #
  #summaryByCrop <- modelByCrop %>% 
  #  mutate(summaries = map(fit, broom::glance)) %>% 
  #  unnest(summaries) %>% 
  #  select(Crop, adj.r.squared, df.residual) %>% 
  #  rename(AdjRSquared.crop = adj.r.squared,
  #         DfResidual.crop = df.residual)
  #
  #regressionByCrop <- modelByCrop %>% 
  #  mutate(tidied = map(fit, tidy)) %>% 
  #  unnest(tidied) %>% 
  #  select(Crop, term, estimate) %>% 
  #  spread(term, estimate) %>% 
  #  rename(InterceptEstimate.crop = `(Intercept)`,
  #         XEstimate.crop = GrainYieldDryPerArea)

  #df.crop <- summaryByCrop %>% 
  #  left_join(regressionByCrop, by = c("Crop"))
  #
  #df.merge <- df.cropyear %>% 
  #  left_join(df.crop, by = c("Crop"))
  
  # Decide whether to accept regression equation from Crop or CropYear
  #   If AdjRSquared of Cropyear is NULL, choose other
  #   Assuming at least n = 10 for good regression, so DfResidual < 9, choose all-years
  #model <- df.merge %>% 
  #  mutate(InterceptEstimate = case_when(is.na(AdjRSquared.cropyear) ~ InterceptEstimate.crop#,
  #                                       #DfResidual.cropyear < 9 ~ InterceptEstimate.crop,
  #                                       #(DfResidual.cropyear < 4) & (AdjRSquared.cropyear < 0.5) ~ InterceptEstimate.crop,
  #                                       TRUE ~ InterceptEstimate.cropyear),
  #         XEstimate = case_when(is.na(AdjRSquared.cropyear) ~ XEstimate.crop#,
  #                               #DfResidual.cropyear < 9 ~ XEstimate.crop,
  #                               #(DfResidual.cropyear < 4) & (AdjRSquared.cropyear < 0.5) ~ XEstimate.crop,
  #                               TRUE ~ XEstimate.cropyear))
  
  # Calculate dry residue mass of missing values using linear model
  #df.gapFill <- df %>% 
  #  full_join(model, by = c("HarvestYear","Crop")) %>%
  #  mutate(ResidueMassDryPerArea = case_when(is.na(ResidueMassDryPerArea) ~ InterceptEstimate + XEstimate * GrainYieldDryPerArea,
  #                   TRUE ~ ResidueMassDryPerArea))
  
  df.estResidue <- df.hi %>% 
    full_join(df.linear, by = c("HarvestYear","Crop")) %>%
    mutate(ResidueMassDryPerArea.linear = case_when(is.na(ResidueMassDryPerArea) ~ InterceptEstimate + XEstimate * GrainYieldDryPerArea)) %>% 
    mutate(HarvestIndex.linear = case_when(!is.na(ResidueMassDryPerArea.linear) ~ GrainYieldDryPerArea / (GrainYieldDryPerArea + ResidueMassDryPerArea.linear))) %>% 
    mutate(ResidueMassDryPerArea.HI = case_when(((HarvestIndex.linear < HIMin) | (HarvestIndex.linear > HIMax)) ~ (GrainYieldDryPerArea/HarvestIndexAvg) - GrainYieldDryPerArea,
                                                is.na(HarvestIndex.linear) & is.na(ResidueMassDryPerArea) ~ (GrainYieldDryPerArea/HarvestIndexAvg) - GrainYieldDryPerArea))
  
  # TODO: Add processing flags here -- all estimated values should be P03
  df.gapFill <- df.estResidue %>% 
    mutate(ResidueMassDryPerArea = case_when(!is.na(ResidueMassDryPerArea) ~ ResidueMassDryPerArea,
                                             is.na(ResidueMassDryPerArea) & is.na(ResidueMassDryPerArea.HI) ~ ResidueMassDryPerArea.linear,
                                             is.na(ResidueMassDryPerArea) & !is.na(ResidueMassDryPerArea.HI) ~ ResidueMassDryPerArea.HI))
     
  return(df.gapFill)
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
                                              is.na(GrainYieldDryPerArea) & !is.na(GrainYieldDryPerArea_P3) ~ 3)) %>% 
    select(-GrainYieldDryPerArea) %>% 
    rename(GrainYieldDryPerArea = GrainYieldDryPerArea_P3)
  
  
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
