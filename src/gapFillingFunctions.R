estimateResidueMassDryXByResidueMoistureProportion <- function(df) {
  # Calculate average moisture proportion of residue by crop and use it to calculate the dry mass of residue for missing values
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
  require(purrr)
  require(broom)
  
  # Create a linear model that relates grain yield to dry residue mass. Model is by crop by year.
  #model <- df %>% 
  #  filter(Crop != "AL", !is.na(Crop),
  #         !is.na(GrainYieldDryPerArea),
  #         !is.na(ResidueMassDryPerArea)) %>% 
  #  nest(-HarvestYear, -Crop) %>%
  #  mutate(
  #    fit = map(data, ~ lm(ResidueMassDryPerArea ~ GrainYieldDryPerArea, data = .x)),
  #    tidied = map(fit, tidy)
  #  ) %>% 
  #  unnest(tidied) %>% 
  #  select(HarvestYear, Crop, term, estimate) %>% 
  #  spread(term, estimate) %>% 
  #  rename(InterceptEstimate = `(Intercept)`,
  #         XEstimate = GrainYieldDryPerArea)
  
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
    rename(AdjRSquared.cropyear = adj.r.squared,
           DfResidual.cropyear = df.residual)
  
  regressionByCropYear <- modelByCropYear %>% 
    mutate(tidied = map(fit, tidy)) %>% 
    unnest(tidied) %>% 
    select(HarvestYear, Crop, term, estimate) %>% 
    spread(term, estimate) %>% 
    rename(InterceptEstimate.cropyear = `(Intercept)`,
           XEstimate.cropyear = GrainYieldDryPerArea)
  
  # Join data, use df to retain NaN and NA values
  df.cropyear <- df %>% 
    select(HarvestYear, Crop) %>% 
    left_join(summaryByCropYear, by = c("HarvestYear", "Crop")) %>% 
    left_join(regressionByCropYear, by = c("HarvestYear", "Crop")) %>% 
    unique()
  
  
  # This should be a function since I'm copy/pasting
  modelByCrop <- df.clean %>% 
    nest(-Crop) %>%
    mutate(
      fit = map(data, ~ lm(ResidueMassDryPerArea ~ GrainYieldDryPerArea, data = .x))
    )
  
  summaryByCrop <- modelByCrop %>% 
    mutate(summaries = map(fit, broom::glance)) %>% 
    unnest(summaries) %>% 
    select(Crop, adj.r.squared, df.residual) %>% 
    rename(AdjRSquared.crop = adj.r.squared,
           DfResidual.crop = df.residual)
  
  regressionByCrop <- modelByCrop %>% 
    mutate(tidied = map(fit, tidy)) %>% 
    unnest(tidied) %>% 
    select(Crop, term, estimate) %>% 
    spread(term, estimate) %>% 
    rename(InterceptEstimate.crop = `(Intercept)`,
           XEstimate.crop = GrainYieldDryPerArea)

  df.crop <- summaryByCrop %>% 
    left_join(regressionByCrop, by = c("Crop"))
  
  df.merge <- df.cropyear %>% 
    left_join(df.crop, by = c("Crop"))
  
  # Decide whether to accept regression equation from Crop or CropYear
  #   If AdjRSquared of Cropyear is NULL, choose other
  #   Assuming at least n = 10 for good regression, so DfResidual < 9, choose all-years
  model <- df.merge %>% 
    mutate(InterceptEstimate = case_when(is.na(AdjRSquared.cropyear) ~ InterceptEstimate.crop,
                                         DfResidual.cropyear < 9 ~ InterceptEstimate.crop,
                                         #(DfResidual.cropyear < 4) & (AdjRSquared.cropyear < 0.5) ~ InterceptEstimate.crop,
                                         TRUE ~ InterceptEstimate.cropyear),
           XEstimate = case_when(is.na(AdjRSquared.cropyear) ~ XEstimate.crop,
                                 DfResidual.cropyear < 9 ~ XEstimate.crop,
                                 #(DfResidual.cropyear < 4) & (AdjRSquared.cropyear < 0.5) ~ XEstimate.crop,
                                 TRUE ~ XEstimate.cropyear))
  #TEMP ============
  model %>% filter(Crop == "WC") %>% select(HarvestYear, Crop, InterceptEstimate, XEstimate)
  
  # Calculate dry residue mass of missing values using linear model
  df.gapFill <- df %>% 
    full_join(model, by = c("HarvestYear","Crop")) %>%
    mutate(ResidueMassDryPerArea = case_when(is.na(ResidueMassDryPerArea) ~ InterceptEstimate + XEstimate * GrainYieldDryPerArea,
                     TRUE ~ ResidueMassDryPerArea))
     
  return(df.gapFill)
}

estimateYieldByAvgYieldAndRelativeYield <- function(df) {
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
  sc.2001.avg.yield <- df.merge %>% 
    filter(HarvestYear == 2001, Crop == "SC") %>% 
    group_by(HarvestYear, Crop) %>% 
    summarize(avg = mean(GrainYieldDryPerArea, na.rm = T)) %>% 
    .$avg
  df.calc <- df.merge %>% 
    group_by(HarvestYear, Crop) %>% 
    mutate(GrainYieldDryPerAreaAvg = mean(GrainYieldDryPerArea, na.rm = T)) %>% 
    ungroup() %>% 
    group_by(Crop) %>% 
    mutate(GrainYieldDryPerAreaAvg = case_when(is.na(GrainYieldDryPerAreaAvg) ~ mean(GrainYieldDryPerArea, na.rm = T),
                                               TRUE ~ GrainYieldDryPerAreaAvg)) %>% 
    ungroup() #%>% 
    #mutate(GrainYieldDryPerAreaAvg = case_when(HarvestYear == 2001 & Crop == "WC" ~ sc.2001.avg.yield,
    #                                           TRUE ~ GrainYieldDryPerAreaAvg))
  
  # If yield is null and crop exists, estimate: YieldAvg * RelativeYield
  df.gapFill <- df.calc %>% 
    mutate(GrainYieldDryPerArea = case_when((is.na(GrainYieldDryPerArea) & CropExists == 1) ~ GrainYieldDryPerAreaAvg * RelativeYield,
                                            TRUE ~ GrainYieldDryPerArea))
  }

#http://stat545.com/block023_dplyr-do.html
le_lin_fit <- function(dat) {
  the_fit <- lm(ResidueMassDryPerArea ~ GrainYieldDryPerArea, dat)
  setNames(data.frame(t(coef(the_fit))), c("intercept", "slope"))
}

writeGapFillResidueStatistics <- function(df) {
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
