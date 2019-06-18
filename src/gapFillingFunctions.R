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
  
  # Create a linear model that relates grain yield to dry residue mass. Model is by crop by year.
  model <- df %>% 
    filter(Crop != "AL", !is.na(Crop),
           !is.na(GrainYieldDryPerArea),
           !is.na(ResidueMassDryPerArea)) %>% 
    nest(-Crop, -HarvestYear) %>% 
    mutate(
      fit = map(data, ~ lm(ResidueMassDryPerArea ~ GrainYieldDryPerArea, data = .x)),
      tidied = map(fit, tidy)
    ) %>% 
    unnest(tidied) %>% 
    select(Crop, HarvestYear, term, estimate) %>% 
    spread(term, estimate) %>% 
    rename(InterceptEstimate = `(Intercept)`,
           XEstimate = GrainYieldDryPerArea)
  
  # Calculate dry residue mass of missing values using linear model
  df.gapFill <- df %>% 
    full_join(model, by = c("HarvestYear", "Crop")) %>% 
    mutate(ResidueMassDryPerArea = case_when(is.na(ResidueMassDryPerArea) ~ InterceptEstimate + XEstimate * GrainYieldDryPerArea,
                     TRUE ~ ResidueMassDryPerArea)) %>% 
    select(-InterceptEstimate, -XEstimate) 
  
  return(df.gapFill)
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
