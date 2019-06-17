gapFillResidueVariables <- function(df) {
  df.gapFilled <- df %>% 
    group_by(Crop) %>% 
    mutate(AvgResidueMoistureProportionByCrop = mean(ResidueMoistureProportion, na.rm = TRUE)) %>% 
    ungroup() %>% 
    mutate(ResidueMassDry = case_when(!is.na(ResidueMassDry) ~ ResidueMassDry,
                                    is.na(ResidueMassDry) ~ ResidueMassWet * (1 - AvgResidueMoistureProportionByCrop))) %>%
    mutate(ResidueMassDryPerArea = case_when(!is.na(ResidueMassDryPerArea) ~ ResidueMassDryPerArea,
                                            is.na(ResidueMassDryPerArea) ~ ResidueMassDry / ResidueSampleArea))
  
  return(df.gapFilled)
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
  
  #df %>% 
  #  group_by(Crop, HarvestYear) %>% 
  #  mutate(AvgResidueMoistureProportionByCropYear = mean(ResidueMoistureProportion, na.rm = TRUE)) %>% 
  #  mutate(SdResidueMoistureProportionByCropYear = sd(ResidueMoistureProportion, na.rm = TRUE)) %>%
  #  ungroup() %>% 
  #  select(HarvestYear, Crop, AvgResidueMoistureProportionByCropYear, SdResidueMoistureProportionByCropYear) %>% 
  #  distinct() %>% 
  #  write_csv("Working/residueGapFillStatsByCropYear_20190617.csv")
}
