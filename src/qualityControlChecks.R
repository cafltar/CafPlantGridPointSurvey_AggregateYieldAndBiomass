remove_calculated_values_manually <- function(x) {
  # Manually removes calculated values deemed erroneous via the following method:
  
  # Args:
  #   x = dataframe with calculated values, as produced by get_clean1999_2016() or estimateResidueMassDryXByResidueMoistureProportion()
  
  require(tidyverse)
  
  x.rm.outliers <- x %>% 
    mutate(ResidueMassDryPerArea = case_when(
      (HarvestYear == 2015 & ID2 == 169) ~ NA_real_,
      (HarvestYear == 2002 & ID2 == 252) ~ NA_real_,
      TRUE ~ ResidueMassDryPerArea)) %>% 
    mutate(GrainYieldDryPerArea = case_when(
      (HarvestYear == 2014 & ID2 == 16) ~ NA_real_,
      TRUE ~ GrainYieldDryPerArea))
  
  return(x.rm.outliers)
}