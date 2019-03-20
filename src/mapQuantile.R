library(sf)
library(tmap)
source("src/cleaningFunctions.R")

df1999_2009.dirty <- get_dirty1999_2009()

sf <- df1999_2009.dirty %>% 
  filter(!is.na(X) & !is.na(Y)) %>% 
  st_as_sf(coords = c("X", "Y"), crs = 4326)

sf %>% 
  filter(Year == 2001) %>% 
  getMapQuartileOutliers("GrainNitrogen", "Year", "Crop") +
  tm_layout(title = "HY 2001: GrainNitrogen") +
  tm_grid(alpha = 0.25, labels.inside.frame = F) +
  tm_compass()
  
sf %>% 
  getMapQuartileOutliers("GrainNitrogen", "Year", "Crop") +
  tm_facets(by = "Year") +
  tm_layout(title = "GrainNitrogen by Year") +
  tm_grid(alpha = 0.25, labels.inside.frame = F) +
  tm_compass()
