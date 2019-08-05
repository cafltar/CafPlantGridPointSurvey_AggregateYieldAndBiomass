library(tidyverse)
library(sf)
library(tmap)

source("src/functions.R")

# read data
df.yields <- read_csv("output/HY1999-2016_20190723_Q01P02.csv") %>% 
  filter(!is.na(GrainYieldDryPerArea))

boundary <- st_read("input/CookEastBoundary_20180131.geojson")

strips <- st_read("input/CookEastStrips/Field_Plan_Final.shp") %>% 
  st_transform(crs = 4326) %>% 
  st_intersection(boundary)

# ---- Moved to mapping-crops.Rmd ----
tmap_mode("plot")
# Create simple feature from dataframe
sf <- df.yields %>% 
  st_as_sf(coords = c("Longitude", "Latitude"), crs = 4326)

# Draw Cook East field boundary
tm.border <- tm_shape(boundary) +
  tm_borders()
tm.border.strips <- tm.border +
  tm_shape(strips) +
  tm_borders()

# Map all crops by year
tm.border.strips +
tm_shape(sf) +
  tm_dots(size = 0.3, col = "Crop", palette = "Set1") +
  tm_facets(by="HarvestYear", free.coords = F)

# Map spring canola and winter canola only
sf.canola <- sf %>% 
  filter(Crop %in% c("SC", "WC"))
  
tm.border +
tm_shape(sf.canola) +
  tm_dots(size = 0.3, col = "Crop", palette = "Set1") +
  tm_facets(by="HarvestYear", free.coords = F)