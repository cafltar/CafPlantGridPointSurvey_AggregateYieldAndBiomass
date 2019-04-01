library(sf)
library(tmap)
source("src/cleaningFunctions.R")

df1999_2009.dirty <- get_dirty1999_2009() %>% 
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
  replace(. == "Garbonzo Beans", "GB")

sf <- df1999_2009.dirty %>% 
  filter(!is.na(X) & !is.na(Y)) %>% 
  st_as_sf(coords = c("X", "Y"), crs = 4326)

sf %>% 
  filter(Year == 2001) %>% 
  getMapQuartileOutliers("GrainNUbbie", "Year", "Crop") +
  tm_layout(title = "HY 2001: GrainNUbbie") +
  tm_grid(alpha = 0.25, labels.inside.frame = F) +
  tm_compass() +
  tm_text("ID2", size = 0.6, ymod = 0.5)

columns <- c("GrainNUbbie", "GrainNUnger", "GrainNFinal")
years <- seq(1999, 2001, 1)

for(y in years) {
  for(c in columns) {
    print(sf %>% 
      filter(Year == y) %>% 
      getMapQuartileOutliers(c, "Year", "Crop") +
      tm_layout(title = paste("HY ", y, ": ", c, sep = "")) +
      tm_grid(alpha = 0.25, labels.inside.frame = F) +
      tm_compass() +
      tm_text("ID2", size = 0.6, ymod = 0.5))
  }
}

boundary <- st_read("input/CookEastBoundary_20180131.geojson")

sf.dirty %>% 
  filter(Year == 2008) %>% 
  getMapQuartileOutliers("GrainCUbbie", "Year", "Crop", boundary) +
  tm_layout(title = "foo") +
  tm_grid(alpha = 0.25, labels.inside.frame = F) +
  tm_compass()
  
df1999_2009.dirty %>% filter(Crop != Crop...3) %>% 
  select(Year, Crop...3, Crop...22, Crop) %>% 
  print(n = 100)

sf %>% 
  filter(`Crop...3` != Crop) %>% 
  select(Year, ID2, `Crop...3`, Crop) %>% 
  mutate(Label = paste(`Crop...3`, Crop, sep = ",")) %>% 
  tm_shape() +
  tm_dots(size = 2, col = "blue") +
  tm_text("Label", size = 0.6, ymod = 0.7) +
  tm_facets(by = "Year") +
  tm_shape(boundary) +
  tm_borders() +
  tm_shape(sf) +
  tm_dots(col = "Crop", size = 0.5)

cropError <- sf %>% 
  filter(Year %in% c(2001, 2003, 2007, 2008)) %>% 
  mutate(CropLabelError = (`Crop...3` != Crop)) %>% 
  select(Year, ID2, `Crop...3`, Crop, CropLabelError) %>% 
  mutate(Label = case_when(CropLabelError == TRUE ~ paste(`Crop...3`, sep = ",")))

tm_shape(cropError) +
  tm_dots(col = "Crop", shape = "CropLabelError", size = 0.7) +
  tm_facets(by = "Year") +
  tm_text("Label", size = 0.8, ymod = 0.2)

