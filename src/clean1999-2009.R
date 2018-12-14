library(tidyverse)
library(readxl)

legacy <- read_excel("input/Cook Farm All years all points.xlsx", na = "-99999")
georef <- geojson_read("input/CookEast_GeoreferencePoints_171127.json",
                       what = "sp")

# Merge the data based on col and row2
df <- legacy %>% 
  select(-Strip, -Field) %>% 
  rename("Row2" = "Row Letter") %>% 
  join(as.data.frame(georef), by = c("Column", "Row2"))

## Check that merged ID2 matches with Sample Location ID (it does)
#errors <- df %>% 
#  mutate(`Sample Location ID` = as.numeric(`Sample Location ID`)) %>% 
#  mutate(`ID2` = as.numeric(`ID2`)) %>% 
#  filter(`Sample Location ID` != `ID2`) %>% 
#  select(`Sample Location ID`, `ID2`)
