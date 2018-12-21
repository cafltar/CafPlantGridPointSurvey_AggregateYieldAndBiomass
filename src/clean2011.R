library(tidyverse)
library(readxl)
library(sf)

source("src/functions.R")

df2011 <- read_excel("input/HarvestCompilation2010-2016_171012.xlsx", 
                     "2011") %>% 
  filter(!is.na(UID) | (!is.na(Row) & !is.na(Column))) %>% 
  arrange(UID)
  

df2011.check <- read_excel("input/Yields and Residue HY2011 112311.xls", 
                           "Grid Point Yields Only") %>% 
  filter(!is.na(UID) | (!is.na(Row) & !is.na(Column))) %>% 
  arrange(UID)

# Values in df2010 were copy pasted from df2010.check, but make sure at least one column matches
check <- df2011$`Total Grain Wet (g)` == df2011.check$`Total Grain Wet (g)`
if(length(check) < length(df2011$`Total Grain Wet (g)`)) {stop("Error")}

# Merge the georef data based on col and row2
df <- append_georef_to_df(df2011, "Row", "Column")

# Check that ID2 values are ok after merging with row2 and col (it's not)
df.id2.check <- df %>% 
  filter(df$UID != df$ID2)
if(nrow(df.id2.check) > 0) { stop("Error in ID2, Row2, Col") }

# There are values where ID2, Row2, and Col do not mesh, after review, choose ID2, not UID

# TODO: Incomplete, need dry weights, need to calc yield, need residue, need C, N values... ahhhh!!!
#df.clean <- df %>% 
#  select(Year,
#         `Current Crop`,
#         Barcode,
#         X,
#         Y,
#         ID2)