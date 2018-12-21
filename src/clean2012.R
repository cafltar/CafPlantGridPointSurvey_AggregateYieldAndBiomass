library(tidyverse)
library(readxl)
library(sf)

source("src/functions.R")

df2012 <- read_excel("input/HarvestCompilation2010-2016_171012.xlsx", 
                     "2012",
                     skip = 1) %>% 
  filter(!is.na(ID2) | (!is.na(ROW2) & !is.na(COLUMN))) %>% 
  arrange(ID2)


df2012.check <- read_excel("input/Yields and Residue 2012 011513.xlsx", 
                           "Grid Points",
                           skip = 1) %>% 
  filter(!is.na(ID2) | (!is.na(ROW2) & !is.na(COLUMN))) %>% 
  arrange(ID2)

# Values in df2010 were copy pasted from df2010.check, but make sure at least one column matches
check <- df2012$`Grain Weight Dry (g)` == df2012.check$`Grain Weight Dry (g)`
if(length(check) < length(df2012$`Grain Weight Dry (g)`)) {stop("Error")}

# There are values where ID2, Row2, and Col do not mesh, after review, choose ID2, not UID

# Merge the georef data based on col and row2
df <- append_georef_to_df(df2012, "ROW2", "COLUMN")

# Check that ID2 values are ok after merging with row2 and col
df.id2.check <- df %>% 
  filter(ID2.x != ID2.y)
if(nrow(df.id2.check) > 0) { stop("Error in ID2, Row2, Col") }

# TODO: Calculate residue dry and per area