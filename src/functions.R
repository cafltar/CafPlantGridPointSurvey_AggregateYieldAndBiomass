require(sf)

write_csv_gridPointSurvey <- function(df, harvest.year) {
  date.today <- format(Sys.Date(), "%Y%m%d")
  file.path <- paste("working/HY", 
                      harvest.year, 
                      "_cleanedData_",
                      date.today,
                      ".csv",
                      sep = "")
  write_csv(df, file.path, na = "")
}

compare_cols <- function(df.in, column1, column2) {
  foo <- NULL
  compare <- df.in[column1] == df.in[column2]
  if(nrow(compare) < nrow(df.in[column1])) { stop("Columns do not match") }
}

append_georef_to_df <- function(df, row.name, col.name) {
  georef <- st_read("input/CookEast_GeoreferencePoints_171127.json") %>% 
    select(-Strip, -Field) %>% 
    mutate(Row2 = as.character(Row2),
           Column = as.integer(Column))
  
  df.merged <- df %>%
    rename("Row2" = row.name, "Column" = col.name) %>% 
    mutate(Row2 = as.character(Row2),
           Column = as.integer(Column)) %>% 
    full_join(data.frame(st_coordinates(georef),
                         st_set_geometry(georef, NULL)),
              by = c("Column", "Row2"))
  
  return(df.merged)
}

check_yields <- function(df.in, sheet.name, year, yield.column) {
  yield.check <- read_excel("input/StripbyStripAvg2.xlsx", sheet = sheet.name) %>% 
    select(2, yield.column) %>% 
    rename("ID2" = 1, "Yield" = 2)
  yield.total.diff <- df.in %>% 
    filter(Year == year) %>% 
    select(Year, ID2, `total grain yield dry(grams/M2)`) %>% 
    full_join(yield.check, by = "ID2") %>% 
    mutate(yield.diff = `total grain yield dry(grams/M2)` - Yield) %>% 
    summarize(total.diff = sum(yield.diff, na.rm = TRUE))
  if(yield.total.diff[[1]] > 0.01) { stop("Yield check failed") }
}

getUngerDF <- function(worksheet) {
  df <- read_xlsx(
    "input/CAF N data set.xlsx",
    sheet = worksheet,
    col_names = TRUE,
    na = c("na", "NA")
  )  %>% 
    select(c(1:14)) %>% 
    mutate(Year = as.numeric(worksheet)) %>% 
    rename(GrainCarbon = contains("%C"),
           GrainNitrogen = contains("%N"),
           GrainProtein = contains("rotein")) %>% 
    mutate(Column = as.integer(COLUMN),
           Row2 = as.character(ROW2),
           GrainMassDry = as.numeric(`Total grain weight (dry) (g)`),
           GrainCarbon = as.numeric(GrainCarbon),
           GrainNitrogn = as.numeric(GrainNitrogen),
           GrainProtein = as.numeric(GrainProtein)) %>% 
    select(Year,
           Column, 
           Row2,
           Crop,
           GrainMassDry,
           GrainCarbon,
           GrainNitrogen,
           GrainProtein)
  
  return(df)
}

removeOutliers <- function(x, na.rm = TRUE) {
  qnt <- quantile(x, probs=c(0.25, 0.75), na.rm = na.rm)
  H <- 3 * IQR(x, na.rm = na.rm)
  y <- x
  y[x < (qnt[1] - H)] <- NA
  y[x > (qnt[2] + H)] <- NA
  return(y)
}