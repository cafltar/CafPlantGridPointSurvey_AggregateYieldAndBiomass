require(sf)

write_csv_gridPointSurvey <- function(df, 
                                      dictionary,
                                      harvest.year, 
                                      output.folder = "working",
                                      accuracy.level = NA,
                                      processing.level = NA,
                                      suffix = NA) {
  # Writes a csv files for data and data dictionary in defined folder in format of "HY{year}_{current date}_{dictionary or quality & processing code}.csv", sets NAs to blank
  #
  # Args:
  #   df: dataframe to be written as csv
  #   dictionary: dataframe with data dictionary
  #   harvest.year: String of year(s) of data (e.g. "2007" or "1999-2009")
  #   output.folder: String of folder to write the file to
  #   accuracy.level: Number for data quality code (1-6)
  #   processing.level: Number for data processing code (1-3)
  #   suffix: Any additional name specification
  
  # TODO: Add some error checking to see if all columns in df are defined in dictionary
  
  date.today <- format(Sys.Date(), "%Y%m%d")
  data.code <- if(!is.na(accuracy.level) && !is.na(processing.level)) {
    paste("P", sprintf("%01d", processing.level),
          "A", sprintf("%01d", accuracy.level), 
          sep = "")
  } else {
    NA
  }
  
  file.name.base <- paste("HY",
                          harvest.year,
                          "_",
                          date.today,
                          sep = "")
  
  file.name.data <- paste(file.name.base,
                    if(!is.na(data.code)) {paste("_", data.code, sep = "")} else {""},
                    if(!is.na(suffix)) {paste("_", suffix, sep = "")} else {""},
                    ".csv",
                    sep = "")
  
  write_csv(df, file.path(output.folder, file.name.data), na = "")
  
  if(!is.null(dictionary))
  {
    file.name.dict <- paste(file.name.base,
                            if(!is.na(data.code)) {paste("_", data.code, sep = "")} else {""},
                            if(!is.na(suffix)) {paste("_", suffix, sep = "")} else {""},
                            "_Dictionary.csv",
                            sep = "")
    write_csv(dictionary, file.path(output.folder, file.name.dict), na = "")
  }
}

compare_cols <- function(df.in, column1, column2) {
  # Compares two columns, stops if columns don't match
  #
  # Args:
  #   df.in: dataframe with columns to compare
  #   column1: column to compare with column2
  #   column2: column to compare with column1
  
  foo <- NULL
  compare <- df.in[column1] == df.in[column2]
  if(nrow(compare) < nrow(df.in[column1])) { stop("Columns do not match") }
}

append_georef_to_df <- function(df, row.name, col.name) {
  # Reads geojson file with CAF georeference points, merges with df based on columns with row2 and column designations
  #
  # Args:
  #   df: dataframe with CAF row2 and column values
  #   row.name: column name with CAF row2 values
  #   col.name: column name with CAF column values
  
  require(sf)
  
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
  # Compares yield values in df.in with yield values in a dataset cleaned by Kadar
  
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
  # Reads and formats data from an excel file produced by Rachel Unger
  
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

append_cropexists <- function(df) {
  # Load data to assess whether or not yield should be estimated
  df.cropPresent <- read_xlsx("input/HY1999-2016_20190708_NOGapfill with updated cropexistcode.xlsx",
                              sheet = "HY1999-2016_20190708_NOGap") %>% 
    select(HarvestYear, ID2, updateCropwaspresentcode) %>% 
    rename(CropExists = updateCropwaspresentcode)
  
  # Merge datasets
  df.merge <- df %>% 
    left_join(df.cropPresent, by = c("HarvestYear", "ID2"))
}

removeOutliers <- function(x, na.rm = TRUE) {
  # Calculates extreme outlier and sets any to NA
  #
  # Args:
  #   x: dataframe with numeric values to have extreme outliers scrubbed
  #   na.rm: if TRUE, ignores NA values with calculating quantile
  
  qnt <- quantile(x, probs=c(0.25, 0.75), na.rm = na.rm)
  H <- 3 * IQR(x, na.rm = na.rm)
  y <- x
  y[x < (qnt[1] - H)] <- NA
  y[x > (qnt[2] + H)] <- NA
  return(y)
}

getOutlierColumn <- function(x, na.rm = T) {
  # Takes a vector of continuous data (x) and returns an int column with;
  #   1 = outside upper/lower inner fence
  #   2 = outside upper/lower outer fence
  #
  # Args:
  #   x: vector with continues data
  #   na.rm: if TRUE, ignores NA values when calculating quantile
  
  qnt <- quantile(x, probs=c(0.25, 0.75), na.rm = na.rm)
  H <- 1.5 * IQR(x, na.rm = na.rm)
  HExtreme <- 3 * IQR(x, na.rm = na.rm)
  
  outlier <- rep(0, length(x))
  
  outlier[(x < (qnt[1] - H)) | (x > (qnt[2] + H))] <- 1
  outlier[(x < (qnt[1] - HExtreme)) | (x > (qnt[2] + HExtreme))] <- 2
  
  return(outlier)
}

getQuartileColumn <- function(x, na.rm = T) {
  # Takes a vector of continuous data (x) and returns int column with quartile
  # Returns column with rankings:
  #   1 = value is less than or equal to Q1
  #   2 = value is greater than Q1 and less than Q2
  #   3 = value is greater than Q2 and less than Q3
  #   4 = value is greater than Q3
  
  qnt <- quantile(x, probs=seq(0,1,0.25), na.rm = na.rm)
  quartiles <- rep(NA, length(x))
  
  quartiles[x <= qnt[2]] <- 1
  quartiles[x > qnt[2] & x < qnt[3]] <- 2
  quartiles[x >= qnt[3] & x < qnt[4]] <- 3
  quartiles[x >= qnt[4]] <- 4
  
  return(quartiles)
}


getMapQuartileOutliers <- function(sf, varCol, yearCol, cropCol, boundary = NULL, labelCol = NULL) {
  # Creates a map of crop variable emphisizing quartile category and crop type
  #
  # Args:
  #   sf: Simple feature
  #   varCol: Column names with continuous variable to calculate quartiles and outliers
  #   yearCol: Column name with Year data, to be grouped by
  #   cropCol: Column name with crop name data, to be grouped by and used for point color
  # Output
  #   returns a tmap with:
  #   size = Quartile category; 1 = less than Q1, 2 = greater than Q1 and less than Q2, 3 = greater than Q2 and less than Q3, 4 = greater than Q4
  #   color = Crop type
  #   shape = Outlier (0 = not outlier, 1 = inner fence, 2 = outer fence)
  #   boundary = sf polygon to act as boundary to data
  
  require(sf)
  require(tmap)

  # Eval passed arguments
  var_ <- rlang::sym(varCol)
  year_ <- rlang::sym(yearCol)
  crop_ <- rlang::sym(cropCol)
  #label_ <- rlang::sym(labelCol)
  
  # Create a boundary map if boundary provided
  if(!is.null(boundary)) {
    boundary.map <- tm_shape(boundary) +
      tm_borders()
  }
  
  # Check that data are available, if not return with boundary or error
  if(all(is.na(sf %>% select(!!var_) %>% st_drop_geometry()))) {
    if(!is.null(boundary)) {
      return(boundary.map)
    } else {
      stop("Data in column is NA and no border specified, nothing to return")
    }
  }
  
  # Add columns for ranking values based on quartiles and outliers
  data <- sf %>% 
    group_by(!!year_, !!crop_) %>% 
    group_map(~mutate(., Quartiles = as.integer(getQuartileColumn(!!var_)))) %>% 
    group_map(~mutate(., Outliers = as.factor(getOutlierColumn(!!var_)))) %>% 
    ungroup()
  
  # Map it!
  if(!is.null(boundary)) {
    map <- boundary.map +
    tm_shape(data) +
      tm_symbols(col = cropCol, shape = "Outliers", size = "Quartiles")
  } else {
    map <- tm_shape(data) +
      tm_symbols(col = cropCol, shape = "Outliers", size = "Quartiles")
  }
  
  if(!is.null(labelCol)) {
    map <- map + tm_text(labelCol, size = 0.6, ymod = 0.5)
  }
  
  return(map)
}
