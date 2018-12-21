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