ellensPoints <- c(19,
                  23,
                  25,
                  43,
                  75,
                  97,
                  123,
                  127,
                  129,
                  133,
                  135,
                  137,
                  153,
                  183,
                  185,
                  189,
                  201,
                  227,
                  229,
                  233,
                  239,
                  251,
                  253,
                  277,
                  304)

df1999_2016 %>% 
  filter(ID2 %in% ellensPoints) %>% 
  write_csv_gridPointSurvey("1999-2016_ForEllen")
