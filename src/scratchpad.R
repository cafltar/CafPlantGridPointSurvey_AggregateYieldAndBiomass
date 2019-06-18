################ gapFillingFunctions.R #######################
### PLAYING ####

foo = df.gapFill %>% 
  #filter(!is.na(Crop)) %>% 
  group_by(Crop) %>% 
  filter(!is.na(ResidueMassDryPerArea) && !is.na(GrainYieldDryPerArea)) %>%
  do(model = lm(ResidueMassDryPerArea ~ GrainYieldDryPerArea, data = .))

fooCoef = tidy(foo, model)
fooCoef

# https://github.com/tidyverse/dplyr/issues/2177
df.gapFill %>% 
  group_by(Crop) %>% 
  filter(!is.na(ResidueMassDryPerArea) && !is.na(GrainYieldDryPerArea)) %>%
  do(lm(ResidueMassDryPerArea ~ GrainYieldDryPerArea, data = .) %>% coef %>% tibble::enframe(name = NULL))

df.gapFill %>% 
  group_by(Crop) %>% 
  filter(!is.na(ResidueMassDryPerArea) && !is.na(GrainYieldDryPerArea)) %>% 
  do(broom::tidy(lm(ResidueMassDryPerArea ~ GrainYieldDryPerArea, data = .)))

# https://stackoverflow.com/questions/40060828/lm-within-mutate-in-group-by
df.gapFill %>% group_by(Crop) %>% mutate(
  r = resid(lm(ResidueMassDryPerArea ~ GrainYieldDryPerArea))
) -> b

# http://douglas-watson.github.io/post/2018-09_dplyr_curve_fitting/
# https://stackoverflow.com/questions/38001345/using-lm-nls-and-glm-to-estimate-population-growth-rate-in-malthusian
bar <- df.gapFill %>% 
  group_by(Crop) %>% 
  do(nls(ResidueMassDryPerArea ~ a * GrainYieldDryPerArea - b, data = ., start = list(a = 0.8, b = 231))) %>% 
  augment(fit)

# graphing to see data so I'm not blind
df.gapFill %>% 
  filter(Crop != "AL" && !is.na(Crop)) %>%
  ggplot(aes(ResidueMassDryPerArea, GrainYieldDryPerArea)) +
  geom_point() +
  facet_wrap(vars(Crop))

# Crop values of "AL" are empty and some NA values present -- This one works!
df.gapFill %>% 
  filter(Crop != "AL", !is.na(Crop)) %>% 
  group_by(Crop) %>% 
  do(broom::tidy(lm(ResidueMassDryPerArea ~ GrainYieldDryPerArea, data = .))) %>% 
  print(n = 22)

# Now I need to do it by crop and year...
df.gapFill %>% 
  filter(Crop != "AL", !is.na(Crop)) %>% 
  group_by(Crop, HarvestYear) %>% 
  tally() %>%  
  filter(n > 0) %>% 
  ungroup() %>% print(n = 100)

# But now data are grouped, did tally do that?
df.gapFill %>% 
  filter(Crop != "AL", !is.na(Crop)) %>% 
  group_by(Crop, HarvestYear) %>% 
  add_tally() %>%  
  filter(n > 3) %>% 
  ungroup() %>% select(HarvestYear, Crop, ID2, n) %>% print(n = 100)

# Ahh! Use "add_tally"!!
# *********************** Working *************
df.gapFill %>% 
  filter(Crop != "AL", !is.na(Crop)) %>% 
  group_by(Crop, HarvestYear) %>% 
  #add_tally() %>%  
  #filter(n > 3) %>% 
  filter(!is.na(ResidueMassDryPerArea), !is.na(GrainYieldDryPerArea)) %>%
  do(broom::tidy(lm(ResidueMassDryPerArea ~ GrainYieldDryPerArea, data = .))) %>% 
  print(n = 200)

# Hmm... maybe issue was with how I was filtering all along...

# read this, first thing! http://stat545.com/block023_dplyr-do.html
df.gapFill %>% 
  filter(Crop != "AL", !is.na(Crop)) %>% 
  group_by(Crop, HarvestYear) %>% 
  filter(!is.na(ResidueMassDryPerArea), !is.na(GrainYieldDryPerArea)) %>%
  do(le_lin_fit(.)) %>% 
  print(n = 200)

# Using broom
fits <- df.gapFill %>% 
  filter(Crop != "AL", !is.na(Crop)) %>% 
  group_by(Crop, HarvestYear) %>% 
  filter(!is.na(ResidueMassDryPerArea), !is.na(GrainYieldDryPerArea)) %>%
  do(fit = lm(ResidueMassDryPerArea ~ GrainYieldDryPerArea, .))
fits %>% augment(fit)

# https://cran.r-project.org/web/packages/broom/vignettes/broom_and_dplyr.html
library(purrr)
df.cropClean <- df.gapFill %>% 
  filter(Crop != "AL", !is.na(Crop))

nested <- df.cropClean %>% nest(-Crop)
nested %>% 
  mutate(
    test = map(data, ~ cor.test(.x$GrainYieldDryPerArea, .x$ResidueMassDryPerArea)),
    tidied = map(test, tidy)
  ) %>% 
  unnest(tidied, .drop = T)

nestedCropHarvestYear %>% 
  mutate(
    test = map(data, ~ cor.test(.x$GrainYieldDryPerArea, .x$ResidueMassDryPerArea)),
    tidied = map(test, tidy)
  ) %>% 
  unnest(tidied, .drop = T)

nestedCropHarvestYear <- df.cropClean %>% 
  filter(!is.na(GrainYieldDryPerArea),
         !is.na(ResidueMassDryPerArea)) %>% 
  nest(-Crop, -HarvestYear)
foo <- nestedCropHarvestYear %>% 
  mutate(
    count = map(data, ~ count(.x))
  ) %>% 
  unnest(count, .drop = T)

#foo %>% 
#  mutate(
#    test = map(data, ~ cor.test(.x$GrainYieldDryPerArea, .x$ResidueMassDryPerArea)),
#    tidied = map(test, tidy)
#  ) %>% 
#  unnest(tidied, .drop = T)

# *********************** Working *************
model <- df.cropClean %>% 
  filter(!is.na(GrainYieldDryPerArea),
         !is.na(ResidueMassDryPerArea)) %>% 
  nest(-Crop, -HarvestYear) %>% 
  mutate(
    fit = map(data, ~ lm(ResidueMassDryPerArea ~ GrainYieldDryPerArea, data = .x)),
    tidied = map(fit, tidy)
  ) %>% 
  unnest(tidied) %>% 
  select(Crop, HarvestYear, term, estimate) %>% 
  spread(term, estimate) %>% 
  rename(ModelIntercept = `(Intercept)`,
         ModelX = GrainYieldDryPerArea)

# Compare two working versions
df.gapFill %>% 
  filter(Crop != "AL", !is.na(Crop)) %>% 
  group_by(Crop, HarvestYear) %>% 
  #add_tally() %>%  
  #filter(n > 3) %>% 
  filter(!is.na(ResidueMassDryPerArea), !is.na(GrainYieldDryPerArea)) %>%
  do(broom::tidy(lm(ResidueMassDryPerArea ~ GrainYieldDryPerArea, data = .))) %>% 
  select(Crop, HarvestYear, term, estimate) %>% 
  spread(term, estimate)
