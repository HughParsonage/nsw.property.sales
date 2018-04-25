library(data.table)
library(httr)
library(magrittr)
library(hutils)
provide.dir("data-raw/land-values")

GET(url = "http://www.valuergeneral.nsw.gov.au/land_value_summaries/lvfiles/LV_20171101.zip",
    write_disk("data-raw/land-values/LV_20171101.zip"))

unzip("data-raw/land-values/LV_20171101.zip", exdir = "data-raw/land-values/LV_20171101")

LV20171101 <-
  list.files("data-raw/land-values/LV_20171101/",
             pattern = "\\.csv$",
             full.names = TRUE) %>%
  lapply(fread, na.strings = "") %>%
  rbindlist(use.names = TRUE, fill = TRUE) %>%
  setnames("PROPERTY ID", "Property_id")
