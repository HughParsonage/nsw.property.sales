library(nsw.property.sales)
library(nsw.property.sales)
library(magrittr)
library(fastmatch)
library(hutils)
library(data.table)
library(PSMA)
PropertySales20012017_latlon <- copy(PropertySales20012017)
PropertySales20012017_latlon %>%
  .[, flat_number := as.integer(gsub("[^0-9]", "", Unit_no))] %>%
  .[, number_first := as.integer(gsub("[^0-9]", "", House_no))] %>%
  .[grepl(" PART$", Street), Street := gsub(" PART$", "", Street)] %>%
  .[grepl(" FIRE TRL$", Street), Street := gsub(" FIRE TRL$", " FIRETRAIL", Street)] %>%
  .[, street_name := toupper(gsub("^(.*) [A-Z]+$", "\\1", Street))] %>%
  .[, street_type := gsub("^.* ([A-Z]+)$", "\\1", Street)] %>%
  .[] %>%
  head(100) %>%
  .[, c("II", "LATITUDE", "LONGITUDE") := geocode(flat_number = flat_number,
                                                  number_first = number_first,
                                                  street_name = street_name,
                                                  street_type = street_type,
                                                  postcode = Postcode)] %>%
  .[]
