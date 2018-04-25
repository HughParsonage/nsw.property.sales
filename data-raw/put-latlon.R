library(nsw.property.sales)
library(magrittr)
library(fastmatch)
library(hutils)
library(data.table)
library(Census2016.DataPack)
library(ASGS)
library(PSMA)
library(ggplot2)


PropertySales20012017_latlon <- copy(PropertySales20012017)
PropertySales20012017_latlon %<>%
  .[, flat_number := as.integer(gsub("[^0-9]", "", Unit_no))] %>%
  .[, number_first := as.integer(gsub("[^0-9]", "", House_no))] %>%
  .[grepl(" PART$", Street), Street := gsub(" PART$", "", Street)] %>%
  .[grepl(" FIRE TRL$", Street), Street := gsub(" FIRE TRL$", " FIRETRAIL", Street)] %>%
  .[, street_name := toupper(gsub("^(.*) [A-Z]+$", "\\1", Street))] %>%
  .[, street_type := gsub("^.* ([A-Z]+)$", "\\1", Street)] %>%
  .[] %>%
  .[, c("II", "LATITUDE", "LONGITUDE") := geocode(flat_number = flat_number,
                                                  number_first = number_first,
                                                  street_name = street_name,
                                                  street_type = street_type,
                                                  postcode = Postcode,
                                                  attempt_decode_street_abbrev = TRUE)] %>%
  .[]


PropertySales20012017_latlon[!is.na(LATITUDE), SA2_NAME16 := latlon2SA(LATITUDE, LONGITUDE, to = "SA2", yr = "2016", return = "v")]

PropertySales19902000_latlon <- copy(PropertySales19902000)
PropertySales19902000_latlon %<>%
  .[, flat_number := as.integer(gsub("[^0-9]", "", Unit_no))] %>%
  .[, number_first := as.integer(gsub("[^0-9]", "", House_no))] %>%
  .[grepl(" PART$", Street), Street := gsub(" PART$", "", Street)] %>%
  .[grepl(" FIRE TRL$", Street), Street := gsub(" FIRE TRL$", " FIRETRAIL", Street)] %>%
  .[, street_name := toupper(gsub("^(.*) [A-Z]+$", "\\1", Street))] %>%
  .[, street_type := gsub("^.* ([A-Z]+)$", "\\1", Street)] %>%
  .[] %>%
  .[, c("II", "LATITUDE", "LONGITUDE") := geocode(flat_number = flat_number,
                                                  number_first = number_first,
                                                  street_name = street_name,
                                                  street_type = street_type,
                                                  postcode = Postcode,
                                                  attempt_decode_street_abbrev = TRUE)] %>%
  .[]

n_dwellings_by_SA2 <-
  Census2016_wide_by_SA2_year %>%
  .[year == 2016, .(SA2_NAME16 = sa2_name, n_dwellings)]

PropertySales20012017_latlon %>%
  .[, .N, keyby = .(SA2_NAME16, Year = year(Contract_date), Month = month(Contract_date))] %>%
  .[Year > 2016] %>%
  .[complete.cases(.)] %>%
  n_dwellings_by_SA2[., on = "SA2_NAME16", nomatch=0L] %>%
  .[, prop_sold := N * uniqueN(Month) / n_dwellings] %$%
  summary(prop_sold)


library(leaflet)
library(grattanCharts)
gpal_v <- colorNumeric(palette = gpal(7, reverse = TRUE), domain = NULL)

DT <-
  PropertySales20012017_latlon %>%
  .[year(Contract_date) > 2016] %>%
  .[Zoning %pin% "Residential"] %>%
  .[Purchase_price > 0] %>%
  .[!is.na(SA2_NAME16)] %>%
  .[, .(avg_purchase_price = median(Purchase_price),
        n_transactions = .N), keyby = .(SA2_NAME16, Zoning)]

DT_Low_density <-
  DT[Zoning == "Low Density Residential"] %>%
  .[, fillColor := gpal_v(avg_purchase_price)] %>%
  .[, labelTitle := paste0(as.character(SA2_NAME16), " avg purchase price")] %>%
  .[, labelText := grattan_dollar(avg_purchase_price)] %>%
  .[]

grattan_leaflet(DT_Low_density, simple = TRUE, Year = "2016")


