ZoningPre2011 <- fread("data-raw/decoders/ZoningPre2011.tsv")

PropertySales19902000 <-
  list.files(path = "data-raw/Weekly-PSI/",
             pattern = "(199[0-9]|2000)\\.DAT$",
             full.names = TRUE,
             recursive = TRUE) %>%
  lapply(pre2001_fread_dat) %>%
  rbindlist(use.names = TRUE, fill = TRUE) %>%
  .[, Area_sqm := Area] %>%
  .[Area_units == "H", Area_sqm := 10e3 * Area] %>%
  .[, Contract_date := dmy(Contract_date)] %>%
  drop_cols(c("Area", "Area_units", "Record_type", "Valuation_num")) %>%
  setorderv("Contract_date") %>%
  set_cols_first("Contract_date") %>%
  .[]


