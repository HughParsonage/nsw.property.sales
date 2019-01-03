library(magrittr)
library(data.table)
library(lubridate)
library(hutils)
library(withr)

stopifnot("DESCRIPTION" %in% list.files())
provide.dir("./data-raw/Weekly-PSI")


download.file <- function(..., mode) {
  utils::download.file(..., mode = "wb")
}
#
yr2018.url <- "http://www.valuergeneral.nsw.gov.au/__psi/yearly/2018.zip"

tempf.zip <- "data-raw/Weekly-PSI/2018.zip"
provide.dir(exdir2018 <- "data-raw/Weekly-PSI/2018")
download.file(yr2018.url, mode = "wb", destfile = tempf.zip)
unzip(tempf.zip, exdir = exdir2018)
invisible(sapply(dir(path = exdir2018, pattern = "\\.zip$", full.names = TRUE),
                 unzip, exdir = exdir2018))


stopifnot(dir.exists("data-raw/Weekly-PSI/2018/"))
with_dir("data-raw/Weekly-PSI/2018/",
         {
           if (!exists("ex_dir")) {
             ex_dir <- "data-raw/Weekly-PSI/2018/"
           }
           if (file.exists("ALL-B.txt")) {
             message("ALL-B.txt already exists in ", ex_dir, ".")
           } else {

             # Instead of lapply rbindlist, use
             # Windows cmd.exe (Much faster and less likely to crash)
             # Simply concatenates all files on top of each other
             # This is OK, because there are no headers, and
             # the files have a simple pattern (starts with a B for prices)
             shell("copy /y /b *.DAT ALL.DAT")
             readr::read_lines("ALL.DAT") %>%
               .[grepl("^B", .)] %>%
               readr::write_lines("ALL-B.txt")

             readr::read_lines("ALL.DAT") %>%
               .[grepl("^C", .)] %>%
               readr::write_lines("ALL-C.txt")
             file.remove("ALL.DAT")
           }
         })


source("data-raw/post2001fread_dat.R")
PropertySales2018 <- post2001fread_dat("data-raw/Weekly-PSI/2018/ALL-B.txt")

for (j in seq_along(PropertySales2018)) {
  v <- PropertySales2018[[j]]
  if (is.character(v)) {
    set(PropertySales2018, j = j, value = if_else(v == "", NA_character_, v))
  }
  rm(v)
}
drop_empty_cols(PropertySales2018)

# Convert all areas to square-metres
PropertySales2018 %>%
  .[, Area_sqm := Area] %>%
  .[Area_units == "H", Area_sqm := 10e3 * Area] %>%
  drop_cols(c("Area", "Area_units"))

# Convert typos in Date fields to probable values
CURRENT.YEAR <- year(now())

testthat::expect_false(any(year(PropertySales2018[["Download_datetime"]]) > CURRENT.YEAR))

PropertySales2018 %>%
  .[and(year(Settlement_date) == 2019,
        year(Download_datetime) == 2001),
    Settlement_date := Settlement_date - years(17)] %>%
  .[and(year(Settlement_date) == 2020,
        year(Download_datetime) == 2001),
    Settlement_date := Settlement_date - years(19)] %>%
  # Give up
  .[year(Settlement_date) > CURRENT.YEAR,
    Settlement_date := Settlement_date - years(year(Settlement_date) - year(Download_datetime))]

drop_col(PropertySales2018, "Record_type")
setorder(PropertySales2018, Settlement_date, Property_id)


# Dealing no is almost 1/6th the size
# This doesn't lose much, but cuts down the size
# by 152 MB
DEALING_ID_by_Dealing_no <-
  PropertySales2018[, .(Dealing_no)] %>%
  unique %>%
  .[, DEALING_ID := .I] %>%
  .[]

PropertySales2018 <- DEALING_ID_by_Dealing_no[PropertySales2018, on = "Dealing_no"]
PropertySales2018[, "Dealing_no" := NULL]

set_cols_first(PropertySales2018, c("Settlement_date", "Property_id"))

cat("\n")
devtools::use_data(PropertySales2018, overwrite = TRUE, compress = "xz")




