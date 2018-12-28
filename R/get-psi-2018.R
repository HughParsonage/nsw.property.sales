library(magrittr)
library(data.table)
library(lubridate)
library(hutils)
library(withr)

stopifnot("DESCRIPTION" %in% list.files())
provide.dir("./data-raw/Weekly-PSI")

download.file <- function(...) {
  utils::download.file(..., mode = "wb")
}

if (exists("PropertySales20012017")) rm(PropertySales20012017)

final_week <- as.Date("2018-12-24")

get_psi <- function(week) {
  yr <- as.character(year(week))
  wk <- as.character(week)

  ex_dir <- file.path("data-raw", "Weekly-PSI", yr)
  provide.dir(ex_dir)
  dest_file <- file.path("data-raw", "Weekly-PSI", yr, paste0(wk, ".zip"))
  if (!file.exists(dest_file) &&
      wk != "2017-04-10" && wk != "2017-12-25" &&
      wk != "2018-01-01" && wk != "2018-01-08") {
    cat(wk, "\n")
    remote.zip <- paste0("http://www.valuergeneral.nsw.gov.au/__psi/weekly/",
                         gsub("-", "", wk), ".zip")
    tryCatch(download.file(url = remote.zip, destfile = dest_file, quiet = TRUE),
             # e.g. 2017-04-10 does not exist
             error = function(e) cat(e$message))
    if (file.exists(dest_file)) {
      unzip(dest_file, exdir = ex_dir)
    }
  }
}

weeks <- final_week - weeks(1:(week(final_week) - 1))
cat("getting psi...")
lapply(weeks, get_psi)

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




