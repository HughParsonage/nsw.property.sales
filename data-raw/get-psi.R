library(magrittr)
library(data.table)
library(lubridate)
library(hutils)

stopifnot("DESCRIPTION" %in% list.files())
provide.dir("./data-raw/Weekly-PSI")

download.file <- function(...) {
  utils::download.file(..., mode = "wb")
}

# http://www.valuergeneral.nsw.gov.au/__psi/weekly/20170821.zip

final_week <- as.Date("2018-12-24")

get_psi <- function(week) {
  yr <- as.character(year(week))
  wk <- as.character(week)

  ex_dir <- file.path("data-raw", "Weekly-PSI", yr)
  provide.dir(ex_dir)
  dest_file <- file.path("data-raw", "Weekly-PSI", yr, paste0(wk, ".zip"))
  if (!file.exists(dest_file) &&
      wk != "2018-01-01" &&
      wk != "2017-04-10" && wk != "2017-12-25" &&
      wk != "2018-08-01" && wk != "2018-08-08") {
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

lapply(weeks, get_psi)

# http://www.valuergeneral.nsw.gov.au/__psi/yearly/2016.zip

get_yearly_psi <- function(yr) {
  current_wd <- getwd()
  on.exit(setwd(current_wd))
  ex_dir <- file.path("data-raw", "Weekly-PSI", yr)
  provide.dir(ex_dir)
  dest_file <- file.path("data-raw", "Weekly-PSI", paste0(yr, ".zip"))
  if (!file.exists(dest_file)) {
    remote.zip <- paste0("http://www.valuergeneral.nsw.gov.au/__psi/yearly/", yr, ".zip")
    tryCatch(download.file(url = remote.zip,
                           destfile = dest_file, quiet = TRUE),
             error = function(e) stop(e$message))
    # unzip(dest_file, exdir = ex_dir)
  }

  setwd(ex_dir)
  if (length(list.files(recursive = TRUE)) == 0) {
    cat(ex_dir)
    stop("zip file ", ex_dir, ".zip must be unzipped manually")
  }

  if (yr %between% c(2001, 2016)) {
    current_wd2 <- getwd()
    for (the_dir in list.dirs(recursive = TRUE, full.names = TRUE)) {
      setwd(the_dir)
      if (AND(!file.exists("ALL-B.txt"),
              length(list.files(pattern = "\\.DAT$")) > 2)) {
        # Progress
        if (runif(1) < 0.1) cat(the_dir)

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
      setwd(current_wd2)
    }
  }
}

lapply(1990:2016, get_yearly_psi)

source("data-raw/post2001fread_dat.R")
source("data-raw/pre2001_fread_dat.R")

post2001_Bfiles <-
  lapply(2001:2017,
         function(x) {
           list.files(path = file.path("data-raw/Weekly-PSI", x),
                      pattern = "ALL-B\\.txt$",
                      recursive = TRUE,
                      full.names = TRUE)
         }) %>%
  unlist

PropertySales20012017.list <- list(length(post2001_Bfiles))
# Don't use lapply to make it easier to debug
for (ff in seq_along(post2001_Bfiles)) {
  if (ff %% 100 == 0) {
    cat("\n")
  } else {
    cat(" ")
  }
  cat(ff %% 100)
  PropertySales20012017.list[[ff]] <- post2001fread_dat(file = post2001_Bfiles[[ff]])

}


PropertySales20012017 <-
  rbindlist(PropertySales20012017.list, use.names = TRUE, fill = TRUE)

for (j in seq_along(PropertySales20012017)) {
  v <- PropertySales20012017[[j]]
  if (is.character(v)) {
    set(PropertySales20012017, j = j, value = if_else(v == "", NA_character_, v))
  }
  rm(v)
}
drop_empty_cols(PropertySales20012017)

# Convert all areas to square-metres
PropertySales20012017 %>%
  .[, Area_sqm := Area] %>%
  .[Area_units == "H", Area_sqm := 10e3 * Area] %>%
  drop_cols(c("Area", "Area_units"))

# Convert typos in Date fields to probable values
CURRENT.YEAR <- year(now())

testthat::expect_false(any(year(PropertySales20012017[["Download_datetime"]]) > CURRENT.YEAR))

PropertySales20012017 %>%
  .[and(year(Settlement_date) == 2018,
        year(Download_datetime) == 2001),
    Settlement_date := Settlement_date - years(17)] %>%
  .[and(year(Settlement_date) == 2020,
        year(Download_datetime) == 2001),
    Settlement_date := Settlement_date - years(19)] %>%
  # Give up
  .[year(Settlement_date) > CURRENT.YEAR,
    Settlement_date := Settlement_date - years(year(Settlement_date) - year(Download_datetime))]

drop_col(PropertySales20012017, "Record_type")
setorder(PropertySales20012017, Settlement_date, Property_id)


# Dealing no is almost 1/6th the size
# This doesn't lose much, but cuts down the size
# by 152 MB
DEALING_ID_by_Dealing_no <-
  PropertySales20012017[, .(Dealing_no)] %>%
  unique %>%
  .[, DEALING_ID := .I] %>%
  .[]

PropertySales20012017 <- DEALING_ID_by_Dealing_no[PropertySales20012017, on = "Dealing_no"]
PropertySales20012017[, "Dealing_no" := NULL]

set_cols_first(PropertySales20012017, c("Settlement_date", "Property_id"))

cat("\n")
devtools::use_data(PropertySales20012017, overwrite = TRUE, compress = "xz")



post2001_Bfiles <-
  lapply(2001:2017,
         function(x) {
           list.files(path = file.path("data-raw/Weekly-PSI", x),
                      pattern = "ALL-B\\.txt$",
                      recursive = TRUE,
                      full.names = TRUE)
         }) %>%
  unlist

PropertySales2018.list <- list(length(post2001_Bfiles))
# Don't use lapply to make it easier to debug
for (ff in seq_along(post2001_Bfiles)) {
  if (ff %% 100 == 0) {
    cat("\n")
  } else {
    cat(" ")
  }
  cat(ff %% 100)
  PropertySales2018.list[[ff]] <- post2001fread_dat(file = post2001_Bfiles[[ff]])

}


PropertySales20012017 <-
  rbindlist(PropertySales20012017.list, use.names = TRUE, fill = TRUE)

for (j in seq_along(PropertySales20012017)) {
  v <- PropertySales20012017[[j]]
  if (is.character(v)) {
    set(PropertySales20012017, j = j, value = if_else(v == "", NA_character_, v))
  }
  rm(v)
}
drop_empty_cols(PropertySales20012017)

# Convert all areas to square-metres
PropertySales20012017 %>%
  .[, Area_sqm := Area] %>%
  .[Area_units == "H", Area_sqm := 10e3 * Area] %>%
  drop_cols(c("Area", "Area_units"))

# Convert typos in Date fields to probable values
CURRENT.YEAR <- year(now())

testthat::expect_false(any(year(PropertySales20012017[["Download_datetime"]]) > CURRENT.YEAR))

PropertySales20012017 %>%
  .[and(year(Settlement_date) == 2018,
        year(Download_datetime) == 2001),
    Settlement_date := Settlement_date - years(17)] %>%
  .[and(year(Settlement_date) == 2020,
        year(Download_datetime) == 2001),
    Settlement_date := Settlement_date - years(19)] %>%
  # Give up
  .[year(Settlement_date) > CURRENT.YEAR,
    Settlement_date := Settlement_date - years(year(Settlement_date) - year(Download_datetime))]

drop_col(PropertySales20012017, "Record_type")
setorder(PropertySales20012017, Settlement_date, Property_id)


# Dealing no is almost 1/6th the size
# This doesn't lose much, but cuts down the size
# by 152 MB
DEALING_ID_by_Dealing_no <-
  PropertySales20012017[, .(Dealing_no)] %>%
  unique %>%
  .[, DEALING_ID := .I] %>%
  .[]

PropertySales20012017 <- DEALING_ID_by_Dealing_no[PropertySales20012017, on = "Dealing_no"]
PropertySales20012017[, "Dealing_no" := NULL]

set_cols_first(PropertySales20012017, c("Settlement_date", "Property_id"))

cat("\n")
devtools::use_data(PropertySales20012017, overwrite = TRUE, compress = "xz")


