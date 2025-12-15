#!/usr/bin/env Rscript --vanilla
#
# ETL Script for Pre-2001 NSW Property Sales Data (1990-2000)
# Adapts existing put-pre2001.R logic for yearly output
#
# The pre-2001 archives contain ARCHIVE_SALES_YYYY.DAT files
# with a different schema than post-2001 data.

library(data.table)
library(lubridate)
library(hutils)
library(magrittr)

stopifnot(file.exists("DESCRIPTION"))

source("data-raw/pre2001_fread_dat.R")
source("data-raw/date-correction.R")

BASE_URL <- "https://www.valuergeneral.nsw.gov.au/__psi/yearly/"
WEEKLY_PSI_DIR <- "data-raw/Weekly-PSI"
provide.dir(WEEKLY_PSI_DIR)
provide.dir("data")
provide.dir("tsv")

process_pre2001_year <- function(year) {
  cat("\n=== Processing", year, "===\n")

  year_dir <- file.path(WEEKLY_PSI_DIR, year)
  zip_file <- file.path(WEEKLY_PSI_DIR, paste0(year, ".zip"))

 # Download if needed
  if (!file.exists(zip_file)) {
    url <- paste0(BASE_URL, year, ".zip")
    cat("  Downloading:", url, "\n")
    download.file(url, destfile = zip_file, mode = "wb", quiet = TRUE)
  }

  # Extract - force overwrite to get the DAT files
  provide.dir(year_dir)
  cat("  Extracting archive...\n")
  unzip(zip_file, exdir = year_dir, overwrite = TRUE)

  # Find the ARCHIVE_SALES_YYYY.DAT file
  dat_files <- list.files(year_dir, pattern = "\\.DAT$",
                          full.names = TRUE, recursive = TRUE)
  cat("  Found", length(dat_files), "DAT files\n")

  if (length(dat_files) == 0) {
    cat("  No DAT files found\n")
    return(FALSE)
  }

  # Process using existing pre2001_fread_dat function
  dat_list <- lapply(dat_files, function(f) {
    cat("    Processing:", basename(f), "\n")
    tryCatch(
      pre2001_fread_dat(f, v1 = "B"),
      error = function(e) {
        cat("      Error:", e$message, "\n")
        NULL
      }
    )
  })

  dat_list <- dat_list[!sapply(dat_list, is.null)]
  if (length(dat_list) == 0) {
    cat("  No records processed\n")
    return(FALSE)
  }

  dat <- rbindlist(dat_list, use.names = TRUE, fill = TRUE)

  # Apply existing put-pre2001.R transformations
  # Convert areas to square metres
  if (all(c("Area", "Area_units") %in% names(dat))) {
    dat[, Area_sqm := Area]
    dat[Area_units == "H", Area_sqm := 10000 * Area]
    dat[, c("Area", "Area_units") := NULL]
  }

  # Parse Contract_date (pre-2001 uses dmy format)
  if ("Contract_date" %in% names(dat) && is.character(dat$Contract_date)) {
    dat[, Contract_date := dmy(Contract_date)]
  }

  # Apply date corrections with tracking
  # See data-raw/date-correction.R for correction codes
  dat <- correct_dates_pre2001(dat, file_year = year)

  # Remove columns as per existing logic
  drop_these <- intersect(c("Record_type", "Valuation_num"), names(dat))
  if (length(drop_these) > 0) {
    dat[, (drop_these) := NULL]
  }

  # Clean character columns
  for (j in seq_along(dat)) {
    v <- dat[[j]]
    if (is.character(v)) {
      set(dat, j = j, value = fifelse(v == "", NA_character_, v))
    }
  }

  # Order by Contract_date (no Settlement_date in pre-2001)
  if ("Contract_date" %in% names(dat)) {
    setorder(dat, Contract_date, Property_id)
    setcolorder(dat, c("Contract_date", setdiff(names(dat), "Contract_date")))
  }

  # Summary
  cat("  Records:", nrow(dat), "\n")
  if ("Contract_date" %in% names(dat)) {
    cat("  Date range:",
        as.character(min(dat$Contract_date, na.rm = TRUE)), "to",
        as.character(max(dat$Contract_date, na.rm = TRUE)), "\n")
  }

  # Save
  obj_name <- paste0("PropertySales", year)
  assign(obj_name, dat)

  rda_file <- file.path("data", paste0(obj_name, ".rda"))
  save(list = obj_name, file = rda_file, compress = "xz")
  cat("  Saved:", rda_file, "\n")

  tsv_file <- file.path("tsv", paste0(obj_name, ".tsv"))
  fwrite(dat, tsv_file, sep = "\t")
  cat("  Saved:", tsv_file, "\n")

  return(TRUE)
}

# Process 1990-2000
years <- 1990:2000

results <- sapply(years, function(y) {
  tryCatch(
    process_pre2001_year(y),
    error = function(e) {
      cat("  ERROR:", e$message, "\n")
      FALSE
    }
  )
})

cat("\n=== Summary ===\n")
cat("Successful:", sum(results), "/", length(years), "\n")
if (any(!results)) {
  cat("Failed years:", years[!results], "\n")
}
