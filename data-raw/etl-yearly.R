#!/usr/bin/env Rscript --vanilla
#
# ETL Script for NSW Property Sales Data
# Creates yearly PropertySales{YEAR}.rda files
#
# Usage: Rscript --vanilla data-raw/etl-yearly.R [year]
#        Rscript --vanilla data-raw/etl-yearly.R        # Process all years
#        Rscript --vanilla data-raw/etl-yearly.R 2023   # Process single year

library(data.table)
library(lubridate)
library(hutils)

stopifnot(file.exists("DESCRIPTION"))

# Configuration
BASE_URL <- "https://www.valuergeneral.nsw.gov.au/__psi/yearly/"
WEEKLY_PSI_DIR <- "data-raw/Weekly-PSI"
provide.dir(WEEKLY_PSI_DIR)
provide.dir("data")
provide.dir("tsv")

# Source parsing functions
source("data-raw/post2001fread_dat.R")
source("data-raw/pre2001_fread_dat.R")

#' Download and extract yearly archive
#' @param year Integer year
#' @return Path to extracted directory
download_year <- function(year) {

  year_dir <- file.path(WEEKLY_PSI_DIR, year)
  zip_file <- file.path(WEEKLY_PSI_DIR, paste0(year, ".zip"))

  # Download if needed

  if (!file.exists(zip_file)) {
    url <- paste0(BASE_URL, year, ".zip")
    cat("  Downloading:", url, "\n")
    tryCatch(
      download.file(url, destfile = zip_file, mode = "wb", quiet = TRUE),
      error = function(e) {
        cat("  ERROR downloading:", e$message, "\n")
        return(NULL)
      }
    )
  }

  if (!file.exists(zip_file)) return(NULL)

  # Extract main archive
  provide.dir(year_dir)
  if (length(list.files(year_dir, recursive = TRUE)) == 0) {
    cat("  Extracting main archive...\n")
    unzip(zip_file, exdir = year_dir)
  }

  # Extract nested weekly zips
  nested_zips <- list.files(year_dir, pattern = "\\.zip$", full.names = TRUE)
  for (nz in nested_zips) {
    tryCatch(
      unzip(nz, exdir = year_dir, overwrite = FALSE),
      warning = function(w) NULL,
      error = function(e) NULL
    )
  }

  return(year_dir)
}

#' Concatenate DAT files into ALL-B.txt
#' @param year_dir Path to year directory
#' @return Path to ALL-B.txt
create_all_b <- function(year_dir) {
  all_b_file <- file.path(year_dir, "ALL-B.txt")

  if (file.exists(all_b_file) && file.size(all_b_file) > 1000) {
    return(all_b_file)
  }

  cat("  Creating ALL-B.txt...\n")

  # Find all DAT files recursively (handles nested directories)
  dat_files <- list.files(year_dir, pattern = "\\.DAT$",
                          recursive = TRUE, full.names = TRUE)

  if (length(dat_files) == 0) {
    # Try txt files for older formats
    txt_files <- list.files(year_dir, pattern = "B\\.txt$",
                            recursive = TRUE, full.names = TRUE)
    if (length(txt_files) > 0) {
      all_lines <- unlist(lapply(txt_files, readLines, warn = FALSE))
      writeLines(all_lines, all_b_file)
      return(all_b_file)
    }
    return(NULL)
  }

  # Read and concatenate B records
  all_lines <- character(0)
  for (f in dat_files) {
    lines <- readLines(f, warn = FALSE)
    b_lines <- lines[grepl("^B", lines)]
    all_lines <- c(all_lines, b_lines)
  }

  writeLines(all_lines, all_b_file)
  cat("    Written", length(all_lines), "B records\n")

  return(all_b_file)
}

#' Process post-2001 data
#' @param all_b_file Path to ALL-B.txt
#' @param year Integer year
#' @return data.table of processed sales
process_post2001 <- function(all_b_file, year) {
  dat <- post2001fread_dat(all_b_file)

  # Clean character columns
  for (j in seq_along(dat)) {
    v <- dat[[j]]
    if (is.character(v)) {
      set(dat, j = j, value = fifelse(v == "", NA_character_, v))
    }
  }

  # Remove artifact columns
  artifact_cols <- grep("^V[0-9]+$", names(dat), value = TRUE)
  if (length(artifact_cols) > 0) {
    dat[, (artifact_cols) := NULL]
  }

  # Convert areas to square metres
  if (all(c("Area", "Area_units") %in% names(dat))) {
    dat[, Area_sqm := Area]
    dat[Area_units == "H", Area_sqm := 10000 * Area]
    dat[, c("Area", "Area_units") := NULL]
  }

  # Fix settlement date typos
  CURRENT_YEAR <- year(Sys.Date())
  if ("Settlement_date" %in% names(dat)) {
    dat[year(Settlement_date) > CURRENT_YEAR,
        Settlement_date := Settlement_date - years(year(Settlement_date) - CURRENT_YEAR)]
  }

  # Fix contract date typos (common patterns: 0YYY -> 2YYY, 1YYY -> 20YY)
  if ("Contract_date" %in% names(dat)) {
    fix_year <- function(wrong, right) {
      dat[year(Contract_date) == wrong,
          Contract_date := as.Date(paste0(right, "-",
                                          month(Contract_date), "-",
                                          mday(Contract_date)))]
    }
    # Typos like 0219 -> 2019
    for (y in 15:25) {
      fix_year(as.integer(paste0("02", y)), as.integer(paste0("20", y)))
      fix_year(as.integer(paste0("10", y)), as.integer(paste0("20", y)))
    }
  }

  # Remove Record_type column
  if ("Record_type" %in% names(dat)) {
    dat[, Record_type := NULL]
  }

  # Deduplicate by composite key
  key_cols <- c("District_code", "Property_id", "Sale_counter", "Settlement_date")
  if (all(key_cols %in% names(dat))) {
    before <- nrow(dat)
    dat <- unique(dat, by = key_cols)
    after <- nrow(dat)
    if (before > after) {
      cat("    Deduplicated:", before, "->", after, "\n")
    }
  }

  # Order by settlement date and property ID
  if (all(c("Settlement_date", "Property_id") %in% names(dat))) {
    setorder(dat, Settlement_date, Property_id)
  }

  # Convert Dealing_no to DEALING_ID (per-year IDs)
  if ("Dealing_no" %in% names(dat)) {
    dealing_map <- dat[, .(Dealing_no)] |> unique()
    dealing_map[, DEALING_ID := .I]
    dat <- dealing_map[dat, on = "Dealing_no"]
    dat[, Dealing_no := NULL]
  }

  # Reorder columns
  first_cols <- c("Settlement_date", "Property_id")
  first_cols <- first_cols[first_cols %in% names(dat)]
  if (length(first_cols) > 0) {
    other_cols <- setdiff(names(dat), first_cols)
    setcolorder(dat, c(first_cols, other_cols))
  }

  return(dat)
}

#' Process pre-2001 data
#' @param year_dir Path to year directory
#' @param year Integer year
#' @return data.table of processed sales
process_pre2001 <- function(year_dir, year) {
  # Find data files
  dat_files <- list.files(year_dir, pattern = "\\.DAT$|\\.txt$",
                          recursive = TRUE, full.names = TRUE)

  if (length(dat_files) == 0) {
    return(NULL)
  }

  # Process each file
  dat_list <- lapply(dat_files, function(f) {
    tryCatch(
      pre2001_fread_dat(f, v1 = "B"),
      error = function(e) NULL
    )
  })

  dat_list <- dat_list[!sapply(dat_list, is.null)]
  if (length(dat_list) == 0) return(NULL)

  dat <- rbindlist(dat_list, use.names = TRUE, fill = TRUE)

  # Clean character columns
  for (j in seq_along(dat)) {
    v <- dat[[j]]
    if (is.character(v)) {
      set(dat, j = j, value = fifelse(v == "", NA_character_, v))
    }
  }

  # Remove Record_type column
  if ("Record_type" %in% names(dat)) {
    dat[, Record_type := NULL]
  }

  # Convert areas to square metres
  if (all(c("Area", "Area_units") %in% names(dat))) {
    dat[, Area_sqm := Area]
    dat[Area_units == "H", Area_sqm := 10000 * Area]
    dat[, c("Area", "Area_units") := NULL]
  }

  # Parse Contract_date if needed
  if ("Contract_date" %in% names(dat) && is.character(dat$Contract_date)) {
    dat[, Contract_date := ymd(Contract_date)]
  }

  # Deduplicate
  key_cols <- intersect(c("District_code", "Property_id", "Contract_date"), names(dat))
  if (length(key_cols) >= 2) {
    before <- nrow(dat)
    dat <- unique(dat, by = key_cols)
    after <- nrow(dat)
    if (before > after) {
      cat("    Deduplicated:", before, "->", after, "\n")
    }
  }

  # Order
  if ("Contract_date" %in% names(dat)) {
    setorder(dat, Contract_date, Property_id)
  }

  return(dat)
}

#' Process a single year
#' @param year Integer year
#' @return TRUE if successful
process_year <- function(year) {
  cat("\n=== Processing", year, "===\n")

  # Download and extract
  year_dir <- download_year(year)
  if (is.null(year_dir)) {
    cat("  Failed to download/extract\n")
    return(FALSE)
  }

  # Process based on year
  if (year <= 2000) {
    dat <- process_pre2001(year_dir, year)
  } else {
    all_b_file <- create_all_b(year_dir)
    if (is.null(all_b_file)) {
      cat("  No data files found\n")
      return(FALSE)
    }
    dat <- process_post2001(all_b_file, year)
  }

  if (is.null(dat) || nrow(dat) == 0) {
    cat("  No records processed\n")
    return(FALSE)
  }

  # Summary
  cat("  Records:", nrow(dat), "\n")
  if ("Settlement_date" %in% names(dat)) {
    cat("  Date range:",
        as.character(min(dat$Settlement_date, na.rm = TRUE)), "to",
        as.character(max(dat$Settlement_date, na.rm = TRUE)), "\n")
  } else if ("Contract_date" %in% names(dat)) {
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

# Main execution
args <- commandArgs(trailingOnly = TRUE)

if (length(args) == 0) {
  # Process all years
  years <- 1990:2024
  cat("Processing all years:", min(years), "to", max(years), "\n")
} else {
  years <- as.integer(args)
}

results <- sapply(years, function(y) {
  tryCatch(
    process_year(y),
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
