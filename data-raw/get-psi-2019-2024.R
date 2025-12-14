library(data.table)
library(lubridate)
library(hutils)

stopifnot(file.exists("DESCRIPTION"))
provide.dir("./data-raw/Weekly-PSI")

# Base URL changed to HTTPS
base_url <- "https://www.valuergeneral.nsw.gov.au/__psi/yearly/"

# Years to download
years_to_download <- 2019:2024

# Function to download and extract yearly data
get_yearly_data <- function(year) {
  cat("Processing year:", year, "\n")

  dest_dir <- file.path("data-raw", "Weekly-PSI", year)
  provide.dir(dest_dir)

  zip_file <- file.path("data-raw", "Weekly-PSI", paste0(year, ".zip"))

  # Download if not already present

  if (!file.exists(zip_file)) {
    url <- paste0(base_url, year, ".zip")
    cat("  Downloading from:", url, "\n")
    download.file(url, destfile = zip_file, mode = "wb", quiet = TRUE)
  }

  # Extract main zip
  if (length(list.files(dest_dir, pattern = "\\.DAT$|\\.txt$", recursive = TRUE)) == 0) {
    cat("  Extracting main zip...\n")
    unzip(zip_file, exdir = dest_dir)

    # Extract any nested zips
    nested_zips <- list.files(dest_dir, pattern = "\\.zip$", full.names = TRUE)
    for (nz in nested_zips) {
      cat("  Extracting nested zip:", basename(nz), "\n")
      unzip(nz, exdir = dest_dir)
    }
  }

  # Concatenate DAT files into ALL-B.txt if not already done
  all_b_file <- file.path(dest_dir, "ALL-B.txt")
  if (!file.exists(all_b_file)) {
    cat("  Creating ALL-B.txt...\n")
    dat_files <- list.files(dest_dir, pattern = "\\.DAT$", full.names = TRUE, recursive = TRUE)

    if (length(dat_files) > 0) {
      all_lines <- unlist(lapply(dat_files, readLines, warn = FALSE))
      b_lines <- all_lines[grepl("^B", all_lines)]
      writeLines(b_lines, all_b_file)
      cat("    Written", length(b_lines), "B records\n")
    } else {
      # Try txt files instead
      txt_files <- list.files(dest_dir, pattern = "B\\.txt$", full.names = TRUE, recursive = TRUE)
      if (length(txt_files) > 0) {
        all_lines <- unlist(lapply(txt_files, readLines, warn = FALSE))
        writeLines(all_lines, all_b_file)
        cat("    Written", length(all_lines), "records from txt files\n")
      }
    }
  }

  return(dest_dir)
}

# Download all years
for (year in years_to_download) {
  tryCatch(
    get_yearly_data(year),
    error = function(e) {
      cat("  ERROR for year", year, ":", e$message, "\n")
    }
  )
}

cat("\nDownload complete. Processing data...\n")

# Source the parsing function
source("data-raw/post2001fread_dat.R")

# Process each year
process_year <- function(year) {
  cat("Processing year:", year, "\n")

  all_b_file <- file.path("data-raw", "Weekly-PSI", year, "ALL-B.txt")

  if (!file.exists(all_b_file)) {
    cat("  No ALL-B.txt found for year", year, "\n")
    return(NULL)
  }

  dat <- post2001fread_dat(all_b_file)

  # Clean character columns
  for (j in seq_along(dat)) {
    v <- dat[[j]]
    if (is.character(v)) {
      set(dat, j = j, value = fifelse(v == "", NA_character_, v))
    }
  }

  # Convert areas to square metres
  if ("Area" %in% names(dat) && "Area_units" %in% names(dat)) {
    dat[, Area_sqm := Area]
    dat[Area_units == "H", Area_sqm := 10e3 * Area]
    dat[, c("Area", "Area_units") := NULL]
  }

  # Fix date typos
  CURRENT_YEAR <- year(Sys.Date())
  if ("Settlement_date" %in% names(dat)) {
    dat[year(Settlement_date) > CURRENT_YEAR,
        Settlement_date := Settlement_date - years(year(Settlement_date) - CURRENT_YEAR)]
  }

  # Fix contract date typos (common patterns)
  if ("Contract_date" %in% names(dat)) {
    fix_year <- function(wrong, right) {
      dat[year(Contract_date) == wrong,
          Contract_date := as.Date(paste0(right, "-", month(Contract_date), "-", mday(Contract_date)))]
    }
    # Common typos
    for (wrong_yr in c(0219, 0220, 0221, 0222, 0223, 0224)) {
      right_yr <- as.integer(paste0("2", substr(as.character(wrong_yr), 3, 4)))
      fix_year(wrong_yr, right_yr)
    }
    for (wrong_yr in c(1019, 1020, 1021, 1022, 1023, 1024)) {
      right_yr <- as.integer(paste0("20", substr(as.character(wrong_yr), 3, 4)))
      fix_year(wrong_yr, right_yr)
    }
  }

  # Remove Record_type column
  if ("Record_type" %in% names(dat)) {
    dat[, Record_type := NULL]
  }

  # Order by settlement date and property ID
  if (all(c("Settlement_date", "Property_id") %in% names(dat))) {
    setorder(dat, Settlement_date, Property_id)
  }

  # Convert Dealing_no to DEALING_ID
  if ("Dealing_no" %in% names(dat)) {
    dealing_map <- dat[, .(Dealing_no)] |> unique()
    dealing_map[, DEALING_ID := .I]
    dat <- dealing_map[dat, on = "Dealing_no"]
    dat[, Dealing_no := NULL]
  }

  # Reorder columns
  if (all(c("Settlement_date", "Property_id") %in% names(dat))) {
    setcolorder(dat, c("Settlement_date", "Property_id"))
  }

  cat("  Processed", nrow(dat), "records\n")
  return(dat)
}

# Process years 2019-2024 and combine
PropertySales2019_2024_list <- list()
for (year in years_to_download) {
  result <- tryCatch(
    process_year(year),
    error = function(e) {
      cat("  ERROR processing year", year, ":", e$message, "\n")
      return(NULL)
    }
  )
  if (!is.null(result)) {
    PropertySales2019_2024_list[[as.character(year)]] <- result
  }
}

# Combine all years
if (length(PropertySales2019_2024_list) > 0) {
  PropertySales2019_2024 <- rbindlist(PropertySales2019_2024_list, use.names = TRUE, fill = TRUE)

  cat("\nTotal records:", nrow(PropertySales2019_2024), "\n")
  cat("Date range:", as.character(min(PropertySales2019_2024$Settlement_date, na.rm = TRUE)),
      "to", as.character(max(PropertySales2019_2024$Settlement_date, na.rm = TRUE)), "\n")

  # Save data
  provide.dir("tsv")
  fwrite(PropertySales2019_2024, "tsv/PropertySales2019_2024.tsv", sep = "\t")
  cat("Written TSV to tsv/PropertySales2019_2024.tsv\n")

  usethis::use_data(PropertySales2019_2024, overwrite = TRUE, compress = "xz")
  cat("Written RDA to data/PropertySales2019_2024.rda\n")
} else {
  cat("No data processed.\n")
}
