#!/usr/bin/env Rscript --vanilla
#
# Fix for 2015 data - uses complete yearly archive
# The 2015 weekly directory was missing early 2015 data
# The yearly archive contains all 53 weekly snapshots

library(data.table)
library(lubridate)
library(hutils)

stopifnot(file.exists("DESCRIPTION"))
source("data-raw/post2001fread_dat.R")

WEEKLY_PSI_DIR <- "data-raw/Weekly-PSI"
year_dir <- file.path(WEEKLY_PSI_DIR, "2015_full")

# Use existing 2015_full directory with complete data
all_b_file <- file.path(year_dir, "ALL-B.txt")

if (!file.exists(all_b_file)) {
  stop("2015_full/ALL-B.txt not found. Run fix-2015-data.R first.")
}

cat("Processing 2015 from complete yearly archive...\n")
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

# Fix date typos
CURRENT_YEAR <- year(Sys.Date())
dat[year(Settlement_date) > CURRENT_YEAR,
    Settlement_date := Settlement_date - years(year(Settlement_date) - CURRENT_YEAR)]

# Fix contract date typos
fix_year <- function(wrong, right) {
  dat[year(Contract_date) == wrong,
      Contract_date := as.Date(paste0(right, "-",
                                      month(Contract_date), "-",
                                      mday(Contract_date)))]
}
for (y in 14:16) {
  fix_year(as.integer(paste0("02", y)), as.integer(paste0("20", y)))
  fix_year(as.integer(paste0("10", y)), as.integer(paste0("20", y)))
}

# Remove Record_type
if ("Record_type" %in% names(dat)) {
  dat[, Record_type := NULL]
}

# Deduplicate
key_cols <- c("District_code", "Property_id", "Sale_counter", "Settlement_date")
before <- nrow(dat)
dat <- unique(dat, by = key_cols)
after <- nrow(dat)
cat("Deduplicated:", before, "->", after, "\n")

# Order
setorder(dat, Settlement_date, Property_id)

# DEALING_ID
if ("Dealing_no" %in% names(dat)) {
  dealing_map <- dat[, .(Dealing_no)] |> unique()
  dealing_map[, DEALING_ID := .I]
  dat <- dealing_map[dat, on = "Dealing_no"]
  dat[, Dealing_no := NULL]
}

# Reorder columns
setcolorder(dat, c("Settlement_date", "Property_id"))

# Summary
cat("Records:", nrow(dat), "\n")
cat("Date range:", as.character(min(dat$Settlement_date, na.rm = TRUE)),
    "to", as.character(max(dat$Settlement_date, na.rm = TRUE)), "\n")

# Monthly breakdown for 2015
monthly <- dat[year(Settlement_date) == 2015, .N, by = month(Settlement_date)]
setnames(monthly, "month", "Month")
setorder(monthly, Month)
cat("\n2015 Monthly breakdown:\n")
print(monthly)
cat("\nJan-May 2015:", dat[year(Settlement_date) == 2015 & month(Settlement_date) <= 5, .N], "\n")
cat("Jun-Dec 2015:", dat[year(Settlement_date) == 2015 & month(Settlement_date) >= 6, .N], "\n")

# Save
PropertySales2015 <- dat
save(PropertySales2015, file = "data/PropertySales2015.rda", compress = "xz")
cat("\nSaved: data/PropertySales2015.rda\n")

fwrite(dat, "tsv/PropertySales2015.tsv", sep = "\t")
cat("Saved: tsv/PropertySales2015.tsv\n")
