#' Date Correction Module for NSW Property Sales Data
#'
#' This module provides functions to detect and correct date typos in
#' Contract_date and Settlement_date fields, adding a correction code
#' column to indicate the type and confidence of any correction made.
#'
#' @section Correction Codes:
#' \describe{
#'   \item{0}{No correction needed - date passes all validation}
#'   \item{1}{Century typo fixed - year 19XX corrected to 20XX where XX is recent (e.g., 1915->2015)}
#'   \item{2}{Leading-zero typo fixed - year 0YYY corrected to 20YY (e.g., 0219->2019)}
#'   \item{3}{Leading-one typo fixed - year 10YY corrected to 20YY (e.g., 1019->2019)}
#'   \item{4}{Bounded by Download_datetime - date exceeded download date, capped}
#'   \item{5}{Contract/Settlement order fixed - dates were swapped}
#'   \item{9}{Unfixable/ambiguous - set to NA, requires manual review}
#' }
#'
#' @section Schema Reference:
#' Date fields are CCYYMMDD format from Notice of Sale (human-entered).
#' Download_datetime is system-generated and reliable.
#'
#' Inferred constraint: Contract_date <= Settlement_date <= Download_datetime
#' (Not explicitly stated in documentation, but follows from definitions:
#' contracts must be exchanged before settled, and settled before file created.)
#'
#' NSW Valuer General documentation:
#'
#' Property_Sales_Data_File_-_Data_Elements_V3.pdf (p.1):
#'   Contract Date: "The calendar date on which contracts were exchanged
#'   as recorded in the Register of Land Values and sourced from the
#'   Notice of Sale."
#'   Settlement Date: "The calendar date on which a contract was settled
#'   as recorded in the Register of Land Values and sourced from the
#'   Notice of Sale."
#'
#' Current_Property_Sales_Data_File_Format_2001_to_Current.pdf (p.2):
#'   Contract Date: "Date", Field Size 8, Required Y, "Format is CCYYMMDD"
#'   Settlement Date: "Date", Field Size 8, Required Y, "Format is CCYYMMDD"
#'   Download Date / Time: "Date", Field Size 16, Required Y,
#'   "The Date / Time for the creation of this file. Format is CCYYMMDD HH24:MI"
#'
#' Archived_Property_Sales_Data_File_Format_1990_to_2001_V2.pdf (p.1):
#'   Contract_date: "Date", Field Size 10, "The calendar date on which
#'   contracts were exchanged as recorded in the Register of Land Values
#'   and sourced from the Notice of Sale. Format is CCYYMMDD"

library(data.table)
library(lubridate)

# Constants
MIN_VALID_YEAR <- 1788L
MAX_VALID_YEAR <- as.integer(year(Sys.Date()))

#' Correct a date column in place using vectorized operations
#' @param dt data.table
#' @param date_col Name of date column to correct
#' @param code_col Name of column to store correction codes
#' @param max_year Maximum valid year (for bounding)
correct_date_column <- function(dt, date_col, code_col, max_year = MAX_VALID_YEAR) {
  if (!date_col %in% names(dt)) return(invisible(NULL))

  date_vec <- dt[[date_col]]
  yr <- year(date_vec)
  mm <- month(date_vec)
  dd <- mday(date_vec)

  # Initialize codes to 0 (no correction)
  codes <- rep(0L, length(date_vec))
  new_dates <- date_vec

  # Pattern 1: Century typo (19XX -> 20XX) where XX suggests recent year
  # e.g., 1915 -> 2015, 1923 -> 2023
  idx <- which(yr >= 1900L & yr <= 1999L & (yr - 1900L + 2000L) <= max_year)
  if (length(idx) > 0) {
    corrected_yr <- yr[idx] - 1900L + 2000L
    new_dates[idx] <- as.Date(paste0(corrected_yr, "-", mm[idx], "-", dd[idx]))
    codes[idx] <- 1L
  }

  # Pattern 2: Leading-zero typo (0YYY -> 20YY)
  # e.g., 0219 -> 2019, 0023 -> 2023
  idx <- which(yr >= 0L & yr <= 999L & (yr %% 100L + 2000L) <= max_year)
  if (length(idx) > 0) {
    corrected_yr <- yr[idx] %% 100L + 2000L
    new_dates[idx] <- as.Date(paste0(corrected_yr, "-", mm[idx], "-", dd[idx]))
    codes[idx] <- 2L
  }

  # Pattern 3: Leading-one typo (10YY -> 20YY)
  # e.g., 1019 -> 2019, 1023 -> 2023
  idx <- which(yr >= 1000L & yr <= 1099L & (yr - 1000L + 2000L) <= max_year)
  if (length(idx) > 0) {
    corrected_yr <- yr[idx] - 1000L + 2000L
    new_dates[idx] <- as.Date(paste0(corrected_yr, "-", mm[idx], "-", dd[idx]))
    codes[idx] <- 3L
  }

  # Pattern 9: Unfixable (ancient dates we can't interpret)
  idx <- which(!is.na(yr) & yr < MIN_VALID_YEAR & codes == 0L)
  if (length(idx) > 0) {
    new_dates[idx] <- as.Date(NA)
    codes[idx] <- 9L
  }

  # Update the data.table
  set(dt, j = date_col, value = new_dates)

  # Update or create code column (keep max code if already exists)
  if (code_col %in% names(dt)) {
    set(dt, j = code_col, value = pmax(dt[[code_col]], codes))
  } else {
    set(dt, j = code_col, value = codes)
  }

  invisible(NULL)
}

#' Apply date corrections to a data.table (post-2001 format)
#' @param dt data.table with Settlement_date, Contract_date, Download_datetime
#' @return data.table with corrected dates and Date_correction_code column
correct_dates_post2001 <- function(dt) {
  dt <- copy(dt)

  # Initialize correction code
  dt[, Date_correction_code := 0L]

  # Get max valid year from Download_datetime where available
  max_year <- fcoalesce(as.integer(max(year(dt$Download_datetime), na.rm = TRUE)), MAX_VALID_YEAR)

  # Correct Settlement_date
  correct_date_column(dt, "Settlement_date", "Date_correction_code", max_year)

  # Correct Contract_date
  correct_date_column(dt, "Contract_date", "Date_correction_code", max_year)

  # Check Contract_date <= Settlement_date (swap if needed)
  if (all(c("Contract_date", "Settlement_date") %in% names(dt))) {
    needs_swap <- !is.na(dt$Contract_date) &
                  !is.na(dt$Settlement_date) &
                  dt$Contract_date > dt$Settlement_date &
                  year(dt$Contract_date) >= MIN_VALID_YEAR &
                  year(dt$Settlement_date) >= MIN_VALID_YEAR

    if (any(needs_swap)) {
      # Swap the dates
      tmp <- dt$Contract_date[needs_swap]
      set(dt, i = which(needs_swap), j = "Contract_date", value = dt$Settlement_date[needs_swap])
      set(dt, i = which(needs_swap), j = "Settlement_date", value = tmp)
      dt[needs_swap, Date_correction_code := pmax(Date_correction_code, 5L)]
    }
  }

  # Check Settlement_date <= Download_datetime (cap if exceeded)
  if (all(c("Settlement_date", "Download_datetime") %in% names(dt))) {
    exceeds_download <- !is.na(dt$Settlement_date) &
                        !is.na(dt$Download_datetime) &
                        dt$Settlement_date > as.Date(dt$Download_datetime)

    if (any(exceeds_download)) {
      set(dt, i = which(exceeds_download), j = "Settlement_date",
          value = as.Date(dt$Download_datetime[exceeds_download]))
      dt[exceeds_download, Date_correction_code := pmax(Date_correction_code, 4L)]
    }
  }

  return(dt)
}

#' Apply date corrections to a data.table (pre-2001 format)
#' @param dt data.table with Contract_date only
#' @param file_year Integer year of the data file (for validation)
#' @return data.table with corrected dates and Date_correction_code column
correct_dates_pre2001 <- function(dt, file_year) {
  dt <- copy(dt)

  # Initialize correction code
  dt[, Date_correction_code := 0L]

  # For pre-2001, use file_year as max reference
  max_year <- as.integer(file_year)

  # Correct Contract_date
  correct_date_column(dt, "Contract_date", "Date_correction_code", max_year)

  return(dt)
}

#' Summary of date corrections applied
#' @param dt data.table with Date_correction_code column
#' @return data.table with correction code counts
summarize_date_corrections <- function(dt) {
  if (!"Date_correction_code" %in% names(dt)) {
    stop("Date_correction_code column not found")
  }

  code_labels <- c(
    "0" = "No correction",
    "1" = "Century typo (19XX->20XX)",
    "2" = "Leading-zero typo (0YYY->20YY)",
    "3" = "Leading-one typo (10YY->20YY)",
    "4" = "Bounded by Download_datetime",
    "5" = "Contract/Settlement swapped",
    "9" = "Unfixable (set to NA)"
  )

  summary_dt <- dt[, .N, by = Date_correction_code]
  summary_dt[, Description := code_labels[as.character(Date_correction_code)]]
  summary_dt[, Percent := round(100 * N / sum(N), 2)]
  setorder(summary_dt, Date_correction_code)

  return(summary_dt)
}
