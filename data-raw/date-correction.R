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
#' Logical constraint: Contract_date <= Settlement_date <= Download_datetime
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
RECENT_YEAR_THRESHOLD <- 2000L  # Years >= this are "recent"

#' Detect typo pattern in a year
#' @param yr Integer year value
#' @param reference_year Integer year to use as reference (e.g., from Download_datetime)
#' @return List with corrected_year and correction_code
detect_year_typo <- function(yr, reference_year = MAX_VALID_YEAR) {
  if (is.na(yr)) {
    return(list(corrected = NA_integer_, code = 0L))
  }

  # Valid year - no correction needed
  if (yr >= MIN_VALID_YEAR && yr <= MAX_VALID_YEAR) {
    return(list(corrected = yr, code = 0L))
  }

  # Pattern 1: Century typo (19XX -> 20XX)
  # e.g., 1915 should be 2015, 1923 should be 2023
  if (yr >= 1900L && yr <= 1999L) {
    suffix <- yr - 1900L  # e.g., 1915 -> 15
    candidate <- 2000L + suffix  # e.g., 2015
    # Only correct if the candidate is plausible (within data range)
    if (candidate >= RECENT_YEAR_THRESHOLD && candidate <= reference_year) {
      return(list(corrected = candidate, code = 1L))
    }
  }

  # Pattern 2: Leading-zero typo (0YYY -> 20YY)
  # e.g., 0219 should be 2019, 0115 should be 2015
  if (yr >= 0L && yr <= 999L) {
    # Interpret as 20YY where YY is the last two digits
    suffix <- yr %% 100L
    candidate <- 2000L + suffix
    if (candidate >= RECENT_YEAR_THRESHOLD && candidate <= reference_year) {
      return(list(corrected = candidate, code = 2L))
    }
  }

  # Pattern 3: Leading-one typo (10YY -> 20YY)
  # e.g., 1019 should be 2019, 1023 should be 2023
  if (yr >= 1000L && yr <= 1099L) {
    suffix <- yr - 1000L  # e.g., 1019 -> 19
    candidate <- 2000L + suffix
    if (candidate >= RECENT_YEAR_THRESHOLD && candidate <= reference_year) {
      return(list(corrected = candidate, code = 3L))
    }
  }

  # Pattern: Future year (bounded by reference)
  if (yr > MAX_VALID_YEAR && yr <= MAX_VALID_YEAR + 100L) {
    # Could be a typo like 2025 when we're in 2024
    # Try to infer correct year
    suffix <- yr %% 100L
    candidate <- 2000L + suffix
    if (candidate <= reference_year) {
      return(list(corrected = candidate, code = 4L))
    } else {
      # Can't determine, use reference year as bound

      return(list(corrected = reference_year, code = 4L))
    }
  }

  # Unfixable - ancient dates or ambiguous
  return(list(corrected = NA_integer_, code = 9L))
}

#' Correct a date value
#' @param date_val Date value
#' @param reference_year Integer year for bounding
#' @return List with corrected_date and correction_code
correct_date <- function(date_val, reference_year = MAX_VALID_YEAR) {
  if (is.na(date_val)) {
    return(list(corrected = as.Date(NA), code = 0L))
  }

  yr <- year(date_val)
  result <- detect_year_typo(yr, reference_year)

  if (result$code == 0L) {
    return(list(corrected = date_val, code = 0L))
  }

  if (is.na(result$corrected)) {
    return(list(corrected = as.Date(NA), code = result$code))
  }

  # Reconstruct date with corrected year
  corrected_date <- tryCatch({
    as.Date(paste0(result$corrected, "-", month(date_val), "-", mday(date_val)))
  }, error = function(e) {
    as.Date(NA)
  })

  if (is.na(corrected_date)) {
    return(list(corrected = as.Date(NA), code = 9L))
  }

  return(list(corrected = corrected_date, code = result$code))
}

#' Apply date corrections to a data.table (post-2001 format)
#' @param dt data.table with Settlement_date, Contract_date, Download_datetime
#' @return data.table with corrected dates and Date_correction_code column
correct_dates_post2001 <- function(dt) {
  dt <- copy(dt)

  # Initialize correction code (will track highest severity correction)
  dt[, Date_correction_code := 0L]

  # Get reference year from Download_datetime
  dt[, ref_year := fifelse(
    is.na(Download_datetime),
    MAX_VALID_YEAR,
    as.integer(year(Download_datetime))
  )]

  # Correct Settlement_date
  if ("Settlement_date" %in% names(dt)) {
    dt[, c("Settlement_date_new", "settle_code") := {
      res <- mapply(correct_date, Settlement_date, ref_year, SIMPLIFY = FALSE)
      list(
        as.Date(sapply(res, `[[`, "corrected"), origin = "1970-01-01"),
        sapply(res, `[[`, "code")
      )
    }]
    dt[settle_code > 0L, Settlement_date := Settlement_date_new]
    dt[, Date_correction_code := pmax(Date_correction_code, settle_code)]
    dt[, c("Settlement_date_new", "settle_code") := NULL]
  }

  # Correct Contract_date
  if ("Contract_date" %in% names(dt)) {
    dt[, c("Contract_date_new", "contract_code") := {
      res <- mapply(correct_date, Contract_date, ref_year, SIMPLIFY = FALSE)
      list(
        as.Date(sapply(res, `[[`, "corrected"), origin = "1970-01-01"),
        sapply(res, `[[`, "code")
      )
    }]
    dt[contract_code > 0L, Contract_date := Contract_date_new]
    dt[, Date_correction_code := pmax(Date_correction_code, contract_code)]
    dt[, c("Contract_date_new", "contract_code") := NULL]
  }

  # Check Contract_date <= Settlement_date constraint
  if (all(c("Contract_date", "Settlement_date") %in% names(dt))) {
    # If Contract > Settlement and both are valid, swap them (code 5)
    swap_rows <- which(
      !is.na(dt$Contract_date) &
      !is.na(dt$Settlement_date) &
      dt$Contract_date > dt$Settlement_date &
      year(dt$Contract_date) >= MIN_VALID_YEAR &
      year(dt$Settlement_date) >= MIN_VALID_YEAR
    )
    if (length(swap_rows) > 0) {
      dt[swap_rows, c("Contract_date", "Settlement_date") :=
           .(Settlement_date, Contract_date)]
      dt[swap_rows, Date_correction_code := pmax(Date_correction_code, 5L)]
    }
  }

  # Check Settlement_date <= Download_datetime constraint
  if (all(c("Settlement_date", "Download_datetime") %in% names(dt))) {
    # If Settlement > Download (impossible), cap at Download
    exceed_rows <- which(
      !is.na(dt$Settlement_date) &
      !is.na(dt$Download_datetime) &
      dt$Settlement_date > as.Date(dt$Download_datetime)
    )
    if (length(exceed_rows) > 0) {
      dt[exceed_rows, Settlement_date := as.Date(Download_datetime)]
      dt[exceed_rows, Date_correction_code := pmax(Date_correction_code, 4L)]
    }
  }

  dt[, ref_year := NULL]

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

  # For pre-2001, use file_year as reference
  reference_year <- as.integer(file_year)

  # Correct Contract_date
  if ("Contract_date" %in% names(dt)) {
    dt[, c("Contract_date_new", "contract_code") := {
      res <- mapply(correct_date, Contract_date,
                    MoreArgs = list(reference_year = reference_year),
                    SIMPLIFY = FALSE)
      list(
        as.Date(sapply(res, `[[`, "corrected"), origin = "1970-01-01"),
        sapply(res, `[[`, "code")
      )
    }]
    dt[contract_code > 0L, Contract_date := Contract_date_new]
    dt[, Date_correction_code := contract_code]
    dt[, c("Contract_date_new", "contract_code") := NULL]
  }

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
