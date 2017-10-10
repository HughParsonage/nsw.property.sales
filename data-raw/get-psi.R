library(magrittr)
library(data.table)
library(lubridate)
library(hutils)

provide.dir("./data-raw/Weekly-PSI")

# http://www.valuergeneral.nsw.gov.au/__psi/weekly/20170821.zip

final_week <- as.Date("2017-10-09")

get_psi <- function(week) {
  yr <- as.character(year(week))
  wk <- as.character(week)

  ex_dir <- file.path("data-raw", "Weekly-PSI", yr)
  provide.dir(ex_dir)
  dest_file <- file.path("data-raw", "Weekly-PSI", yr, paste0(wk, ".zip"))
  if (!file.exists(dest_file)) {
    remote.zip <- paste0("http://www.valuergeneral.nsw.gov.au/__psi/weekly/", gsub("-", "", wk), ".zip")
    tryCatch(download.file(url = remote.zip, destfile = dest_file, quiet = TRUE),
             error = function(e) cat(e$message))
    unzip(dest_file, exdir = ex_dir)
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
    tryCatch(download.file(url = remote.zip, destfile = dest_file, quiet = TRUE),
             error = function(e) stop(e$message))
    # unzip(dest_file, exdir = ex_dir)
  }
  setwd(ex_dir)
  if (yr %between% c(2001, 2016)) {
    current_wd2 <- getwd()
    for (the_dir in list.dirs(recursive = TRUE, full.names = TRUE)) {
      setwd(the_dir)
      on.exit(setwd(current_wd2))
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

post2001fread_dat <- function(filename, v1 = "B") {
  dat <-
    tryCatch(fread(file = filename, sep = ";", fill = TRUE, header = FALSE,
                   na.strings = c("NA", ""),
                   quote = ""),
             error = function(e) {
               cat(filename)
               stop(e)
             })

  DIR_YEAR <- sub("^.*/(2[0-9]{3})/?.*$", "\\1", filename, perl = TRUE)

  if (v1 == "B") {
    setnames(dat, 1, "Record_type")
    setnames(dat, 2, "District_code")
    setnames(dat, 3, "Property_id")
    setnames(dat, 4, "Sale_counter")
    setnames(dat, 5, "Download_datetime")
    dat[, Download_datetime := ymd_hm(Download_datetime)]

    setnames(dat, 6, "Property_name")
    setnames(dat, 7, "Unit_no")
    setnames(dat, 8, "House_no")
    setnames(dat, 9, "Street")
    setnames(dat, 10, "Locality")
    setnames(dat, 11, "Postcode")
    setnames(dat, 12, "Area")
    setnames(dat, 13, "Area_units")

    setnames(dat, 14, "Contract_date")
    dat[, Contract_date := ymd(Contract_date)]

    setnames(dat, 15, "Settlement_date")
    dat[, Settlement_date := ymd(Settlement_date)]

    setnames(dat, 16, "Purchase_price")
    if (DIR_YEAR >= "2011") {
      # http://www.valuergeneral.nsw.gov.au/__data/assets/pdf_file/0019/216406/Property_Sales_Data_File_Zone_Codes_and_Descriptions_V2.pdf
      setnames(dat, 17, "Zone_Cd_2011")
    } else {
      setnames(dat, 17, "Zone_Cd_pre2011")
    }
    setnames(dat, 18, "Nature_of_property")
    if (!is.character(dat[[18L]])) {
      set(dat, j = 18L, value = as.character(dat[[18L]]))
    }
    dat[Nature_of_property == "V", Nature_of_property := "Vacant"]
    dat[Nature_of_property == "R", Nature_of_property := "Residence"]
    dat[Nature_of_property == "3", Nature_of_property := "Other"]

    setnames(dat, 19, "Primary_purpose")
    setnames(dat, 20, "Strata_lot_no")
    setnames(dat, 21, "Component_code")
    setnames(dat, 22, "Sale_code")
    setnames(dat, 23, "Percent_interest_of_sale")
    if (is.logical(dat[["Percent_interest_of_sale"]])) {
      dat[, Percent_interest_of_sale := as.integer(Percent_interest_of_sale)]
      dat[, Percent_interest_of_sale := as.integer(100L)]
    } else {
      dat[is.na(Percent_interest_of_sale), Percent_interest_of_sale := 100L]
    }

    setnames(dat, 24, "Dealing_no")
  }

  if (v1 == "C") {
    setnames(dat, 1, "Record_type")
    setnames(dat, 2, "District_code")
    setnames(dat, 3, "Property_id")
    setnames(dat, 4, "Sale_counter")
    setnames(dat, 5, "Download_datetime")
    dat[, Download_datetime := ymd_hm(Download_datetime)]
    dat[, Property_Legal_Description := paste0(.SD), .SDcols = names(dat)[-c(1:5)]]
  }
  dat
}


pre2001_fread_dat <- function(filename, v1 = "B") {
  dat <-
    tryCatch({
      fread(filename,
            sep = ";",
            fill = TRUE,
            header = FALSE,
            skip = 1L,
            na.strings = "") %>%
        .[V1 == v1]
    },
    error = function(e) {
      tryCatch({
        fread(filename,
              sep = ";",
              fill = TRUE,
              header = FALSE,
              skip = 1L,
              # 1999 needs this:
              quote = "",
              na.strings = "") %>%
          .[V1 == v1]
      },
      error = function(e) {
        tryCatch({
          setDT(read.csv2(filename,
                          header = FALSE,
                          fill = TRUE,
                          na.strings = c(""))) %>%
            .[V1 == v1]

        },
        error = function(e) {
          cat(filename)
          return(data.table:::null.data.table())
        })
      })
    })
  # http://www.valuergeneral.nsw.gov.au/__data/assets/pdf_file/0014/216401/Archived_Property_Sales_Data_File_Format_1990_to_2001_V2.pdf
  if (v1 == "B") {

    setnames(dat, 1, "Record_type")
      setnames(dat, 2, "District_code")
      setnames(dat, 3, "Source") # Intrnl use only
      setnames(dat, 4, "Valuation_num")
      setnames(dat, 5, "Property_id")
      setnames(dat, 6, "Unit_no")
      setnames(dat, 7, "House_no")
      setnames(dat, 8, "Street")
      setnames(dat, 9, "Locality") # Suburb orig
      setnames(dat, 10, "Postcode")
      setnames(dat, 11, "Contract_date")
      setnames(dat, 12, "Purchase_price")
      setnames(dat, 13, "Land_description")
      setnames(dat, 14, "Area")
      setnames(dat, 15, "Area_units")
      setnames(dat, 16, "Dimensions")
      setnames(dat, 17, "Comp_code") # Intrnl use only
      setnames(dat, 18, "Zoning_Pre2011")
      setnames(dat, 19, "Vendor_name") # Blanked out
      setnames(dat, 20, "Purchaser_name") # Blanked out


      dat[,
          c("Source", "Comp_code",
            "Vendor_name", "Purchaser_name") := NULL]

  }

  drop_empty_cols(dat)

  if (AND(v1 == "B",
          # If Zone_Cd is blank, it has been dropped
          # and so the following (pointless) join
          # will error.
          any(c("Zone_Cd_pre2011",
                "Zone_Cd_2011") %chin% names(dat)))) {
    if (DIR_YEAR < "2011") {
      ZoningDecoder <- fread("data-raw/decoders/ZoningPre2011.tsv")
      dat <- ZoningDecoder[dat, on = "Zone_Cd_pre2011"]
    } else {
      ZoningDecoder <- fread("data-raw/decoders/ZoningPost2011.tsv")
      dat <- ZoningDecoder[dat, on = "Zone_Cd_2011"]
    }
    drop_cols(dat, c("Zone_Cd_2011", "Zone_Cd_pre2011"))
  }
  # if (runif(1) < 0.01) cat(".")

  dat
}

dat2017B <-
  list.files(path = "data-raw/Weekly-PSI/2017/",
             pattern = "\\.DAT$",
             full.names = TRUE) %>%
  lapply(fread_dat,
         v1 = "B") %>%
  rbindlist(use.names = TRUE, fill = TRUE) %>%
  drop_empty_cols %>%
  drop_col("Record_type")

post2001_Bfiles <-
  lapply(2001:2017,
         function(x) {
           list.files(path = file.path("data-raw/Weekly-PSI", x),
                      pattern = "ALL-B\\.txt$",
                      recursive = TRUE,
                      full.names = TRUE)
         }) %>%
  unlist

PropertySales20012017 <- list(length(post2001_Bfiles))
# Don't use lapply to make it easier to debug
for (ff in seq_along(post2001_Bfiles)) {
  cat(ff)
  PropertySales20012017[[ff]] <- post2001fread_dat(file = post2001_Bfiles[[ff]])
  cat("\n")
}


PropertySales20012017 <- rbindlist(PropertySales20012017, use.names = TRUE, fill = TRUE)

for (j in seq_along(PropertySales20012017)) {
  v <- PropertySales20012017[[j]]
  if (is.character(v)) {
    set(PropertySales20012017, j = j, value = if_else(v == "", NA_character_, v))
  }
}
drop_empty_cols(PropertySales20012017)
PropertySales20012017 <- rbindlist(PropertySales20012017, use.names = TRUE, fill = TRUE)

# Convert all areas to square-metres
PropertySales20012017 %>%
  .[, Area_sqm := Area] %>%
  .[Area_units == "H", Area_sqm := 10e3 * Area] %>%
  drop_cols(c("Area", "Area_units"))

devtools::use_data(PropertySales20012017)

