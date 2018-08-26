library(data.table)
library(magrittr)
library(tools)
library(hutils)



"http://www.valuergeneral.nsw.gov.au/__psi/yearly/2017.zip" %>%
  download.file(destfile = "data-raw/2017/2017.zip")

unzip(zipfile = "data-raw/2017/2017.zip",
      exdir = "data-raw/2017/2017-Yearly")

zip2017files <-
  dir(path = "data-raw/2017/2017-Yearly",
      full.names = TRUE,
      pattern = "\\.zip$")

stopifnot(length(zip2017files) == 52)

for (.zip in zip2017files) {
  out_dir <- tools::file_path_sans_ext(.zip)
  if (dir.exists(out_dir)) {
    stop(out_dir, "exists.")
  } else {
    provide.dir(out_dir)
  }
  unzip(.zip, exdir = out_dir)
}

root_dir <- getwd()
stopifnot("DESCRIPTION" %chin% dir(root_dir))
for (the_dir in list.dirs(path = "data-raw/2017/2017-Yearly", recursive = TRUE, full.names = TRUE)) {
  if (the_dir == "data-raw/2017/2017-Yearly") {
    next
  }
  setwd(file.path(root_dir, the_dir))
  if (AND(!file.exists("ALL-B.txt"),
          length(list.files(pattern = "\\.DAT$")) > 2)) {
    # Progress
    if (runif(1) < 0.1) cat(the_dir)
    the_week <- sub("^.*/(2017[0-9]{4})$", "\\1", the_dir)
    stopifnot(nchar(the_week) == 8)

    # Instead of lapply rbindlist, use
    # Windows cmd.exe (Much faster and less likely to crash)
    # Simply concatenates all files on top of each other
    # This is OK, because there are no headers, and
    # the files have a simple pattern (starts with a B for prices)
    shell("copy /y /b *.DAT ALL.DAT")
    readr::read_lines("ALL.DAT") %>%
      .[grepl("^B", .)] %>%
      readr::write_lines(file.path(root_dir, "data-raw", "2017", paste0(the_week, "-", "ALL-B.txt")))

    readr::read_lines("ALL.DAT") %>%
      .[grepl("^C", .)] %>%
      readr::write_lines(file.path(root_dir, "data-raw", "2017", paste0(the_week, "-", "ALL-C.txt")))
    file.remove("ALL.DAT")
  } else {
    stop(getwd())
  }
}

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
  if (length(list.files(recursive = TRUE)) == 0) {
    cat(ex_dir)
    stop("zip file ", ex_dir, ".zip must be unzipped manually")
  }

  if (yr %between% c(2001, 2017)) {
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
    if (is.character(dat[[14L]])) {
      if (any(startsWith(.subset2(dat, 14L), "217"))) {
        dat[startsWith(Contract_date, "217"),
            Contract_date := sub("^217", "2017", Contract_date)]
      }
    } else if (is.integer(dat[[14]])) {
      dat[Contract_date %/% 1e4L == 217L, Contract_date := Contract_date + 180e5L]
      dat[Contract_date == 2010805L, Contract_date := 20100805]
    }
    dat[, Contract_date := ymd(Contract_date)]
    min_contract_year <- dat[, min(year(Contract_date), na.rm = TRUE)]

    if (coalesce(min_contract_year, 1950L) < 1950L) {
      print(dat[year(Contract_date) < 1950])
      stop(filename, " years before 1950")
    }


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
      # Missing so assumed to be 100%
      dat[, Percent_interest_of_sale := as.integer(Percent_interest_of_sale)]
      dat[, Percent_interest_of_sale := as.integer(100L)]
    } else {
      dat[is.na(Percent_interest_of_sale), Percent_interest_of_sale := 100L]
    }

    setnames(dat, 24, "Dealing_no")

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


