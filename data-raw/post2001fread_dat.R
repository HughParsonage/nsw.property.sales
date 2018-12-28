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
