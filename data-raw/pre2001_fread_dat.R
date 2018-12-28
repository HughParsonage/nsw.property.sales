
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

    ZoningDecoder <- fread("data-raw/decoders/ZoningPre2011.tsv")
    dat <- ZoningDecoder[dat, on = "Zone_Cd_pre2011"]
    drop_cols(dat, c("Zone_Cd_2011", "Zone_Cd_pre2011"))
  }
  # if (runif(1) < 0.01) cat(".")

  dat
}
