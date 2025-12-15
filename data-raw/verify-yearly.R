#!/usr/bin/env Rscript --vanilla
#
# Verify all yearly PropertySales files

library(data.table)

cat("=== Yearly PropertySales Files Summary ===\n\n")

results <- list()

for (year in 1990:2024) {
  rda_file <- sprintf("data/PropertySales%d.rda", year)

  if (!file.exists(rda_file)) {
    cat(sprintf("%d: MISSING\n", year))
    next
  }

  load(rda_file)
  obj_name <- paste0("PropertySales", year)
  dat <- get(obj_name)

  n <- nrow(dat)
  cols <- ncol(dat)
  size_mb <- round(file.size(rda_file) / 1024^2, 1)

  # Get date column (Settlement_date for post-2001, Contract_date for pre-2001)
  if ("Settlement_date" %in% names(dat)) {
    date_col <- "Settlement_date"
    min_date <- min(dat$Settlement_date, na.rm = TRUE)
    max_date <- max(dat$Settlement_date, na.rm = TRUE)
  } else if ("Contract_date" %in% names(dat)) {
    date_col <- "Contract_date"
    min_date <- min(dat$Contract_date, na.rm = TRUE)
    max_date <- max(dat$Contract_date, na.rm = TRUE)
  } else {
    date_col <- NA
    min_date <- NA
    max_date <- NA
  }

  results[[as.character(year)]] <- data.table(
    Year = year,
    Records = n,
    Columns = cols,
    Size_MB = size_mb,
    Date_Column = date_col,
    Min_Date = as.character(min_date),
    Max_Date = as.character(max_date)
  )

  cat(sprintf("%d: %s records, %d cols, %.1f MB (%s to %s)\n",
              year, format(n, big.mark = ","), cols, size_mb,
              as.character(min_date), as.character(max_date)))

  rm(list = obj_name)
}

cat("\n=== Column Comparison ===\n")

# Compare pre-2001 vs post-2001 schemas
load("data/PropertySales1990.rda")
pre2001_cols <- names(PropertySales1990)
cat("\nPre-2001 columns (1990-2000):\n")
print(pre2001_cols)

load("data/PropertySales2001.rda")
post2001_cols <- names(PropertySales2001)
cat("\nPost-2001 columns (2001-2024):\n")
print(post2001_cols)

cat("\n=== Totals ===\n")
summary_dt <- rbindlist(results)
cat("Total records across all years:", format(sum(summary_dt$Records), big.mark = ","), "\n")
cat("Total size:", round(sum(summary_dt$Size_MB), 1), "MB\n")
