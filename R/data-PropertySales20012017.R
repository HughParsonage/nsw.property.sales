#' @title Property Sales 2001-2017
#' @description All property sales recorded by the NSW Valuer-General since 2001.
#' Field names are provided 'as-is', except some dates that appeared to be
#' typos were corrected. Zone codes have been decoded, but \code{District_code}
#' has not (as the documentation indicated these may have changed over the years).
#' \code{Area} was originally in hectares or square metres, but has been converted to
#' \code{Area_sqm} (area in square metres) to avoid confusion.
#'
#' Some columns have been dropped, notably the 'Dealing no' which is now \code{DEALING_ID},
#' an integer that may change as this dataset is updated. Note that little is lost by using
#' \code{DEALING_ID} as opposed to \code{Dealing_no}, unless you know the \code{Dealing_no},
#' as it is simply a unique identifier of the dealing.

"PropertySales20012017"
