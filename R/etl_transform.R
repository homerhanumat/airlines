globalVariables(".")

#' @rdname etl_load.etl_airlines
#' @inheritParams etl_transform.etl_airlines
#' @export

etl_transform.etl_airlines <- function(obj, years = 2015, months = 1:12, ...) {
  must_unzip <- match_files_by_year_months(list.files(attr(obj, "raw_dir")), 
                                           pattern = "On_Time_On_Time_Performance_%Y_%m.zip", years, months)
  
  unzipped <- match_files_by_year_months(list.files(attr(obj, "load_dir")), 
                                         pattern = "flights_%Y_%m.csv", years, months)
#  cat(unzipped)
  missing <- !gsub("On_Time_On_Time_Performance", "flights", must_unzip) %in% 
    gsub("\\.csv", "\\.zip", unzipped)
  tounzip <- must_unzip[missing]
  
  if (length(tounzip) > 0) {
    lapply(paste0(attr(obj, "raw_dir"), "/", tounzip), clean_flights)
  }
  invisible(obj)
}

#' @importFrom readr read_csv
#' @importFrom lubridate make_datetime

clean_flights <- function(path_zip) {
  # rename the CSV to match the ZIP
  load_dir <- gsub("/raw", "/load", dirname(path_zip))
  path_csv <- basename(path_zip) %>%
    gsub("On_Time_On_Time_Performance", "flights", x = .) %>%
    paste0(load_dir, "/", x = .) %>%
    gsub("\\.zip", "\\.csv", x = .)
  # col_types <- readr::cols(
  #   DepTime = col_integer(),
  #   ArrTime = col_integer(),
  #   CRSDepTime = col_integer(),
  #   CRSArrTime = col_integer(),
  #   Carrier = col_character()
  # )
  # can't get col_types argument to work!
  # readr::read_csv(path_zip, col_types = col_types) %>%
  
  # Move away from deprecated SE versions of data verbs.
  # Also, write_csv writes out 1000 (and presumably also x000)
  # in scientific notation.  Hence (for example) a 10am scheduled 
  # departure time will be written as 1e3, which cannot be interpreted as
  # smallint by PostgreSQL.  Hence we apply format() to any numerical
  # variables that could take values x000.
  # Seems to work with MySQL, too.
  readr::read_csv(path_zip) %>%
    mutate(year = format(Year, scientific = FALSE),
           dep_time = format(DepTime, scientific = FALSE),
           dep_delay = format(DepDelay, scientific = FALSE),
           sched_dep_time = format(CRSDepTime, scientific = FALSE),
           arr_time = format(ArrTime, scientific = FALSE),
           arr_delay = format(ArrDelay, scientific = FALSE),
           sched_arr_time = format(CRSArrTime, scientific = FALSE)) %>%
    mutate(dep_time = ifelse(grepl("NA", dep_time), NA, dep_time),
           dep_delay = ifelse(grepl("NA", dep_delay), NA, dep_delay),
           sched_dep_time = ifelse(grepl("NA", sched_dep_time), NA, sched_dep_time),
           arr_time = ifelse(grepl("NA", arr_time), NA, arr_time),
           sched_arr_time = ifelse(grepl("NA", sched_arr_time), NA, sched_arr_time),
           arr_delay = ifelse(grepl("NA", arr_delay), NA, arr_delay)) %>%
    select(
      year, month = Month, day = DayofMonth, 
      dep_time, sched_dep_time, dep_delay = dep_delay, 
      arr_time, sched_arr_time, arr_delay = arr_delay, 
      carrier = Carrier,  tailnum = TailNum, flight = FlightNum,
      origin = Origin, dest = Dest, air_time = AirTime, distance = Distance,
      cancelled = Cancelled, diverted = Diverted
    ) %>%
#    filter(origin %in% c("JFK", "LGA", "EWR")) %>%
    mutate(hour = as.numeric(sched_dep_time) %/% 100,
           minute = as.numeric(sched_dep_time) %% 100,
           time_hour = lubridate::make_datetime(as.numeric(year),
                                                month, day, hour, minute, 0)) %>%
#    mutate_(tailnum = ~ifelse(tailnum == "", NA, tailnum)) %>%
    arrange(year, month, day, dep_time) %>%
    readr::write_csv(path = path_csv, na = "")
    
}

## deprecated
# 
# unzip_month <- function(path_zip) {
#   files <- unzip(path_zip, list = TRUE)
#   # Only extract biggest file
#   csv <- files$Name[order(files$Length, decreasing = TRUE)[1]]
#   message(paste("Unzipping", csv))
#   load_dir <- gsub("/raw", "/load", dirname(path_zip))
#   unzip(path_zip, exdir = load_dir, overwrite = TRUE, junkpaths = TRUE, files = csv)
#   
#   # fix unprintable charater bug. See:
#   # https://github.com/beanumber/airlines/issues/11
#   # UPDATE: this doesn't seem to be an issue since readr uses UTF-8 by default
#   path_csv <- paste0(load_dir, "/", csv)
#   if (grepl("2001_3.csv", path_csv)) {
#     bad <- readLines(path_csv)
#     good <- gsub("[^[:print:]]", "", bad)
#     writeLines(good, path_csv)
#   }
#   
#   # rename the CSV to match the ZIP
#   path_csv_new <- gsub(".zip", ".csv", paste0(load_dir, "/", basename(path_zip)))
#   file.rename(path_csv, path_csv_new)
#   return(path_csv_new)
# }
