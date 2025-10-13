# --- script.R ---
# Draait elk uur op :35 via GitHub Actions en schrijft CSV naar ./data/

library(httr)
library(jsonlite)
library(dplyr)
library(purrr)
library(tidyr)

endpoint <- "https://www.greenwheels.com/api/graphql"

q <- '
query locations {
  locations {
    id
    address
    locationType
    geoPoint { lat lng }
    city { name }
    isRailway
    isDiscount
    isSpecialAccess
    cars {
      availability { available }
      id
      type
      license
      model
      fuelType
      hasKey
      class
      isInUse
    }
  }
}
'

res <- httr::POST(
  url = endpoint,
  httr::add_headers(
    `Content-Type` = "application/json",
    `apollographql-client-name` = "web",
    `apollographql-client-version` = "v5.27.1"
  ),
  body = list(operationName = "locations", query = q, variables = list()),
  encode = "json"
)

httr::stop_for_status(res)
out <- httr::content(res, as = "parsed", type = "application/json")

# Eén consistente timestamp in Europa/Amsterdam
tz <- "Europe/Amsterdam"
Sys.setenv(TZ = tz)
tz_now <- as.POSIXct(Sys.time(), tz = "Europe/Amsterdam")

locs <- jsonlite::fromJSON(jsonlite::toJSON(out$data$locations, auto_unbox = TRUE), flatten = TRUE) %>%
  mutate(
    car = purrr::map(cars, function(x) {
      if (is.null(x) || length(x) == 0) return(NULL)
      if (is.data.frame(x)) as.list(x[1, , drop = FALSE]) else x[[1]]
    })
  ) %>%
  select(-cars) %>%
  tidyr::unnest_wider(car, names_sep = ".") %>%
  mutate(
    across(c(isRailway, isDiscount, isSpecialAccess, car.isInUse, car.hasKey, car.availability.available), as.logical),
    across(c(geoPoint.lat, geoPoint.lng), as.numeric),
    date    = format(tz_now, "%Y-%m-%d"),
    hour    = as.integer(format(tz_now, "%H")),
    minute  = as.integer(format(tz_now, "%M")),
    weekday = format(tz_now, "%A")
  )

# 1) per-run snapshot
dir.create("data", showWarnings = FALSE)
datetime <- format(tz_now, "%Y%m%d%H%M")  # gebruik dezelfde timestamp
outfile <- file.path("data", paste0("locations_", datetime, ".csv"))
write.csv(locs, outfile, row.names = FALSE, fileEncoding = "UTF-8")
cat("Wrote snapshot:", outfile, "\n")

# 2) Deventer incremental
inc_path <- "data/deventer_incremental.csv"

deventer <- locs %>%
  filter(city.name == "Deventer") %>%
  mutate(run_ts = format(tz_now, "%Y-%m-%d %H:%M:%S %Z"))

if (file.exists(inc_path)) {
  old <- read.csv(inc_path, stringsAsFactors = FALSE, check.names = FALSE)
  all_names <- union(names(old), names(deventer))
  old[setdiff(all_names, names(old))] <- NA
  deventer[setdiff(all_names, names(deventer))] <- NA
  old <- old[all_names]; deventer <- deventer[all_names]
  all <- dplyr::bind_rows(old, deventer)
  key <- c("car.id","date","hour","minute")
  if (all(key %in% names(all))) {
    all <- dplyr::distinct(all, dplyr::across(all_of(key)), .keep_all = TRUE)
  }
} else {
  all <- deventer
}

write.csv(all, inc_path, row.names = FALSE, fileEncoding = "UTF-8")
cat("Updated incremental:", inc_path, "rows:", nrow(all), "\n")

