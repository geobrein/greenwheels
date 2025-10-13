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

# Voeg runtime-datum/tijdkolommen toe (Europe/Amsterdam) aan de pipeline
tz_now <- as.POSIXct(Sys.time(), tz = "Europe/Amsterdam")

# Flatten -> pak 1e (enige) auto per locatie -> unnest naar vlakke kolommen
locs <- jsonlite::fromJSON(jsonlite::toJSON(out$data$locations, auto_unbox = TRUE), flatten = TRUE) %>%
  dplyr::mutate(
    car = purrr::map(cars, function(x) {
      if (is.null(x) || length(x) == 0) return(NULL)
      if (is.data.frame(x)) as.list(x[1, , drop = FALSE]) else x[[1]]
    })
  ) %>%
  dplyr::select(-cars) %>%
  tidyr::unnest_wider(car, names_sep = ".") %>%
  mutate(
    across(c(isRailway, isDiscount, isSpecialAccess, car.isInUse, car.hasKey, car.availability.available), as.logical),
    across(c(geoPoint.lat, geoPoint.lng), as.numeric),
    date    = format(tz_now, "%Y-%m-%d"),
    hour    = as.integer(format(tz_now, "%H")),
    minute  = as.integer(format(tz_now, "%M")),
    weekday = format(tz_now, "%A")
  )

# Schrijf naar CSV met timestamp in bestandsnaam
datetime <- format(Sys.time(), "%Y%m%d%H%M")
dir.create("data", showWarnings = FALSE)
outfile <- file.path("data", paste0("locations_", datetime, ".csv"))
write.csv(locs, outfile, row.names = FALSE, fileEncoding = "UTF-8")

cat("Wrote:", outfile, "\n")
