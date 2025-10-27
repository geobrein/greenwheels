# --- script.R ---
# Draait elk uur op :35 via GitHub Actions en schrijft incrementeel naar ./data/deventer_incremental.csv

library(httr)
library(jsonlite)
library(dplyr)
library(purrr)
library(tidyr)
library(tibble)

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

# Eerst locs_raw robuust maken ipv direct in een pipe mutaten.
locs_raw <- jsonlite::fromJSON(
  jsonlite::toJSON(out$data$locations, auto_unbox = TRUE),
  flatten = TRUE
)

#  Zorgt dat locs_raw altijd een data.frame/tibble is, ook als fromJSON een list teruggeeft
if (!is.data.frame(locs_raw)) {
  locs_raw <- purrr::map(locs_raw, ~as.list(.x)) %>% dplyr::bind_rows()
}
locs_raw <- tibble::as_tibble(locs_raw)

# Kolom 'car': nooit NULL teruggeven
locs_step1 <- locs_raw %>%
  mutate(
    car = purrr::map(cars, function(x) {
      # [CHANGE] Voorheen: return(NULL) bij geen auto's. Dat brak unnest_wider/mutate downstream.
      # Nieuw: altijd een tibble met dezelfde kolommen gevuld met NA.
      if (is.null(x) || length(x) == 0) {
        tibble(
          availability.available = NA,
          id = NA,
          type = NA,
          license = NA,
          model = NA,
          fuelType = NA,
          hasKey = NA,
          class = NA,
          isInUse = NA
        )
      } else if (is.data.frame(x)) {
        tibble::as_tibble(x[1, , drop = FALSE])
      } else {
        tibble::as_tibble(x[[1]])
      }
    })
  ) %>%
  select(-cars)

# Unnest de 'car' kolom, en forceer opnieuw tibble zodat mutate daarna niet crasht
locs_step2 <- locs_step1 %>%
  tidyr::unnest_wider(car, names_sep = ".") %>%
  tibble::as_tibble()

locs <- locs_step2 %>%
  mutate(
    across(
      c(isRailway, isDiscount, isSpecialAccess,
        car.isInUse, car.hasKey, car.availability.available),
      as.logical
    ),
    across(c(geoPoint.lat, geoPoint.lng), as.numeric),
    date    = format(tz_now, "%Y-%m-%d"),
    hour    = as.integer(format(tz_now, "%H")),
    minute  = as.integer(format(tz_now, "%M")),
    weekday = format(tz_now, "%A"),
    run_ts  = format(tz_now, "%Y-%m-%d %H:%M:%S %Z")  # [NEW] handig om exacte run later terug te vinden
  )

# Filter direct op Deventer
deventer <- locs %>%
  filter(city.name == "Deventer")

# Als er geen auto's/locaties in Deventer zijn
if (nrow(deventer) == 0) {
  cat("No rows for Deventer at this run. Nothing to append.\n")
  quit(save = "no", status = 0)
}

dir.create("data", showWarnings = FALSE)

inc_path <- "data/deventer_incremental.csv"

# Meteen naar incremental schrijven met alleen Deventer
if (file.exists(inc_path)) {
  old <- read.csv(
    inc_path,
    stringsAsFactors = FALSE,
    check.names = FALSE  # [NEW] zorgt dat kolomnamen zoals "car.id" niet worden verbasterd
  )

  # Kolommen alignen tussen oud en nieuw zodat bind_rows nooit faalt
  all_names <- union(names(old), names(deventer))

  old[setdiff(all_names, names(old))] <- NA
  deventer[setdiff(all_names, names(deventer))] <- NA

  old <- old[all_names]
  deventer <- deventer[all_names]

  all <- dplyr::bind_rows(old, deventer)

  # Dedup op auto + timestampcomponenten
  key <- c("car.id","date","hour","minute")
  if (all(key %in% names(all))) {
    all <- dplyr::distinct(all, dplyr::across(all_of(key)), .keep_all = TRUE)
  }
} else {
  all <- deventer
}

write.csv(all, inc_path, row.names = FALSE, fileEncoding = "UTF-8")
cat("Updated incremental:", inc_path, "rows:", nrow(all), "\n")
