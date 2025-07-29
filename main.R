# Load libraries
# NetCDFs
library(ncdf4)

# Data wrangling
library(purrr)
library(readxl)
library(tidyverse)
select <- dplyr::select
library(reshape2)
library(glue)
library(lubridate)

# Set data path to Climate_Compensation_Data_Folder directory
DPATH <- case_when(
  Sys.info()["user"] == "anorawu" ~
    "/Users/anorawu/Team MG Dropbox/Wanru Wu/Climate_Compensation_Data_Folder"
)

target_rcp <- "rcp45" # or rcp85 depend on which scenario we wnat

# Calculate the pop and temp data
for (target_year in 2010:2100) {

  # Load population data for each year each region under SSP2, IIASA GDP (low)
  # specifications that matches the bilateral damage table
  ncpath <- glue("{DPATH}/CIL/CIL_Socioecon.nc")

  ncin <- nc_open(ncpath)
  ssp <- ncvar_get(ncin, "ssp")
  region <- ncvar_get(ncin, "region")
  model <- ncvar_get(ncin, "model")
  year <- ncvar_get(ncin, "year")
  pop <- ncvar_get(ncin, "pop")
  nc_close(ncin)

  pop_proj <- expand_grid(ssp, region, model, year) %>%
    filter(ssp == "SSP2" & model == "IIASA GDP" & year == target_year) %>%
    rename_with(~ str_c(.x, "_col")) %>%
    mutate(
      ssp_idx = map_int(ssp_col, ~ which(.x == ssp)),
      region_idx = map_int(region_col, ~ which(.x == region)),
      model_idx = map_int(model_col, ~ which(.x == model)),
      year_idx = map_int(year_col, ~ which(.x == year)),
    ) %>%
    rowwise() %>%
    mutate(
      pop = pop[year_idx, model_idx, region_idx, ssp_idx]) %>%
    ungroup() %>%
    select(region_col, year_col, ssp_col, model_col, pop) %>%
    rename(region = region_col, year = year_col, ssp = ssp_col, model = model_col)


  # Load daily temperature data for each region under CCSM4 as in Climate_cash_transfer
  ncpath <- glue("{DPATH}/CIL/CCSM4_daily_temps/{target_rcp}/{target_year}/1.6.nc4")

  ncin <- nc_open(ncpath)
  day <- ncvar_get(ncin, "time")
  region <- ncvar_get(ncin, "hierid")
  temp <- ncvar_get(ncin, "tas") # matrix in shape of [region, day]
  nc_close(ncin)

  # extract date from day variable
  year_doy <- tibble(
    day_idx = seq_along(day),
    year = as.integer(substr(as.character(day), 1, 4)),
    doy  = as.integer(substr(as.character(day), 5, 7))
  ) %>%
    mutate(
      date = as.Date(doy - 1, origin = paste0(year, "-01-01")),
      month = month(date)
    )

  hot_mask <- temp >= 33 #logical matrix of shape [region, day]
  month_vec <- year_doy$month
  # create an empty matrix of shape [region, month]
  hot_day_counts <- matrix(0, nrow = length(region), ncol = 12)

  for (m in 1:12) {
    day_indices <- which(month_vec == m) # day cols we want
    hot_day_counts[, m] <- rowSums(hot_mask[, day_indices, drop = FALSE])
  }

  # assign month column names
  colnames(hot_day_counts) <- as.character(1:12)

  hot_days_by_month <- as_tibble(hot_day_counts) %>%
    mutate(region = region) %>%
    mutate(rcp = target_rcp) %>%
    mutate(climate_model = "CCSM4")

  output <- hot_days_by_month %>%
    left_join(pop_proj, by="region") %>%
    # turn 1d array into character
    mutate(
      region = as.vector(region),
      year = as.vector(year),
      ssp = as.vector(ssp),
      model = as.vector(model)
    )

  exp_path <- glue("{DPATH}/output/temp_cut_pop_{target_year}_{target_rcp}.csv")
  write_csv(output, exp_path)

}


