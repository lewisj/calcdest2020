postGisQuery <- function(x) {
  # library imports required for the parallel command (possibly can be tidied up)
  library(RPostgreSQL)
  
  # "Tried using pool to handle connection management/performance (see https://shiny.rstudio.com/articles/pool-basics.html)"
  conn <- dbConnect(drv=dbDriver("PostgreSQL"),
                    dbname="ais",
                    user="postgres",
                    password="password",
                    host="192.168.224.245")
  
  # close db connection after function call exits
  on.exit(dbDisconnect(conn))
  
  id <- x[7]
  mmsi <- as.character(x[1])
  exitTime <- as.POSIXct(x[4], tz='UTC')
  entryTime <- as.POSIXct(x[5], tz='UTC')
  
  query <- paste0("SELECT mmsi, acquisition_time, lon, lat
                  FROM pos WHERE mmsi = '", mmsi,
                  "' AND acquisition_time >= '", exitTime,
                  "' AND acquisition_time <= '", entryTime, "';")
  
  returnedAisDf <- dplyr::arrange(dbGetQuery(conn, query), acquisition_time)
  
  returnedAisDf$id <- id
  returnedAisDf
}


removeDodgyAis <- function(returnedAisDf) {
  filter(returnedAisDf, lat<=90)
}


getAisWithSensibelDistanceBetweenPings <- function(returnedAisDf) {
  # Return emtpy dataframe if route is "gappy" (i.e. big distance bettween AIS ping)
  
  lats <- returnedAisDf$lat; lons <- returnedAisDf$lon
  numPings <- length(lats)
  start_lats <- lats[1:(numPings-1)]; start_lons <- lons[1:(numPings-1)]
  end_lats <- lats[2:numPings]; end_lons <- lons[2:numPings]
  
  distances <- distVincentySphere(cbind(start_lons, start_lats), cbind(end_lons, end_lats))
  
  max_distance <- 400000# m (according to https://cran.r-project.org/web/packages/geosphere/geosphere.pdf)
  isNotGappy <- all(distances <= max_distance)
  if (isNotGappy == TRUE) returnedAisDf else NULL
}

queryVGD <- function(inputDf,shiptype,grossTons,dateRange){
  
  if(!nrow(inputDf)) return(NULL)
  
  inputDf %>%
  { if(shiptype == "All Vessel Types") . else filter(.,!is.na(ihsShipTypel2)&ihsShipTypel2==shiptype) } %>%
    filter(tons>=grossTons[1]&tons<=grossTons[2],
           exitTimeStartCell>=dateRange[1],
           entryTimeEndCell<=dateRange[2]) %>%
  { if(nrow(.)) . else NULL }
  
  
}

getTopCells <- function(inputDf, nTopCells, encNames) {
  
  if(is.null(inputDf)) return(NULL)
  
  inputDf %>% 
    group_by(cell=startCell) %>% 
    summarise(lat = first(startLat),
              lon = first(startLon),
              n = n(),
              n_mmsi = n_distinct(mmsi)) %>%
    ungroup() %>%
    arrange(desc(n_mmsi)) %>%
    slice(1:nTopCells) %>%
    left_join(encNames, by='cell')

}

