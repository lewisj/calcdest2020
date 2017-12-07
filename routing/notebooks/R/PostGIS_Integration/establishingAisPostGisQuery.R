# shortest time 
# consider limiting ais pings (don't need every second)
# most common?

# Connect to PostGIS
library("RPostgreSQL")
m = dbDriver("PostgreSQL")
con = dbConnect(m, dbname="ais",user="postgres",password="password",host="192.168.224.245")


selectedDf = read.csv("suez_malacca_mmsi_list.csv", header = FALSE, stringsAsFactors = FALSE)
colnames(selectedDf) = c("mmsi","exitTimeStartCell", "entryTimeEndCell")

postGisQuery <- function(x) {
  mmsi = x[1]
  exitTime = x[2]
  entryTime = x[3]
  
  q = paste0("SELECT mmsi, acquisition_time, lon, lat
           FROM pos WHERE mmsi = '",mmsi,
             "' AND acquisition_time >= '",exitTime,
             "' AND acquisition_time <= '",entryTime, "';")
  rs = dbSendQuery(con, q)
  fetch(rs,n=-1)
}



postGisQueryDf = do.call(rbind, apply(selectedDf, 1, FUN=postGisQuery))

apply(selectedDf[1:100,], 1, FUN=postGisQuery)
str(selectedDf)

plyr::ddply(selectedDf[1:100,],'mmsi',.fun = postGisQuery,.parallel = T)


write.csv(postGisQueryDf, "postGisQuery.csv", row.names = FALSE)


# REMOVE SLOW JOURNEYS
hist(as.numeric(difftime(selectedDf$entryTimeEndCell,selectedDf$exitTimeStartCell,'days')))

selectedDf_journeyTime = selectedDf
selectedDf_journeyTime$journeyTime = as.numeric(difftime(selectedDf$entryTimeEndCell,selectedDf$exitTimeStartCell,'days'))
