dataDir <- 'destPortData'

fp <- file.path

eezLandUnion <- rgdal::readOGR(fp(dataDir, "EEZ_land_union_v2_201410/EEZ_land_v2_201410.shp"))

pnts <- readr::read_csv(fp(dataDir,"avcs_centroid.csv"),col_names = F)

ihs <- readr::read_csv(fp(dataDir,"2017_ihs_jun.csv"),col_names = F) %>%
  rename(mmsi=X1,ihsShipTypel5=X2,ihsShipTypel2=X3,UKHOShipType=X4,tons=X5) %>%
  filter(!is.na(mmsi))%>% 
  group_by(mmsi) %>%
  mutate(n = 1:n()) %>%
  ungroup() %>%
  filter(n == 1) %>%
  select(-n)

cellLocs <- gsub('POINT\\(','',pnts$X2) %>%
  gsub('\\)','',.) %>%
  strsplit(' ') %>%
  do.call('rbind',.) %>% 
  as.data.frame(stringsAsFactors=F) %>%
  bind_cols(pnts,.) %>% 
  mutate(V1=as.numeric(V1),V2=as.numeric(V2))%>%
  select(-X2)

cellLocsSpDf <- SpatialPointsDataFrame( cbind(cellLocs$V1, cellLocs$V2),
                                        cellLocs,
                                        proj4string = CRS("+proj=longlat +datum=WGS84 +no_defs"))

cellLocs$eezCountry <- as.character(over(cellLocsSpDf, eezLandUnion)$Country)

enc_names <- readr::read_csv(fp(dataDir,'avcs_cat_full.csv'),col_names = F) %>%
  select(cell=X1,name=X2)




# # t <- readr::read_csv("S:/Programmes/MIMS/Programme Activities - Open/Future_Capabilities_Team/Projects/Routing/data/top_dest_output_with_header.csv")
# # write_feather(t,"S:/Programmes/MIMS/Programme Activities - Open/Future_Capabilities_Team/Projects/Routing/data/top_dest_output_with_header.feather")
# t <- read_feather("S:/Programmes/MIMS/Programme Activities - Open/Future_Capabilities_Team/Projects/Routing/data/top_dest_output_with_header.feather")
# 
# # mmsi counts by destinations and month
# t_count <- t %>% 
#   mutate(month = month(entryTimeEndCell)) %>%
#   group_by(endCell,month) %>%
#   summarise(n = n(),
#             n_mmsi = n_distinct(mmsi)) %>%
#   arrange(desc(n))
# 
# # number of connections
# ts <- t %>% 
#   group_by(startCell,endCell) %>%
#   summarise(n=n()) %>%
#   arrange(desc(n))
# 
# # complete grid
# grid <- {unique(c(ts$startCell,ts$endCell))} %>%
#   expand.grid(startCell=.,endCell=.,stringsAsFactors = F) %>%
#   left_join(.,ts) %>%
#   replace_na(list(n=0)) %>% 
#   spread(endCell,n) %>%
#   data.frame
# 
# str(grid)
# row.names(grid) <- grid$startCell
# grid$startCell <- NULL
# 
# tsm <- as.matrix(grid)

# tjoin <- t %>%
#   left_join(cellLocs,by=c('startCell'='X1')) %>%
#   rename(startLon=V1,startLat=V2) %>%
#   left_join(cellLocs,by=c('endCell'='X1')) %>%
#   rename(endLon=V1,endLat=V2) %>%
#   left_join(ihs,by='mmsi')



# customer MMSI ---------------


customMmsi <- readr::read_csv(fp(dataDir,'ukho_customer_mmsi.csv'),col_names = F)



# SD ----------------------------------------------------------------------

polys <- readr::read_csv(fp(dataDir,"avcs_cat.csv"),col_names = F)

sp.polys <- lapply(polys$X2,rgeos::readWKT)



# Full data -----------------------------


if (dataMode == 'test') {
  df <- read_feather(fp(dataDir,'destinations201601_2weeks.feather'))
  # df_jan2weeks <- filter(df, month(X4)==1 & day(X4)<15)
  # write_feather(df_jan2weeks,fp(dataDir,'destinations201601_2weeks.feather'))
} else if (dataMode == 'app') {
  # write_feather(df,'destinations2016.feather')
  df <- read_feather(fp(dataDir,'destinations2016.feather'))
}
names(df) <- c('mmsi','startCell','endCell','exitTimeStartCell','entryTimeEndCell')

tjoin <- df %>%
  left_join(cellLocs,by=c('startCell'='X1')) %>%
  rename(startLon=V1,startLat=V2, startEezCountry=eezCountry) %>%
  left_join(cellLocs,by=c('endCell'='X1')) %>%
  rename(endLon=V1,endLat=V2, endEezCountry=eezCountry) %>%
  left_join(ihs,by='mmsi') %>% 
  group_by(mmsi) %>%
  arrange(exitTimeStartCell) %>%
  mutate(durationEndCell = difftime(lead(exitTimeStartCell),entryTimeEndCell,units = 'hours')) %>%
  ungroup()

rm(df)
# summary(tjoin$tons)


# tjoin <- tjoin %>%
#   # filter(ihsShipTypel2=='Tankers',tons>3000) %>% #,mmsi%in%mmsiList) %>%
#   # group_by(startCell,endCell) %>%
#   mutate(duration=difftime(entryTimeEndCell,exitTimeStartCell,units = 'days'),
#          duration= as.numeric(duration))
#   # ungroup


