

base <- c(-79.527466,   8.883334)
linkeddf <- readRDS('linkdf.rds')




require('sf')


g = st_sfc(st_point(1:2))
st_sf(a=3,g)
st_sf(g, a=3)
st_sf(a=3, st_sfc(st_point(1:2))) # better to name it!
# create empty structure with preallocated empty geometries:
nrows <- 10
geometry = st_sfc(lapply(1:nrows, function(x) st_geometrycollection()))
df <- st_sf(id = 1:nrows, geometry = geometry)
g = st_sfc(st_point(1:2), st_point(3:4))
s = st_sf(a=3:4, g)
s[1,]
class(s[1,])
s[,1]
class(s[,1])
s[,2]
class(s[,2])
g = st_sf(a=2:3, g)
pol = st_sfc(st_polygon(list(cbind(c(0,3,3,0,0),c(0,0,3,3,0)))))
h = st_sf(r = 5, pol)
g[h,]
h[g,]



plyr::dlply(linkeddf,'cell',function(x){
  rbind(base,c(x$lon,x$lat)) %>%
  # rbind(c(-160,0),c(160,0)) %>%
    st_linestring %>%
    st_sfc(crs = 4326) %>%
    st_wrap_dateline(c("WRAPDATELINE=YES","DATELINEOFFSET=10")) %>%
    st_sf(n_mmsi=x$n_mmsi,name=x$name,cell=x$cell,.) 
}) %>% 
  do.call(rbind,.) %>%
  leaflet(.) %>% 
  addTiles() %>% 
  addPolylines(group = 'edges',
               weight = 5,
               opacity = ~getOpacity(n_mmsi),
               layer = ~paste0(cell,'_'),
               label = ~paste0(cell,': ',name,', n_mmsi=',n_mmsi))  


l2 <- rbind(c(-160,0),c(160,0)) %>%
  st_linestring %>%
  st_sfc(crs = 4326) %>%
  st_sf(id=3,.)


rbind(l1,l2)

  

st_sfc(st_linestring(rbind(c(-100.5,57.0),
                                 c(100.5,57.0))),crs=4326) %>% 
  st_wrap_dateline(c("WRAPDATELINE=YES", "DATELINEOFFSET=60")) %>%
leaflet() %>% 
  addTiles() %>% 
  addPolylines(weight = 4, opacity = 0.35, color = 'black') 



# final destination ---------------------

# filter by veessl type/size starting destination
# get mmsi list of outbouint traffic
# for each endCell/mmsi get onward destinations a so on
# if time in cell is less than x then ignore cell (transiting though)


id <- 'RU5MELN0'
cutoff <- 5

dataFlt <- tjoin %>%
  filter(ihsShipTypel2 == 'Tankers',
         tons > 2000)

dataFlt

mmsiList <- dataFlt %>%
  filter(startCell==id) %>%
  .$mmsi %>%
  unique()

# data.flt %>%
#   arrange(mmsi,exitTimeStartCell) %>%
#   View
# 
# uniqueCells <- function(v){
#   lapply(1:length(v),function(i) length(unique(v[1:i]))!=i) %>% 
#     unlist()
# }

finalDest <- data.flt %>%
  # filter by mmsi
  # select( mmsi, startCell,  endCell,   exitTimeStartCell,    entryTimeEndCell,durationEndCell) %>%
  filter(mmsi %in% mmsiList,
         startCell != endCell) %>%
  mutate(isSource=startCell==id) %>%
  # how long did vessel stay in start cell
  group_by(mmsi) %>%
  arrange(exitTimeStartCell) %>%
  # create separte tripids for vessels in case they visted cell >1
  mutate(tripid = cumsum(isSource)) %>%
  # tripid is 0 before vessel visted cell
  filter(tripid!=0) %>% 
  group_by(tripid,add = T) %>%
  mutate(finalDest = cumsum(cumsum(durationEndCell>cutoff))) %>%
  filter(finalDest<=1)  %>% 
  # mutate(routeid = paste(endCell,collapse = '; ')) %>% 
  ungroup() %>%
  filter(finalDest==1) %>%
  group_by(endCell,endLat,endLon) %>%
  summarise(n=n(),
            n_mmsi = n_distinct(mmsi),
            meanDays=mean(as.numeric(difftime(entryTimeEndCell,exitTimeStartCell,units = 'days'))),
            meanHrsDest = mean(as.numeric(durationEndCell),na.rm = T)) %>%
  arrange(desc(n))

  
group_by(routeid) %>%
  summarise(n=n(),
            n_mmsi=n_distinct(mmsi)) %>% View
  # did vessel return to a previously visted cell
  mutate(returnCell = uniqueCells(startCell)) %>%
  # retain obs only before they revisted a cell
  filter(!returnCell) %>%
  # retain only non-transit cells
  filter((durationStartCell>3)|isSource) %>%
  # identify the leg of the each journey
  mutate(leg = 1:n()) %>%
  ungroup() %>%
  arrange(mmsi) %>%
  select(mmsi,startCell,durationStartCell, isSource, tripid,returnCell, leg)

t2 %>%
  group_by(mmsi,tripid) %>%
  summarise(legs = max(leg))
  
t2 %>% filter(leg==1)
  
edges <- t2 %>%
  # filter(!isSource) %>%
  filter(leg<5) %>%
  select(-durationStartCell,-isSource,-returnCell) %>%
  group_by(mmsi,tripid) %>%
  mutate(endCell=lead(startCell)) %>%
  filter(!is.na(endCell)) %>%
  group_by(startCell,endCell) %>%
  summarise(value=n()) %>%
  rename(source=startCell,target=endCell) %>% 
  filter(value>1) %>%
  data.frame()

# for each cell
# plot the average time vessel is in the cell
# 

# qrage -------------
require(qrage)
summary(edges$value)
max(edges$value)*0.01
qrage(links=edges,cut=0.1,distance = 1000)


# d3 -------------------
require(networkD3)
# http://christophergandrud.github.io/networkD3/#radial

nodes <- c(edges$source,edges$target) %>%
  factor() %>%
  data.frame(name=.,
             group=1,
             size=1)

edges <- edges %>%
  mutate(source = factor(edges$source,levels = levels(nodes$name)),
         target = factor(edges$target,levels = levels(nodes$name)),
         source = as.integer(edges$source),
         target = as.integer(edges$target))
  

forceNetwork(Links = edges, Nodes = nodes,
             Source = "source", Target = "target",
             Value = "value", NodeID = "name",
             Group = "group", opacity = 0.8)






  
data(MisLinks)
data(MisNodes)
str(MisLinks)
str(MisNodes)
# Plot



forceNetwork(Links = MisLinks, Nodes = MisNodes,
             Source = "source", Target = "target",
             Value = "value", NodeID = "name",
             Group = "group", opacity = 0.8)



# collapsible tree -------------------
# https://adeelk93.github.io/collapsibleTree/
require(collapsibleTree)
sourcePort <- t2 %>%
  filter(!isSource) %>%
  filter(leg<6) %>%
  select(-durationStartCell,-isSource,-returnCell) %>%
  plyr::ddply(.,c('mmsi','tripid'),.fun = function(df){
    tidyr::spread(df,-mmsi,-tripid)
  })
collapsibleTree(sourcePort, c("2", "3", "4"))


collapsibleTree()

str(warpbreaks)



head(warpbreaks)
View(warpbreaks)

# Data from US Forest Service DataMart
species <- read.csv(system.file("extdata/species.csv", package = "collapsibleTree"))
collapsibleTree(df = species, c("REGION", "CLASS", "NAME"), fill = "green")

# Visualizing the order in which the node colors are filled
library(RColorBrewer)
collapsibleTree(
  warpbreaks, c("wool", "tension"),
  fill = brewer.pal(9, "RdBu"),
  fillByLevel = TRUE
)
collapsibleTree(
  warpbreaks, c("wool", "tension"),
  fill = brewer.pal(9, "RdBu"),
  fillByLevel = FALSE
)

# Tooltip can be mapped to an attribute, or default to leafCount
collapsibleTree(
  warpbreaks, c("wool", "tension", "breaks"),
  tooltip = TRUE,
  attribute = "breaks"
)

# Node size can be mapped to any numeric column, or to leafCount
collapsibleTree(
  warpbreaks, c("wool", "tension", "breaks"),
  nodeSize = "breaks"
)

# collapsibleTree.Node example
data(acme, package="data.tree")
acme$Do(function(node) node$cost <- data.tree::Aggregate(node, attribute = "cost", aggFun = sum))
collapsibleTree(acme, nodeSize  = "cost", attribute = "cost", tooltip = TRUE)

# Emulating collapsibleTree.data.frame using collapsibleTree.Node
species <- read.csv(system.file("extdata/species.csv", package = "collapsibleTree"))
hierarchy <- c("REGION", "CLASS", "NAME")
species$pathString <- paste(
  "species",
  apply(species[,hierarchy], 1, paste, collapse = "//"),
  sep = "//"
)
df <- data.tree::as.Node(species, pathDelimiter = "//")
collapsibleTree(df)


tjoin

# exploration of b5 cells ---------------------
# unsupervised clustering of band5 cells?
# - number of unique vessels
# - traffic composition

# - unique outbound connections
t1 <- tjoin %>%
  group_by(startCell,ihsShipTypel2) %>%
  summarise(nUniqueOutputs = n_distinct(endCell),
            nOutputs= n(),
            nUniqueVessels = n_distinct(mmsi)) %>%
  rename(cell= startCell)
  
# - unique inbound connections
t2 <- tjoin %>%
  group_by(endCell,ihsShipTypel2) %>%
  summarise(nUniqueInputs = n_distinct(startCell)) %>%
  rename(cell = endCell)


# for each cell for mean transit time
timeinCell <- tjoin %>%
  group_by(mmsi) %>%
  arrange(exitTimeStartCell) %>%
  mutate(durationStartCell=(exitTimeStartCell-lag(entryTimeEndCell))/60/60,
         durationStartCell=as.integer(round(durationStartCell))) %>%
  ungroup() %>%
  group_by(startCell,ihsShipTypel2) %>%
  summarise(meanHoursInCell = mean(durationStartCell,na.rm=T))

# distribution of meanHours by ship type
timeinCell %>%
  ggplot(.,aes(x=meanHoursInCell,fill=ihsShipTypel2)) +
  geom_density(alpha=0.5,adjust=0.2)+
  geom_vline(xintercept = 3)+
  facet_wrap(~ihsShipTypel2)+
  scale_x_log10()

t3 <- left_join(t1,t2,by=c('cell','ihsShipTypel2')) %>%
  left_join(.,rename(timeinCell,cell=startCell),by=c('cell','ihsShipTypel2'))
  
ggplot(t3)+
  geom_point(aes(x=meanHoursInCell,y=nOutputs),alpha=0.1)+
  facet_wrap(~ihsShipTypel2)+
  scale_x_log10()+
  scale_y_log10()

# there are some start cells ids not in end cells
all(unique(tjoin$startCell) %in% unique(tjoin$endCell))
# there are some end cells not in start cells
all(unique(tjoin$endCell) %in% unique(tjoin$startCell))



# topPassages --------------------

cutoff <- 5

t1 <- tjoin %>%
  # select ship type
  filter(ihsShipTypel2=='Tankers',tons>3000) %>% #,mmsi%in%mmsiList) %>%
  select(-startLat,-startLon,-tons,-ihsShipTypel5, -ihsShipTypel2, -UKHOShipType,
         -endLon,-endLat) %>%
  # retain only cells
  group_by(mmsi) %>%
  arrange(exitTimeStartCell) %>%
  mutate(durationStartCell=difftime(exitTimeStartCell,lag(entryTimeEndCell),units = 'hours'),
         durationStartCell=as.integer(round(durationStartCell)),
         durationEndCell = lead(durationStartCell),
         startIndex = durationStartCell>cutoff,
         endIndex = durationEndCell>cutoff) %>%
  filter(startIndex|endIndex) %>%
  mutate(id = cumsum(startIndex)) %>%
  filter(id!=0)  %>%
  # do(collapseTrack(.)) %>%
  filter(!is.na(endCell)) %>%
  ungroup()

t1 %>% arrange(mmsi) %>% View

t2 <- t1 %>%
  select(mmsi,startCell,exitTimeStartCell,durationStartCell,startIndex,id) %>%
  filter(startIndex)

t3 <- t1 %>%
  select(mmsi,endCell,entryTimeEndCell,durationEndCell,endIndex,id) %>%
  filter(endIndex) %>%
  left_join(t2,.,by=c('mmsi','id')) %>%
  filter(!is.na(endCell)) %>%
  select(mmsi,startCell,endCell,exitTimeStartCell,entryTimeEndCell)

t3 %>%
  arrange(startCell,endCell) %>%
  View()

t3 %>% 
  group_by(startCell,endCell) %>%
  summarise(n=n(),
            nVessels=n_distinct(mmsi),
            meanDays=mean(as.numeric(entryTimeEndCell-exitTimeStartCell)/60/60/24)) %>%
  ungroup() %>%
  left_join(enc_names,by=c('startCell'='cell')) %>%
  rename(startName=name) %>%
  left_join(enc_names,by=c('endCell'='cell')) %>%
  rename(endName=name) %>%
  arrange(desc(meanDays)) %>%
  View
  


  group_by(startCell,endCell) %>%
  summarise(n=n(),
            nVessels=n_distinct(mmsi),
            deltaT = entryTimeEndCell-exitTimeStartCell,
            meanDays=mean(as.numeric(entryTimeEndCell-exitTimeStartCell)/60/60/24)) %>%
  filter(meanDays>1) %>%
  arrange(desc(meanDays)) %>%
  left_join(enc_names,by=c('startCell'='cell'))


tt <- tjoin %>%
  filter(ihsShipTypel2=='Tankers',tons>3000) %>% #,mmsi%in%mmsiList) %>%
  select(-startLat,-startLon,-tons,-ihsShipTypel5, -ihsShipTypel2, -UKHOShipType,
         -endLon,-endLat) %>%
  mutate(delta_t=entryTimeEndCell-exitTimeStartCell,
         delta_2 = as.numeric(difftime(entryTimeEndCell,exitTimeStartCell,units = 'hours')))
tt
str(tt)


lubridate::

enc_names



tjoin %>%
  # select ship type
  filter(ihsShipTypel2=='Tankers',tons>3000) %>% #,mmsi%in%mmsiList) %>%
  select(-startLat,-startLon,-tons,-ihsShipTypel5, -ihsShipTypel2, -UKHOShipType,
         -endLon,-endLat) %>%
  # retain only cells
  group_by(mmsi) %>%
  arrange(exitTimeStartCell) %>%
  mutate(durationStartCell=difftime(exitTimeStartCell,lag(entryTimeEndCell),units = 'hours'),
         durationStartCell=as.integer(round(durationStartCell)),
         durationEndCell = lead(durationStartCell),
         startIndex = durationStartCell>cutoff,
         endIndex = durationEndCell>cutoff) %>%
  mutate(durationEndCell = difftime(lead(exitTimeStartCell),entryTimeEndCell,units = 'hours')) %>%
  ungroup() %>%
  arrange(mmsi) %>%
  View





# shortest path igrpah ----------------------------------------------------



require(igraph)


el <- matrix(nc=3, byrow=TRUE,
             c(1,2,0, 1,3,2, 1,4,1, 2,3,0, 2,5,5, 2,6,2, 3,2,1, 3,4,1,
               3,7,1, 4,3,0, 4,7,2, 5,6,2, 5,8,8, 6,3,2, 6,7,1, 6,9,1,
               6,10,3, 8,6,1, 8,9,1, 9,10,4) )
g2 <- add_edges(make_empty_graph(10), t(el[,1:2]), weight=el[,3])

shortest_paths(g2,from = 1,2)


el <- tjoin %>%
  filter(ihsShipTypel2=='Tankers',tons>3000) %>%
  mutate(distkm = distGeo(p1=select(.,startLon,startLat),p2=select(.,endLon,endLat))/1e3) %>%
  mutate(speedKnts = (distkm/(duration*24))*0.539957) %>%
  filter(speedKnts<50) %>%
  group_by(startCell,endCell) %>%
  summarise(duration=mean(duration,na.rm=T),
            n=n(),
            n_mmsi = n_distinct(mmsi),
            startLon = first(startLon),
            startLat = first(startLat),
            endLon = first(endLon),
            endLat = first(endLat)) %>%
  ungroup()

arrange(el,desc(n)) %>% View

quantile(el$speedKnts,0.90)
summary(el$speedKnts)
hist(el$speedKnts)



celldf <- tibble(cell = unique(c(el$startCell,el$endCell))) %>%
  mutate(nodeid=1:nrow(.)) %>%
  left_join(portLocs,by=c('cell'='X1')) %>%
  left_join(enc_names,by='cell')
       
  
elm <- left_join(el,celldf,by=c('startCell'='cell')) %>%
  rename(source=nodeid) %>%
  left_join(.,celldf,by=c('endCell'='cell')) %>%
  rename(target=nodeid) %>%
  select(source,target,duration) %>%
  as.matrix()


g2 <- add_edges(make_empty_graph(nrow(celldf)), t(elm[,1:2]), weight=elm[,3])

sp <- shortest_paths(g2, 100, 200)

route <- tibble(nids =as.vector(sp$vpath[[1]])) %>%
  left_join(celldf,by=c('nids'='nodeid'))



tjoin %>%
  filter(ihsShipTypel2=='Tankers',tons>3000) %>%
  filter(startCell=='AU5144P0',endCell=='GB53142C') %>%
  View

# cellid, lat, lon, name, nodeid
require(geosphere)

geosphere::distGeo(c(0,0),c(1,1))




# tjoin %>%
#   filter(ihsShipTypel2=='Tankers',tons>3000) %>%
#   filter(startCell=='EG5EGR11',endCell=='SG5C4039') %>%
#   select(mmsi, exitTimeStartCell,entryTimeEndCell) %>%
#   write.csv(.,'suez_malacca_mmsi_list.csv',row.names = F)
# 
# 









