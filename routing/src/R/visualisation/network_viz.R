
source('load_data.R')

require('dplyr')
library(GGally)
library(network)
library(sna)
library(ggplot2)
require('tidyr')
require(lubridate)
require('stringr')
require(igraph)
require('igraph')
require(rgeos)
require(leaflet)


# -----------------
# https://briatte.github.io/ggnet/
  
str(tsm)
net = network(tsm, directed = FALSE)

# vertex names
network.vertex.names(net) = row.names(tsm)

code <- row.names(tsm) %>% str_sub(0,2)

nodes <- data.frame(Node=row.names(tsm),stringsAsFactors = F) %>%
  mutate(Group = str_sub(Node,0,2))

str(nodes)



library(igraph)
dat=read.csv(file.choose(),header=TRUE,row.names=1,check.names=FALSE) 
m=as.matrix(dat)
net=graph.adjacency(m,mode="directed",weighted=TRUE,diag=FALSE) 

plot.igraph(net,vertex.label=V(net)$name,layout=layout.fruchterman.reingold, vertex.label.color="black",edge.color="black",edge.width=E(net)$weight/3, edge.arrow.size=1.5,edge.curved=TRUE)

ggnet(net)
ggnet2(net, mode = "circle")
ggnet2(net, mode = "kamadakawai")
# ggnet2(net, mode = "fruchtermanreingold", layout.par = list(cell.jitter = 0.75))
# ggnet2(net, mode = "target", layout.par = list(niter = 100))

net %v% "code" <- code

net

ggnet2(net,size=1,edge.size = as.vector(tsm[tsm!=0]))
net$val


ts$startCell

ggplot(data = ts, aes(from_id = startCell, to_id = endCell)) +
  geom_net(aes(colour = log10(n)), layout.alg = "kamadakawai", 
           size = 2,  ecolour = "grey60",
           directed =FALSE, fontsize = 3, ealpha = 0.5) #+
  # scale_colour_manual(values = c("#FF69B4", "#0099ff")) +
  # xlim(c(-0.05, 1.05)) +
  # theme_net() +
  # theme(legend.position = "bottom")




# --------------
# https://cran.r-project.org/web/packages/ggCompNet/vignettes/examples-from-paper.html

sw <- sample_smallworld(dim=2, size=10, nei=1, p=0.1)

plot(sw, vertex.size=6, vertex.label=NA, layout=layout_in_circle)



# ggnetwork ---------------
# https://briatte.github.io/ggnetwork/#edge-weights


library(network)
library(sna)
n <- network(rgraph(10, tprob = 0.2), directed = FALSE)
n
n %v% "family" <- sample(letters[1:3], 10, replace = TRUE)
n %v% "importance" <- sample(1:3, 10, replace = TRUE)

e <- network.edgecount(n)
set.edge.attribute(n, "type", sample(letters[24:26], e, replace = TRUE))
set.edge.attribute(n, "day", sample(1:3, e, replace = TRUE))

network.edgecount(net)



# ------------------
# https://github.com/garthtarr/edgebundleR
library(edgebundleR)

require(igraph)
ws_graph = watts.strogatz.game(1, 50, 4, 0.05)

edgebundle(tsm,tension = 0.1,fontsize = 20)



# --------------
require(qrage)
#Data to determine the connection between the nodes
data(links)
#Data that determines the color of the nodes
data(nodeColor)
#Data that determines the size of the node
data(nodeValues)
#Create graph
links

qrage(links=links,cut=0.1)


qrage(links=data.frame(ts[1:100,]),cut = 0.1,)


# igrpah ----------------

library(igraph)
dat=read.csv(file.choose(),header=TRUE,row.names=1,check.names=FALSE) 
m=as.matrix(dat)
str(tsm)

net=graph.adjacency(tsm[1:1000,1:1000],mode="directed",weighted=TRUE,diag=FALSE) 

net

plot.igraph(net,
            vertex.size=3,
            vertex.label=NA,
            #vertex.label=V(net)$name,
            layout=layout.fruchterman.reingold,
            #layout=layout_as_tree,
            #vertex.label.color="black",
            #edge.color="black",
            #edge.width=E(net)$weight/100,
            edge.arrow.size=0.5,
            edge.curved=TRUE)

layout <- layout.fruchterman.reingold(g)

# networkD3 ----------------------
# http://christophergandrud.github.io/networkD3/
require(networkD3)

data(MisLinks)
data(MisNodes)
str(MisLinks)
str(MisNodes)
# Plot

forceNetwork(Links = MisLinks, Nodes = MisNodes,
             Source = "source", Target = "target",
             Value = "value", NodeID = "name",
             Group = "group", opacity = 0.8)

str(data.frame(ts))
ts$value <- log(ts$n)

summary(MisLinks$target)

forceNetwork(Links = data.frame(ts[1:50,]),Nodes=nodes,
             Source = "startCell", Target = "endCell",
             Value = "value", NodeID='Node',Group='Group',
             opacity = 0.8)


netd3 <- igraph_to_networkD3(net)
netd3$nodes$group <- 1
str(netd3$links)
forceNetwork(Links = netd3$links, Nodes = netd3$nodes, 
             Source = 'source', Target = 'target', 
             NodeID = 'name', Group = 'group')




# L5 centroids --------------------------------------------------------------------


pal <- colorFactor(c("red",'navy'), domain = c('in','out'))
topDests %>%
  # slice(1:400) %>%
  leaflet() %>%
  addTiles() %>% 
  addCircleMarkers(~V1, ~V2,
                   popup = ~n_mmsi, 
                   fillColor=~pal(top400),
                   radius = ~sqrt(sqrt(n_mmsi)),
                   stroke=F,
                   fillOpacity = 0.8)


m %>% addCircleMarkers(~lng, ~lat, radius = runif(100, 4, 10), color = c('red'))


# SD ----------------------------------------------------------------------

polys <- readr::read_csv("avcs_cat.csv",col_names = F)
View(polys)





