destinationsDf = read.csv("/home/andrew/git/routing/notebooks/R/PostGIS_Integration/destinationsDf.csv")

tracksDf <- do.call(rbind, apply(destinationsDf, 1, FUN=postGisQuery))

library(parallel)
nodes <- detectCores()
cl <- makeCluster(nodes)

tracksDf_par <- do.call(rbind, parApply(cl, destinationsDf, 1, FUN=postGisQuery))





library(doParallel)
library(plyr)
library(RPostgreSQL)

registerDoParallel(cl)

a=plyr::aaply(destinationsDf[1:10,], 1, .fun = nrow, .parallel = T)
