source('loadLibraries.R')
source('utils.R', local = TRUE)
  
# for parallelising PostGIS query
#cl <- makeCluster(detectCores())
cl <- makeCluster(16)


dataMode <- 'app'  # one of 'test', 'development', 'app'

if (dataMode != 'development'){
  source('load_data.R')
}
