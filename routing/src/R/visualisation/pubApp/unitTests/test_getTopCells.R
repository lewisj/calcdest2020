cat("\nRunning test_getTopCells.R ")
source('../utils.R', local = T)

test_that('test getTopCells returns correctly', {
  
  encNames <- tibble(cell = c('a','b','c'),
                     name = c('nameA','nameB','nameC'))
  
  inputDf <- tibble(startCell = c('a','a','a','b','b','b','c','c','c'),
                    startLat = c(52,52,52,-20,-20,-20,30,30,30),
                    startLon = c(-1.5,-1.5,-1.5,4,4,4,20,20,20),
                    mmsi = c(1,2,3,1,2,2,1,1,1))
  
  actualDf <- getTopCells(inputDf,
                          nTopCells = 2,
                          encNames)
  
  expectedDf <- tibble(cell = c('a','b'),
                    lat = c(52,-20),
                    lon = c(-1.5,4),
                    n = as.integer(c(3,3)),
                    n_mmsi= as.integer(c(3,2)),
                    name = c('nameA','nameB'))
  
  
  expect_identical(actualDf, expectedDf)
  
})


test_that('test getTopCells returns correctly when inputDf is NULL', {
  
  encNames <- tibble(cell = c('a','b','c'),
                     name = c('nameA','nameB','nameC'))
  
  inputDf <- NULL
  
  actualDf <- getTopCells(inputDf,
                          nTopCells = 2,
                          encNames)
  
  expectedDf <- NULL
  
  
  expect_identical(actualDf, expectedDf)
  
})


test_that('test getTopCells returns correctly when name is not present', {
  
  encNames <- tibble(cell = c('a','c'),
                     name = c('nameA','nameC'))
  
  inputDf <- tibble(startCell = c('a','a','a','b','b','b','c','c','c'),
                    startLat = c(52,52,52,-20,-20,-20,30,30,30),
                    startLon = c(-1.5,-1.5,-1.5,4,4,4,20,20,20),
                    mmsi = c(1,2,3,1,2,2,1,1,1))
  
  actualDf <- getTopCells(inputDf,
                          nTopCells = 2,
                          encNames)
  
  expectedDf <- tibble(cell = c('a','b'),
                       lat = c(52,-20),
                       lon = c(-1.5,4),
                       n = as.integer(c(3,3)),
                       n_mmsi= as.integer(c(3,2)),
                       name = c('nameA',NA))
  
  
  expect_identical(actualDf, expectedDf)
  
})

