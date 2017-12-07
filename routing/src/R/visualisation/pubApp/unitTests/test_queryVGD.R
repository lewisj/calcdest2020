cat("\nRunning test_queryVGD.R ")
source('../utils.R', local = T)

test_that('test queryVGD returns correctly regarding ihsShipTypel2', {
  
  inputDf <- tibble(exitTimeStartCell = ymd_hms(c('2016-01-01 00-00-00', '2016-01-21 00-00-00', '2016-02-01 00-00-00')),
                    entryTimeEndCell = ymd_hms(c('2016-01-02 00-00-00', '2016-01-22 00-00-00', '2016-02-02 00-00-00')),
                    ihsShipTypel2 = c("Tankers", NA, "Fishing"),
                    tons=as.integer(c(1500, 1000, 1750)))
  
  actualDf <- queryVGD(inputDf, 
                       'Tankers',
                       c(1000, 2000),
                       c(ymd_hms(c('2016-01-01 00-00-00','2016-12-31 23-59-59'))))
  
  expectedDf <- inputDf[c(1),]
  
  expect_identical(actualDf, expectedDf)
  
})


test_that('test queryVGD returns correctly regarding date', {
  
  inputDf <- tibble(exitTimeStartCell = ymd_hms(c('2016-06-08 00-00-00', '2016-06-21 00-00-00', '2016-02-01 00-00-00')),
                    entryTimeEndCell = ymd_hms(c('2016-06-25 00-00-00', '2016-08-22 00-00-00', '2016-06-02 00-00-00')),
                    ihsShipTypel2 = c("Tankers", 'Tankers', "Tankers"),
                    tons=as.integer(c(1500, 1000, 1750)))
  
  actualDf <- queryVGD(inputDf, 
                       'Tankers',
                       c(1000, 2000),
                       c(ymd_hms(c('2016-06-01 00-00-00','2016-06-30 23-59-59'))))
  
  expectedDf <- inputDf[c(1),]
  
  expect_identical(actualDf, expectedDf)
  
})


test_that('test queryVGD returns correctly regarding tonnage', {
  
  inputDf <- tibble(exitTimeStartCell = ymd_hms(c('2016-06-08 00-00-00', '2016-06-21 00-00-00', '2016-06-01 00-00-00')),
                    entryTimeEndCell = ymd_hms(c('2016-06-25 00-00-00', '2016-06-22 00-00-00', '2016-06-02 00-00-00')),
                    ihsShipTypel2 = c("Tankers", 'Tankers', "Tankers"),
                    tons=as.integer(c(700, 1000, 2500)))
  
  actualDf <- queryVGD(inputDf, 
                       'Tankers',
                       c(1000, 2000),
                       c(ymd_hms(c('2016-06-01 00-00-00','2016-06-30 23-59-59'))))
  
  expectedDf <- inputDf[c(2),]
  
  expect_identical(actualDf, expectedDf)
  
})


test_that('test queryVGD returns correctly when inputDf is empty', {
  
  inputDf <- tibble()
  
  actualDf <- queryVGD(inputDf, 
                       'Tankers',
                       c(1000, 2000),
                       c(ymd_hms(c('2016-01-01 00-00-00','2016-01-31 23-59-59'))))
  
  expectedDf <- NULL
  
  expect_identical(actualDf, expectedDf)
  
})


test_that('test queryVGD returns correctly when returned tibble has 0 rows', {
  
  inputDf <- tibble(exitTimeStartCell = ymd_hms(c('2016-01-01 00-00-00', '2016-01-21 00-00-00', '2016-02-01 00-00-00')),
                    entryTimeEndCell = ymd_hms(c('2016-01-02 00-00-00', '2016-01-22 00-00-00', '2016-02-02 00-00-00')),
                    ihsShipTypel2 = c("Tankers", NA, "Tankers"),
                    tons=as.integer(c(1500, 1000, 1750)))
  
  actualDf <- queryVGD(inputDf, 
                       'Tankers',
                       c(10000, 20000),
                       c(ymd_hms(c('2016-01-01 00-00-00','2016-01-31 23-59-59'))))
  
  expectedDf <- NULL
  
  expect_identical(actualDf, expectedDf)

})
