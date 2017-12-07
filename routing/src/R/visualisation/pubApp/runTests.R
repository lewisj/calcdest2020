library(shinytest)
library(testthat)
source('loadLibraries.R')


# === GUI Tests ===
# test_that('Application works', {
#   # Use compareImages=FALSE because the expected image screenshots were created
#   # on Linux, and they may differ from screenshots taken on Windows
#   expect_pass(testApp('.', compareImages = FALSE))
# })

test_file("unitTests/test_queryVGD.R")
test_file("unitTests/test_getTopCells.R")
