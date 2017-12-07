#library(devtools)
#install_github("rstudio/shinytest")
#webdriver::install_phantomjs()

library(devtools)
devtools::install_github('asmith26/shinytest')
library("shinytest")

recordTest(".")
