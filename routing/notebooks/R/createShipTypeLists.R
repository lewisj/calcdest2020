data = read.csv("/home/andrew/git/routing/src/scala/src/main/resources/2017_ihs_jun.csv", header = FALSE)
colnames(data) = c("ihsMMSI", "ihsShipTypeLevel5", "ihsShipTypeLevel2", "ihsUkhoVesselType", "ihsGrossTonnage")

write.csv(sort(unique(data$ihsShipTypeLevel5)), "/tmp/ihsShipTypeLevel5.csv")
write.csv(sort(unique(data$ihsShipTypeLevel2)), "/tmp/ihsShipTypeLevel2.csv")
write.csv(sort(unique(data$ihsUkhoVesselType)), "/tmp/ihsUkhoVesselType.csv")
write.csv(sort(unique(data$ihsGrossTonnage)), "/tmp/ihsGrossTonnage.csv")
