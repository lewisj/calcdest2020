app <- ShinyDriver$new("../")
app$snapshotInit("testVesselTypesInput")

# Input 'mymap_groups' was set, but doesn't have an input binding.
app$snapshot()
app$setInputs(slt_VT = "Dry Cargo/Passenger")
app$snapshot()
app$setInputs(slt_VT = "Bulk Carriers")
app$snapshot()
app$setInputs(slt_VT = "Fishing")
app$snapshot()
app$setInputs(slt_VT = "All Vessel Types")
app$snapshot()
