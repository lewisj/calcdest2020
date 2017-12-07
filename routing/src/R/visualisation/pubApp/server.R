function(input, output, session) {
  
  data <- reactive({
    
    leafletProxy('mymap') %>% clearMarkers()
    leafletProxy('mymap') %>% clearShapes()
    
    queryVGD(tjoin, 
             input$slt_VT,
             input$sld_tons,
             input$sld_date)
    
  })
  
  output$nrows <- renderUI({
    
    num_destinations <- nrow(data())
    
    if(is.null(num_destinations)) {
      print(HTML('<span style="color: red;">0 records</span>'))
    } else {
      print(paste(num_destinations,'records'))
    }
    
  })
  
  cells <- reactive({
    
    if(input$rdo_inout=='inbound'){
      
      topDests <- data() %>% 
        group_by(endCell,endLat,endLon) %>%
        summarise(n = n(),
                  n_mmsi = n_distinct(mmsi)) %>%
        arrange(desc(n_mmsi)) %>%
        mutate(code=str_sub(endCell,0,2)) %>%
        rename(cell=endCell,
               lat=endLat,
               lon=endLon) %>%
        left_join(enc_names,by='cell')
      topDests[1:input$sld_topn,]
      
    } else {
      
      getTopCells(data(), input$sld_topn, enc_names)
      
    }
    
  })
  
  output$mymap <- renderLeaflet({
    
    leaflet() %>% 
      addProviderTiles(providers$CartoDB.Positron,group='CartoDB.Positron') %>%
      # addProviderTiles(providers$Stamen.Toner, group = "Toner") %>%
      addProviderTiles(providers$Stamen.TonerLite, group = "Toner Lite") %>%
      addTiles(group = "OSM") %>%
      # addProviderTiles("CartoDB.PositronOnlyLabels")
      # addProviderTiles('Esri.OceanBaseMap',group='Esri.OceanBaseMap') %>%
      addProviderTiles('Esri.WorldImagery',group='Esri.WorldImagery') %>%
      # addProviderTiles('Esri.WorldTerrain',group='Esri.WorldTerrain') %>%
      addTiles('http://fusillidata.ukwest.cloudapp.azure.com/tank_2016/{z}/{x}/{y}.png',group='Tanker AIS') %>%
      addTiles('http://fusillidata.ukwest.cloudapp.azure.com/passenger_2016/{z}/{x}/{y}.png',group='Passenger AIS') %>%
      addLayersControl(
        baseGroups = c('CartoDB.Positron', "Toner Lite","OSM (default)",'Esri.WorldImagery'),
        overlayGroups = c('Tracks',"edges", "cells",'pings','Waypoints','Tanker AIS','Passenger AIS'),
        options = layersControlOptions(collapsed = FALSE)
      ) %>%
      # addProviderTiles(providers$Esri.WorldStreetMap) %>%
      # addMiniMap() %>%
      # addTiles() %>%
      addMeasure(position = "bottomleft",
                 primaryLengthUnit = "meters",
                 primaryAreaUnit = "meters") %>%
      hideGroup('Tanker AIS') %>%
      hideGroup('Passenger AIS') %>%
      setView(lng = 0,lat = 20,zoom=3)
    
  })
  
  
  observe({
    
    input$resetQuery
    
    leafletProxy('mymap') %>% clearMarkers()
    leafletProxy('mymap') %>% clearShapes()
    selectedTrack <<- NULL
    
    pal <- colorFactor(c('navy','red'), domain = c(TRUE, FALSE))
    # pal <- colorNumeric('Spectral',reverse = T, log10(cells()$n_mmsi))
    # pal <- colorQuantile('Spectral',reverse = T, cells()$n_mmsi)
    
    if(is.null(cells())) return()
    
    topCells <- cells()
    nTopCell <- min(nrow(topCells), 250)
    topCells$top250 <- FALSE
    topCells$top250[1:nTopCell] <- TRUE
    
    leafletProxy('mymap',data= topCells) %>%
      addCircles(~lon, ~lat,group = 'cells',
                 label = ~paste0(cell,': ',name),
                 # popup = ~n_mmsi, 
                 layer = ~cell,
                 # fillColor=~pal((n_mmsi)),
                 fillColor =~pal(top250),
                 radius =2e3,
                 stroke=T,
                 # color =~pal((n_mmsi)), 
                 color =~pal(top250),
                 fillOpacity = 0.8)
    
    
    
  })
  
  observeEvent(input$rdo_inout,{
    
    toggleState('sld_transitCell',input$rdo_inout!='inbound')
    
  })
  
  
  observeEvent(input$mymap_shape_mouseover,{
    print(input$mymap_shape_mouseover$id)
  })
  
  
  observeEvent(input$mymap_shape_click,{
    
    group <- input$mymap_shape_click$group
    clickId <- input$mymap_shape_click$id
    
    switch(group,
           'edges' = edgeClick(clickId),
           'Tracks' = trackClick(clickId),
           return()
    )
    
    
    
  })
  
  selectedTrack <- NULL
  makeReactiveBinding('selectedTrack')
  
  trackid <- NULL
  makeReactiveBinding('trackid')
  
  edgeClick <- function(clickId){
    
    trackid <<- gsub('_' ,'',clickId)
    
    toggleModal(session,'mdl_edgeInfo',toggle = 'open')
    
  }
  
  trackClick <- function(clickId){
    
    selectedTrack <<- clickId
    
    selectedAisDf <- filterReturnedAisDf %>%
      filter(id == clickId)
    
    
    leafletProxy('mymap', data=selectedAisDf) %>%
      clearGroup('pings') %>%
      addCircles(~lon, ~lat, group = 'pings',
                 fillColor ="#03F",
                 fillOpacity = 0.2,
                 weight = 5,     # outline width
                 opacity = 0.5,  # outline opacity
                 color = "#03F", # outline colour
                 popup=~acquisition_time)
    
    
  }
  
  observe({
    
    if(is.null(selectedTrack)) return()
    if(is.null(input$sld_tol)) return()
    
    selectedTrackID <- selectedTrack
    tol <- input$sld_tol
    
    tol <- ifelse(class(tol)=='integer',as.numeric(tol),tol) 
    
    selectedSLDF <- sldf[selectedTrackID,]
    
    selectedSLDF_s <- gSimplify(selectedSLDF,tol=tol)
    
    # make global to use in download
    waypoints <<- selectedSLDF_s@lines[[1]]@Lines[[1]]@coords
    
    leafletProxy('mymap') %>%
      clearGroup('Waypoints') %>%
      addMarkers(lng=waypoints[,1],lat = waypoints[,2],group = 'Waypoints')
    
    
  })
  
  output$selectTrackUI <- renderUI({
    
    if(is.null(selectedTrack)) return(NULL)
    absolutePanel(top = 20,right = 500,fixed = F,draggable = T,
                  sliderInput('sld_tol',min = 0.01,max = 1,
                              value=0.1,label='waypoint tolerance (dd)'))
    
  })
  
  
  observe({
    
    toggleState('downloadShp',!is.null(selectedTrack))
    
  })
  
  observe({
    
    toggleState('downloadCells',!is.null(cells()))
    
  })
  
  
  observe({
    
    toggleState('downloadRoutes', !is.null(linkeddf) && nrow(linkeddf)>0)
    
  })
  
  
  trackDf <- eventReactive(trackid,{
    
    cellDf %>%
      filter(startCell==id,endCell==trackid) %>%
      select(mmsi,startCell,endCell,exitTimeStartCell,entryTimeEndCell) %>%
      mutate(Duration=difftime(entryTimeEndCell,exitTimeStartCell,units = 'days'))
    
  })
  
  output$routeDurationHist <- renderPlot({
    
    trackDf() %>%
      ggplot()+
      geom_histogram(aes(x=Duration),binwidth = 1)+
      xlab('Duration (days)')
    
  })
  output$routePlot <- renderPlot({
    
    trackDf() %>%
      ggplot()+
      geom_point(aes(x=exitTimeStartCell,y=Duration))
    
  })
  
  
  queryDf <- eventReactive(input$plot1_brush,{
    brushedPoints(trackDf(), input$plot1_brush)
  })
  
  output$routeInfo <- renderUI({
    
    from <- enc_names %>% filter(cell==id) %>% .$name
    to <- enc_names %>% filter(cell==trackid) %>% .$name
    
    tagList(
      h4(paste(id,from)),
      h4('   >>>>   '),
      h4(paste(trackid,to)),
      hr(),
      h4(paste('Unique vessels:',length(unique(trackDf()$mmsi)))),
      # p(),
      h4(p(paste('Journeys:', nrow(trackDf()))))
    )
    
    
    
    
  }) 
  
  output$routeDurationStats <- renderPrint({
    summary(as.numeric(trackDf()$Duration))
  })
  
  observe({
    # str(queryDf())
    
    toggleState('btn_plotTracks',!(is.null(queryDf())||!(nrow(queryDf()))))
  })
  
  
  observeEvent(input$btn_plotTracks,{
    
    # Get tracks to query PostGIS against (from a user click)
    tracksToQuery <- queryDf()
    
    tracksToQuery$id <- 1:nrow(tracksToQuery)
    
    
    disable('btn_plotTracks')
    
    toggleModal(session,'mdl_edgeInfo',toggle = 'close')
    
    
    # Run query on the first 100 tracks otherwise the number of tracks available (if <100)
    # num_query <- nrow(tracksToQuery)
    # if (num_query > 100) {
    #   num_query <- 100
    # }
    # 
    # returnedAisDfList <- parApply(cl, tracksToQuery[1:num_query,], 1, FUN=postGisQuery)
    
    returnedAisDfList <- parApply(cl, tracksToQuery, 1, FUN=postGisQuery)
    
    if (length(returnedAisDfList) == 0) {
      print("No tracks found - try increasing the max_distance between AIS pings allowed (see utils.R)")
      return()
    }
    
    returnedAisDf <- do.call(rbind, returnedAisDfList) %>%
      removeDodgyAis()
    
    filterReturnedAisDf <<- returnedAisDf
    # filterReturnedAisDf <<- plyr::ddply(returnedAisDf_full, 'id', getAisWithSensibelDistanceBetweenPings)
    
    lineDf <- filterReturnedAisDf %>%
      group_by(id,mmsi) %>%
      summarise(n=n()) %>%
      data.frame() %>%
      left_join(ihs, by='mmsi')
    
    # lineDf <- data.frame(id=unique(returnedAisDf$id))
    row.names(lineDf) <- lineDf$id
    
    # made global so it is used to calc waypoints
    sldf <<- plyr::dlply(filterReturnedAisDf,'id',function(x){
      Lines(list(Line(cbind(x$lon,x$lat))),x$id[1])}) %>%
      setNames(.,NULL) %>%
      SpatialLines() %>%
      SpatialLinesDataFrame(data=lineDf)
    
    pal <- colorFactor(palette = "RdYlBu",domain = unique(filterReturnedAisDf$mmsi))
    
    leafletProxy('mymap') %>% 
      clearGroup('edges') %>%
      addPolylines(data = sldf,
                   group = 'Tracks',
                   color = ~pal(mmsi),
                   layer=~id,
                   label=~paste0(id, '. ', mmsi, ' - ', tons, 'tons - ', ihsShipTypel5))
    
    enable('btn_plotTracks')
    
    
  })
  
  
  observe({
    
    if(is.null(data())) return()
    
    cutoff <- input$sld_transitCell
    click <- input$mymap_shape_click
    inOut <- input$rdo_inout
    removeStartEndSameCountry <- input$removeStartEndSameCountry
    onlyShowTopCellConnects <- input$chk_onlyShowTopNCncts
    chk_customerOnly <- input$chk_customerOnly
    nMin <- input$sld_min
    
    
    if(is.null(click)||click$group!='cells') return()
    
    # print(id)
    id <<- input$mymap_shape_click$id
    # print(id)
    # print(input$mymap_marker_mouseover)
    
    if(inOut=='inbound'){
      
      cellDf <<- data() %>%
        filter(endCell == id)
      
      linkeddf <<-  cellDf %>%
        group_by(startCell,startLat,startLon) %>%
        summarise(n = n(),
                  n_mmsi=n_distinct(mmsi),
                  meanDays=mean(as.numeric(difftime(entryTimeEndCell,exitTimeStartCell,units = 'days'))),
                  meanHrsDest = mean(durationEndCell,na.rm = T)
        ) %>%
        ungroup() %>%
        rename(cell=startCell,
               lon=startLon,
               lat=startLat) %>%
        filter(n>input$sld_min) %>% 
        left_join(enc_names,by='cell') %>%
        data.frame()
    } else {
      
      
      # final destinations filtering
      
      mmsiList <- data() %>%
        filter(startCell==id) %>%
        .$mmsi %>%
        unique()
      
      cellList <- cells()$cell
      
      cellDf <<- data() %>%
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
        # for each vessel trip
        group_by(tripid,add = T) %>%
        # double cumsum gives returns 1 when vessel is in final dest cell 
        mutate(finalDest = cumsum(cumsum(durationEndCell>cutoff))) %>%
        filter(finalDest<=1)  %>%
        ungroup() %>% 
        # collapse table to return 'real' start and end cell/time/country
        group_by(mmsi,tripid) %>%
        summarise(startCell=first(startCell),
                  endCell= last(endCell),
                  exitTimeStartCell = first(exitTimeStartCell),
                  entryTimeEndCell = last(entryTimeEndCell),
                  startLat=first(startLat),
                  startLon = first(startLon),
                  endLat=last(endLat),
                  endLon=last(endLon),
                  startEezCountry = first(startEezCountry),
                  endEezCountry = last(endEezCountry),
                  durationEndCell = last(durationEndCell)) %>%
        ungroup %>%
        filter(startCell != endCell) %>%
        { if(removeStartEndSameCountry) filter(., startEezCountry != endEezCountry) else . } %>%
        { if(onlyShowTopCellConnects) filter(., endCell %in% cellList) else . } %>%
        { if(chk_customerOnly) filter(.,mmsi %in% customMmsi$X1) else . }
      
      
      
      # 
      linkeddf <<- cellDf %>%
        group_by(endCell,endLat,endLon) %>%
        summarise(n=n(),
                  n_mmsi = n_distinct(mmsi),
                  meanDays=mean(as.numeric(difftime(entryTimeEndCell,exitTimeStartCell,units = 'days'))),
                  meanHrsDest = mean(as.numeric(durationEndCell),na.rm = T)) %>%
        arrange(desc(n))%>%
        ungroup() %>%
        rename(cell=endCell,
               lon=endLon,
               lat=endLat)%>%
        filter(n>nMin) %>%
        left_join(enc_names,by='cell') %>%
        arrange(desc(n_mmsi)) %>%
        data.frame()
      
      
      
    }
    
    if(nrow(linkeddf)){
      
      # df2 <- linkeddf
      lon <- input$mymap_shape_click$lng
      lat <- input$mymap_shape_click$lat
      
      base <- c(lon,lat)
      
      rownames(linkeddf) <- linkeddf$cell
      
      # sldf <- plyr::dlply(linkeddf,'cell',function(x){
      #   Lines(list(Line(rbind(base,c(x$lon,x$lat)))),x$cell)}) %>%
      #   setNames(.,NULL) %>%
      #   SpatialLines() %>%
      #   SpatialLinesDataFrame(data=linkeddf)
      
      
      sldf <- plyr::dlply(linkeddf,'cell',function(x){
        rbind(base,c(x$lon,x$lat)) %>%
          # rbind(c(-160,0),c(160,0)) %>%
          st_linestring %>%
          st_sfc(crs = 4326) %>%
          st_wrap_dateline(c("WRAPDATELINE=YES","DATELINEOFFSET=170")) %>%
          st_sf(n_mmsi=x$n_mmsi,name=x$name,cell=x$cell,.) 
      }) %>% 
        do.call(rbind,.)
      
      
      
      # Setup opacity calculation
      getOpacity <- function(oldValue) {
        # https://stackoverflow.com/a/929107/5179470
        newMin <- 0.2
        newMax <- 0.8
        oldMin <- min(linkeddf$n_mmsi)
        oldMax <- max(linkeddf$n_mmsi)
        oldRange <- (oldMax - oldMin)  
        newRange <- (newMax - newMin)
        return((((oldValue - oldMin) * newRange) / oldRange) + newMin)
      }
      
      # plot(sldf)
      leafletProxy('mymap') %>% 
        clearGroup('edges') %>%
        clearGroup('Tracks') %>%
        clearGroup('pings') %>%
        clearGroup('Waypoints') %>%
        addPolygons(data=sp.polys[polys$X1==id][[1]],group='cell_extent',layer='cellPoly',stroke = T,fill = NULL,color = 'red') %>%
        addPolylines(data = sldf,
                     group = 'edges',
                     weight = 5,
                     opacity = ~getOpacity(n_mmsi),
                     layer = ~paste0(cell,'_'),
                     label = ~paste0(cell,': ',name,', n_mmsi=',n_mmsi)
        )
      
      selectedTrack <<- NULL
      
    }
    
    
  })
  
  id <- NULL
  makeReactiveBinding('id')
  
  linkeddf <- NULL
  makeReactiveBinding('linkeddf')
  
  
  
  output$id <- renderText({
    paste(id,enc_names$name[enc_names$cell==id],isolate(input$rdo_inout))
  })
  
  output$cellList <- renderTable({
    if(length(linkeddf)){
      linkeddf %>% 
        arrange(desc(n_mmsi)) #%>%
      # mutate(pct = round((n / sum(n))*100,2))
      # select(lat:meanDays,cell,name)
      # select(lat:meanDays,cell,name)
    }
    
  })
  
  # output$tonnagePlot <- renderPlot({
  #   data() %>%
  #     ggplot()+geom_histogram(aes(x=tons))+ylab('')
  #   
  # })
  
  
  # Download shapefile on click
  output$downloadShp <- downloadHandler(
    
    shapefilePrefix <- paste0('AIS_track-', gsub(':','-',Sys.time())),
    
    # Download file name
    filename = function() {
      
      paste0(shapefilePrefix,'.zip')
      
    },
    
    # Download file content
    content = function(file) {
      
      trackDf <- filterReturnedAisDf %>% filter(id == selectedTrack)
      mmsi <- trackDf$mmsi[1]
      
      # polyline shapefile
      
      # lineDf <- data.frame(id='1',mmsi=mmsi,startCell=id,endCell=trackid)
      # rownames(lineDf) <- lineDf$id
      # 
      # sldf <- Lines(list(Line(cbind(trackDf$lon,trackDf$lat))),'1') %>%
      #   list() %>%
      #   SpatialLines() %>%
      #   SpatialLinesDataFrame(data=lineDf)
      
      # create multipart lines of vessel track
      lineDf <- trackDf %>%
        select(-mmsi,-id) %>%
        rename(startLon = lon,
               startLat = lat,
               startTime = acquisition_time) %>%
        mutate(endLon = lead(startLon),
               endLat = lead(startLat),
               endTime = lead(startTime),
               segid = 1:n()) %>%
        slice(1:(nrow(.)-1)) %>%
        data.frame()
      rownames(lineDf) <- lineDf$segid
      
      sldf <- plyr::dlply(lineDf,'segid',function(x){
        Lines(list(Line(rbind(c(x$startLon,x$startLat),
                              c(x$endLon,x$endLat)))),x$segid)}) %>%
        setNames(.,NULL) %>%
        SpatialLines() %>%
        SpatialLinesDataFrame(data=lineDf)
      
      
      sldf@proj4string <- CRS("+proj=longlat +datum=WGS84 +elips=WGS84")
      
      # Clean tmp directory (if it exists)
      if (length(Sys.glob("tmp/*"))>0){
        file.remove(Sys.glob("tmp/*"))
      }
      
      name <- paste(mmsi,id,trackid,sep='-')
      
      # Write shapefile to tmp dir
      rgdal::writeOGR( sldf, dsn="tmp", layer=paste0("AIS_polyline_",name), driver="ESRI Shapefile")
      
      
      # Point shapefile
      spdf <- SpatialPointsDataFrame(cbind(trackDf$lon,trackDf$lat),data=trackDf)
      
      spdf@proj4string <- CRS("+proj=longlat +datum=WGS84 +elips=WGS84")
      
      rgdal::writeOGR(spdf, dsn="tmp", layer=paste0("AIS_points_",name), driver="ESRI Shapefile")
      
      # waypoint shapefile
      wypnts <- SpatialPointsDataFrame(waypoints,data=data.frame(id=1:nrow(waypoints)))
      
      wypnts@proj4string <- CRS("+proj=longlat +datum=WGS84 +elips=WGS84")
      
      rgdal::writeOGR(wypnts, dsn="tmp", layer=paste0("AIS_waypoints_",name), driver="ESRI Shapefile")
      
      
      # Zip tmp dir to file named the output from filename
      zip(zipfile=file, files=Sys.glob("tmp/*"))
    },
    
    # Download file type
    contentType = "application/zip"
  )
  
  # Download cells shapefile on click
  output$downloadCells <- downloadHandler(
    
    shapefilePrefix <- paste0('band5Traffic-',gsub(':','-',Sys.time())),
    
    # Download file name
    filename = function() {
      
      paste0(shapefilePrefix,'.zip')
      
    },
    
    # Download file content
    content = function(file) {

      cells <- cells()
      
      # Clean tmp directory (if it exists)
      if (length(Sys.glob("tmp/*"))>0){
        file.remove(Sys.glob("tmp/*"))
      }
      
      
      # Point shapefile
      spdf <- SpatialPointsDataFrame(cbind(cells$lon,cells$lat),data=cells)
      
      spdf@proj4string <- CRS("+proj=longlat +datum=WGS84 +elips=WGS84")
      
      rgdal::writeOGR(spdf, dsn="tmp", layer="band5cells", driver="ESRI Shapefile")
      
      
      # Zip tmp dir to file named the output from filename
      zip(zipfile=file, files=Sys.glob("tmp/*"))
    },
    
    # Download file type
    contentType = "application/zip"
  )
  
  # Download shapefile on click
  output$downloadRoutes <- downloadHandler(
    filename = function() {
      paste0('Routes_',input$rdo_inout,'-',id,'_',gsub(' ','_',enc_names$name[enc_names$cell==id]),".csv")
    },
    content = function(file) {
      write.csv(linkeddf, file, row.names = FALSE)
    }
  )
}

# shinyApp(ui, server)