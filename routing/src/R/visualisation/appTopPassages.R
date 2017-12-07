library(shiny)
library(leaflet)

# TODO ST
# - add mean time spent in destcell
# - filter out 'transit' cells
# - page layout
# - line scale
# - marker scale

# TODO LT
# - Final Destination
# - Robinson Project
# - 

ui <- fluidPage(
  leafletOutput("mymap",height='600px'),
  
  fluidRow(
    column(width=3,
           selectInput('slt_VT','Vessel Type',c('Tankers','Dry Cargo/Passenger','Bulk Carriers'),
                       selected = "Tankers"),
           sliderInput('sld_tons','Gross Tonnage',min=1e3,max=600e3,step = 1e3,value = c(2e3,600e3)),
           h4(textOutput('nrows')),
           plotOutput('tonnagePlot',width = 250,height = 150)
    ),
    column(width=3,
           uiOutput('UI_selectStartEnd')

    ),
    column(width=6,
           tableOutput('route')
           

    )#,
    # column(width=3
    # )
  )
  
)


server <- function(input, output, session) {
  
  
  
  data <- reactive({
    print(input$slt_VT)
    if(!is.null(input$slt_VT)){
      
      print(input$sld_date)
      
      tjoin %>%
        filter(!is.na(ihsShipTypel2)&ihsShipTypel2==input$slt_VT,
               tons>=input$sld_tons[1]&tons<=input$sld_tons[2]
        )
    } else {tjoin}
    
  })
  
  output$nrows <- renderText({
    paste(nrow(data()),'records')
  })
  
  ports <- reactive({
    # if(input$rdo_inout=='inbound'){
      
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
      topDests$top400 <- 'out'
      topDests$top400[1:250] <- 'in'
      topDests#'[1:input$sld_topn,]
      
    # } else {
    #   
    #   topArrives <- data() %>% 
    #     group_by(startCell,startLat,startLon) %>%
    #     summarise(n = n(),
    #               n_mmsi = n_distinct(mmsi)) %>%
    #     arrange(desc(n_mmsi)) %>%
    #     mutate(code=str_sub(startCell,0,2)) %>%
    #     rename(cell=startCell,
    #            lat=startLat,
    #            lon=startLon) %>%
    #     left_join(enc_names,by='cell')
    #   topArrives$top400 <- 'out'
    #   topArrives$top400[1:250] <- 'in'
    #   topArrives#[1:input$sld_topn,]
    # }
    
    
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
        overlayGroups = c("edges", "cells",'Tanker AIS'),
        options = layersControlOptions(collapsed = FALSE)
      ) %>%
      addProviderTiles(providers$Esri.WorldStreetMap) %>%
      addMiniMap() %>%
      addTiles() %>%
      hideGroup('Tanker AIS')
    
  })
  
  
  observe({


    leafletProxy('mymap') %>% clearMarkers()
    leafletProxy('mymap') %>% clearShapes()

    pal <- colorFactor(c("red",'navy'), domain = c('in','out'))
    # pal <- colorNumeric('Spectral',reverse = T, log10(ports()$n_mmsi))
    # pal <- colorQuantile('Spectral',reverse = T, ports()$n_mmsi)

    leafletProxy('mymap',data=ports()) %>%
      addCircles(~lon, ~lat,group = 'cells',
                 label = ~paste0(cell,': ',name),
                 # popup = ~n_mmsi,
                 layer = ~cell,
                 # fillColor=~pal((n_mmsi)),
                 fillColor =~pal(top400),
                 radius =2e3,
                 stroke=T,
                 # color =~pal((n_mmsi)),
                 color =~pal(top400),
                 fillOpacity = 0.8)


  })
  
  
  el <- reactive({
    data()%>%
      mutate(distkm = distGeo(p1=select(.,startLon,startLat),p2=select(.,endLon,endLat))/1e3) %>%
      mutate(speedKnts = (distkm/(duration*24))*0.539957) %>%
      filter(speedKnts<30) %>%
      group_by(startCell,endCell) %>%
      summarise(duration=mean(duration,na.rm=T),
                startLon = first(startLon),
                startLat = first(startLat),
                endLon = first(endLon),
                endLat = first(endLat),
                n_mmsi = n_distinct(mmsi)) %>%
      filter(n_mmsi>2) %>%
      ungroup()
    
  })
  
  
  celldf <- reactive({
    tibble(cell = unique(c(el()$startCell,el()$endCell))) %>%
      mutate(nodeid=1:nrow(.)) %>%
      left_join(portLocs,by=c('cell'='X1')) %>%
      left_join(enc_names,by='cell')
  })
  
  
  observe({
    s <- (input$slt_start)
    print(s)
  })
  
  route <- NULL
  makeReactiveBinding('route')
  observe({
    
    s <- (input$slt_start)
    e <- (input$slt_end)
    
    inTest <- function(x) !is.null(x)&&length(x)&&x!=''
    
    if(inTest(s)&&inTest(e)){
      
    celldf <- isolate(celldf())
    el <- isolate(el())
    
    s <- as.numeric(s)
    e <- as.numeric(e)
    
    elm <- left_join(el,celldf,by=c('startCell'='cell')) %>%
      rename(source=nodeid) %>%
      left_join(.,celldf,by=c('endCell'='cell')) %>%
      rename(target=nodeid) %>%
      select(source,target,duration) %>%
      as.matrix()
    

    g2 <- add_edges(make_empty_graph(nrow(celldf)), t(elm[,1:2]), weight=elm[,3])
    
    sp <- shortest_paths(g2, s, e)
    
    route <<- tibble(nids =as.vector(sp$vpath[[1]])) %>%
      left_join(celldf,by=c('nids'='nodeid'))
    
    
    }
    
  })

  output$UI_selectStartEnd <- renderUI({
    
    
    choices = celldf()$nodeid %>%
      setNames(.,paste(celldf()$name))
    tagList(
    selectizeInput('slt_start',choices=choices,label='Start Cell'),
    selectizeInput('slt_end',choices=choices,label='End Cell')
    )
    
    
  })
  

  output$route <- renderTable({
    route
  })
  
  observeEvent(route,{
    
    leafletProxy('mymap') %>% 
      clearGroup('edges') %>%
      addPolylines(lng = route$V1,lat = route$V2,group = 'edges')
    
    
    
  })

  
  output$tonnagePlot <- renderPlot({
    data() %>%
      ggplot()+geom_histogram(aes(x=tons))+ylab('')

  })
  
  
}

shinyApp(ui, server)