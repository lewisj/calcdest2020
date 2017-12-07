


shinyUI(
  bootstrapPage(
    shinyjs::useShinyjs(),
    tagList(tags$head(
      tags$link(rel="stylesheet", type="text/css",href="styles.css"),
      tags$script(type="text/javascript", src = "busy.js")
    )),
    fluidPage(
      
      p(),
      sidebarLayout(
        # input tabs on the left
        sidebarPanel(width=3,
                     selectInput('slt_VT','Vessel Type',c('Tankers','Dry Cargo/Passenger','Bulk Carriers','Fishing', 'All Vessel Types'),
                                 selected = 'Tankers'),
                     sliderInput('sld_tons','Gross Tonnage',min=0,max=250e3,step = 1e3,value = c(2e3,600e3)),
                     # plotOutput('tonnagePlot',width = 250,height = 150)
                     sliderInput('sld_date',label = 'Date range',
                                 min = min(tjoin$exitTimeStartCell),
                                 max = max(tjoin$exitTimeStartCell),
                                 value = c(min(tjoin$exitTimeStartCell),
                                           max(tjoin$exitTimeStartCell))
                     ),
                     h4(uiOutput('nrows')),
                     hr(),
                     sliderInput('sld_topn',label = 'Display top n cells',min = 1,max = 1000,value=800),
                     checkboxInput('chk_onlyShowTopNCncts',label = 'Only display connections to these cells',value = T),
                     radioButtons('rdo_inout',choices = c('inbound','outbound'),label = 'Traffic',selected = 'outbound',inline = T) %>% shinyjs::disabled(),
                     sliderInput('sld_min',label = 'Minimumm traffic',min = 1,max = 100,value=25),
                     # sliderInput('sld_TravelTime',label = 'Journey time (days) >=',min = 0,max = 5,value=1,step = 0.5)
                     checkboxInput('removeStartEndSameCountry', 
                                   'Remove routes starting/ending in same country'),
                     checkboxInput('chk_customerOnly',label = 'UKHO customers (2016) only',value = F),
                     sliderInput('sld_transitCell',label = 'Ignore cells if hours inside <=',min = 0,max = 10,value=5,step =1),
                     # downloadButton("downloadShp", "Download Cluster Shapefile Zip", class="btn-block"),
                     tags$a(HTML(paste(icon('download'))),target='_blank',
                            class="btn btn-default shiny-download-link btn-primary shinyjs-disabled",
                            href="",id="downloadShp"),
                     tags$a(HTML(paste(icon('map-marker'))),target='_blank',
                            class="btn btn-default shiny-download-link btn-primary",
                            href="",id="downloadCells"),
                     tags$a(HTML(paste(icon('table'))),target='_blank',
                            class="btn btn-default shiny-download-link btn-primary shinyjs-disabled",
                            href="",id="downloadRoutes"),
                     p(),
                     actionButton('resetQuery', 'Reset Query',width = '150px',
                                  class="btn btn-block btn-success"),
                     bsTooltip(id = "downloadCells", title = "Download band 5 cell selection", 
                               placement = "top", trigger = "hover"),
                     bsTooltip(id = "downloadRoutes", title = "Download routes table", 
                               placement = "top", trigger = "hover"),
                     bsTooltip(id = "downloadShp", title = "Download vessel track", 
                               placement = "top", trigger = "hover")
        ),
        mainPanel(
          
          leafletOutput("mymap",height='600px',width='100%'),
          h4(textOutput('id')),
          tableOutput('cellList'),
          bsModal('mdl_edgeInfo',trigger = 'sdf',size = 'large',
                  title = 'Route Info',
                  fluidRow(
                    column(width=6,
                           uiOutput('routeInfo'),
                           hr(),
                           h4('Duration statistics (days)'),
                           verbatimTextOutput('routeDurationStats'),
                           plotOutput('routeDurationHist',height='200px',width='200px')
                    ),
                    column(width=6,
                           plotOutput('routePlot', brush = brushOpts(
                             id = "plot1_brush")),
                           actionButton('btn_plotTracks',label = 'Plot Tracks',icon = icon('ship'),class="btn-success")%>%disabled()
                    )
                  )
                  # verbatimTextOutput('brush_info'),
          )
        )
      )
    ),
    uiOutput('selectTrackUI'),
    
    div(class = "busy", 
        h4("working..."),
        h2(HTML('<i class="fa fa-cog fa-spin"></i>'))
    )
  )
)
