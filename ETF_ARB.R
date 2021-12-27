require("httr");require("dplyr");require("purrr");require("data.table");require("rvest");require("quantmod")
require("pbapply")
# ********************************************************************************************************
#                                                     getConstituents
# ********************************************************************************************************
# ticker
ticker = "IWM"
# function to get constituents from barChart
getConstituents = function(ticker){
# page url
pg <- html_session(paste0("https://www.barchart.com/etfs-funds/quotes/",ticker,"/constituents"))
# save page cookies
cookies <- pg$response$cookies
# Use a named character vector for unquote splicing with !!!
token <- URLdecode(dplyr::recode("XSRF-TOKEN", !!!setNames(cookies$value, 
                                                           cookies$name)))
# get data by passing in url and cookies
pg <- 
  pg %>% rvest:::request_GET(
    paste0("https://www.barchart.com/proxies/core-api/v1/EtfConstituents?",
           "composite=",ticker,"&fields=symbol%2CsymbolName%2Cpercent%2CsharesHeld%2C",
           "symbolCode%2CsymbolType%2ClastPrice%2CdailyLastPrice&orderBy=percent",
           "&orderDir=desc&meta=field.shortName%2Cfield.type%2Cfield.description&",
           "page=1&limit=10000&raw=1"),
    config = httr::add_headers(`x-xsrf-token` = token)
  )

# raw data
data_raw <- httr::content(pg$response)
# convert into a data table
data <- rbindlist(lapply(data_raw$data,"[[",6), fill = TRUE, use.names = TRUE) %>% suppressWarnings()
# subset stocks only 
data = subset(data,data$symbolType == 1)
# trim data frame
data = data[,1:3]
# format percentages
data$percent <- as.numeric(data$percent)/100
# sort by weight
data = data[order(data$percent, decreasing = TRUE),]
# return data frame
data
}
# test function
ETF = getConstituents(ticker=ticker)
# ******************************************************************************************
# function to get stock data from database
getStockData = function(ticker){

  # database name based on dates/find the nearest Sunday
  daytoday <- format(Sys.Date(), "%Y%m%d")
  if(weekdays(as.Date(daytoday,format="%Y%m%d")) == "Friday")
  {
    Friday <- as.Date(daytoday,format="%Y%m%d")
  }else{
    Friday <- as.Date(daytoday,format="%Y%m%d")+7
  }
  while(weekdays(Friday) != "Friday") 
  {
    Friday <- Friday - 1
  }
  lastWeek <- format(Friday - 0:5, "%Y-%m-%d")
  # establish connection
  driver = dbDriver("SQLite")
  con = dbConnect(driver, dbname = paste0("/Volumes/3TB/SQLite/","20211212",#format(as.Date(lastWeek[6]),"%Y%m%d"),
                                          "_getSymbols.db"))
  # get data
  df <- data.table(dbGetQuery(con,paste0("SELECT * FROM getSymbols WHERE Symbol='",ticker,"'")))
  # convert to XTS
  df = xts(df[,c("Close")], order.by = as.Date(df$Date, origin="1970-01-01"))
  # colnames
  colnames(df) = ticker
  # return XTS
  df
}

# require data from data base
dta = pblapply(as.list(unique(c(ticker, ETF$symbol))), function(x){
  df = try(getStockData(ticker=x),silent = TRUE)
  df
})
# combine data
dta = do.call(merge,dta)
dta = dta["2021"]
# ******************************************************************************************
###  yahoo finance alternative
# e <- new.env()
# getSymbols(c(ticker, ETF$symbol[1:50]), from="2020-12-30", env = e)
# dta = do.call(merge,eapply(e,Ad))
# colnames(dta) = gsub(".Adjusted","",names(dta))
# *********************************************************************************************************
#                            Reading in Historical Market Cap Data
# *********************************************************************************************************
FILES = list.files("/Volumes/6TB/RH_API/FUNDAMENTALS",full.names = TRUE)
FILES = pblapply(as.list(FILES), function(x){
  #print(x)
  FILE = try(readRDS(x), silent = TRUE)
  if(!inherits(FILE,'try-error')){
  # extract columns we need
  FILE = na.omit(FILE[,c("symbol","market_cap","market_date")])
  # extract date
  DATE = na.omit(unique(FILE$market_date))[1]
  # transpose data frame
  FILE = t(FILE[,1:2])
  # format column names
  colnames(FILE) = FILE[1,]
  # as data frame
  FILE = as.data.frame(FILE)
  # drop first column
  FILE = FILE[2,]
  # convert to XTS
  FILE = xts(FILE, order.by = as.Date(DATE,format="%Y-%m-%d"))
  }else{
    FILE <- NULL
  }
  # return XTS
  FILE
})
# saveRDS(FILES,"mktCapsHist.rds")
# FILES = readRDS("mktCapsHist.rds")
# function to call tickers needed instead of rbinding everything
getMKTCAP = function(ticker,FILES)
{
  mktCap = lapply(as.list(1:length(FILES)),function(ii){
    mc = FILES[[ii]]
    N = which(names(mc) == ticker)
    if(length(N) == 0)
    {
      mc = NULL
    }else{
      mc = mc[,N]
    }
  
    mc
  })
  # exclude empty list
  mktCap = mktCap[lapply(mktCap,length)>0]
  if(length(mktCap) != 0){
  # rbind list
  mktCap = do.call(rbind,mktCap)
  # fix duplicate date issues
  mktCap = make.index.unique(mktCap,drop = TRUE)
  # reclass - make sure it is numeric
  mktCap = reclass(as.numeric(coredata(mktCap)),match.to = mktCap)
  # assign a column name
  colnames(mktCap) = ticker
  }else{
    mktCap = NULL
  }
  # return XTS
  mktCap
}
# pass in all the constituents to get historical mktCaps
mktCaps = pblapply(as.list(unique(ETF$symbol)), function(x){
  print(x)
  getMKTCAP(ticker=x, FILES = FILES)
})
# eliminate NULLs
mktCaps = mktCaps[lapply(mktCaps, length)>0]
# merge all data
mktCaps = do.call(merge,mktCaps)
# approximate NAs for MktCaps
mktCaps = na.approx(mktCaps)
mktCaps = mktCaps["::20211217"]
# saveRDS(mktCaps, "mktCapsSPY.rds")
# mktCaps = readRDS("mktCapsSPY.rds")
# saveRDS(mktCaps, "mktCapsIWM.rds")
# mktCaps = readRDS("mktCapsIWM.rds")
# saveRDS(mktCaps, "mktCapsQQQ.rds")
# mktCaps = readRDS("mktCapsQQQ.rds")
# saveRDS(mktCaps, "mktCapsXLE.rds")
# mktCaps = readRDS("mktCapsXLE.rds")

toPlot = mktCaps[.endpoints(mktCaps,on="months"),1:5]
chart_Series(toPlot[,1],name = names(toPlot[,1]))
add_TA(toPlot[,2],on=1, col = "red")
add_TA(toPlot[,3],on=1, col = "blue")
add_TA(toPlot[,4],on=1, col = "black")
add_TA(toPlot[,5],on=1, col = "green")
addLegend("topleft",on=0,legend.names = names(toPlot),lty=c(1,1,1,1), lwd=c(1,1,1,1))
# *********************************************************************************************************
#                            Function to get the top-N stocks by mktCap each day
# *********************************************************************************************************
# subset available data only
IDX = dta[,1]
dta = pblapply(as.list(names(mktCaps)), function(ticker){
  N = which(names(dta) == ticker)
  if(length(N) == 1){
    tmp = dta[,N]
  }else{
    tmp = NULL
  }
  tmp
})
dta = dta[lapply(dta, length)>0]
dta = do.call(merge,dta)
dta = merge(IDX,dta)
dta = na.locf(dta)
# calculate Daily, Weekly, Monthly Returns
dRETS = ROC(dta,type="discrete")
wRETS = do.call(merge,pblapply(as.list(names(dta)),function(x){weeklyReturn(dta[,x])}))
colnames(wRETS) <- names(dta)
mRETS = do.call(merge,pblapply(as.list(names(dta)),function(x){monthlyReturn(dta[,x]["202102::"])}))
colnames(mRETS) <- names(dta)

# mktCaps: mktCaps data.frame 
# topN   : number of stocks to include
# freq   : 'D' for Daily, 'W' for Weekly, 'M' for Monthly
rankMktCap = function(mktCaps,topN,freq){
  DAYS   = unique(index(mktCaps))
 if(freq=="D"){
   RETS = dRETS
   df = lapply(as.list(1:(length(DAYS)-1)), function(ii){
   idx = DAYS[ii]
   # extract marketCaps for that day
   df = as.data.frame(t(mktCaps[idx]))
   # sort
   #df = data.frame(df[order(df[,1],decreasing = TRUE),],row.names = row.names(df))
   df = df[order(df[,1],decreasing = TRUE),,drop=FALSE]
   NOMS = row.names(df)[1:topN]
   df =  lapply(as.list(NOMS), function(x){
     tmp0 = try(RETS[index(RETS)[ii+1],x],silent = TRUE)
     if(!inherits(tmp0, 'try-error')) tmp0
   })
   df = df[lapply(as.list(df), length)>0]
   df =  do.call(merge, df)
   NOMS = names(df)
   df = as.data.frame(rowMeans(df))
   df = cbind(idx,df,paste(NOMS,collapse = "-"))
   df
 })}
 if(freq=="W"){
   DAYS =  as.Date(DAYS[weekdays(DAYS) == "Friday"])
   RETS = wRETS
   # subset mktCaps to 'Weekly'
   mktCaps = mktCaps[as.Date(index(wRETS))]
   df = lapply(as.list(1:(length(DAYS)-1)), function(ii){
   idx = DAYS[ii]
   # extract marketCaps for that day
   df = as.data.frame(t(mktCaps[idx]))
   # sort
   #df = data.frame(df[order(df[,1],decreasing = TRUE),],row.names = row.names(df))
   df = df[order(df[,1],decreasing = TRUE),,drop=FALSE]
   NOMS = row.names(df)[1:topN]
   df =  lapply(as.list(NOMS), function(x){
     tmp0 = try(RETS[index(RETS)[ii+1],x],silent = TRUE)
     if(!inherits(tmp0, 'try-error')) tmp0
   })
   df = df[lapply(as.list(df), length)>0]
   df =  do.call(merge, df)
   NOMS = names(df)
   df = as.data.frame(rowMeans(df))
   df = cbind(idx,df,paste(NOMS,collapse = "-"))
   df
   })
 }
  if(freq=="M"){
    RETS = mRETS
    DAYS   = unique(index(RETS))
    # subset mktCaps to 'Monthly'
    #mktCaps = mktCaps[as.Date(index(mRETS))]
    mktCaps = mktCaps[.endpoints(mktCaps, on = "months"),]
    mktCaps = mktCaps["202102::"]
    df = lapply(as.list(1:(length(DAYS)-1)), function(ii){
      idx = DAYS[ii]
      
      # extract marketCaps for that day
      df = as.data.frame(t(mktCaps[index(mktCaps)[ii]]))
      # sort
      #df = data.frame(df[order(df[,1],decreasing = TRUE),],row.names = row.names(df))
      df = df[order(df[,1],decreasing = TRUE),,drop=FALSE]
      NOMS = row.names(df)[1:topN]
      df =  lapply(as.list(NOMS), function(x){
        tmp0 = try(RETS[index(RETS)[ii+1],x],silent = TRUE)
          if(!inherits(tmp0, 'try-error')) tmp0
      })
      df = df[lapply(as.list(df), length)>0]
      df =  do.call(merge, df)
      NOMS = names(df)
      df = as.data.frame(rowMeans(df))
      df = cbind(idx,df,paste(NOMS,collapse = "-"))
      df
    })
  }
  # rbind results
 df = rbindlist(df)
 # format column names
 colnames(df) = c("Date","Return","Tickers")
 # XTS object
 XTS = make.index.unique(xts(df$Return,order.by=as.Date(df$Date, "%Y-%m-%d")), drop = TRUE)
 # return list
 list(XTS,df)
}
# *********************************************************************************************************
#                                                     Daily
# *********************************************************************************************************
RES_03 = rankMktCap(mktCaps = mktCaps, topN=3, freq = "D")
RES_05 = rankMktCap(mktCaps = mktCaps, topN=5, freq = "D")
RES_10 = rankMktCap(mktCaps = mktCaps, topN=10, freq = "D")
RES_15 = rankMktCap(mktCaps = mktCaps, topN=15, freq = "D")
BM     = xts(coredata(dRETS[,1]), order.by = as.Date(index(dRETS[,1])))
rets = merge(RES_03[[1]], RES_05[[1]],RES_10[[1]],RES_15[[1]],BM)
colnames(rets) = c("Top03","Top05","Top10","Top15","BM")
charts.PerformanceSummary(rets, geometric = FALSE)
unique(RES_03[[2]]$Tickers)
unique(RES_05[[2]]$Tickers)
unique(RES_10[[2]]$Tickers)
unique(RES_15[[2]]$Tickers)
# *********************************************************************************************************
#                                                     Weekly
# *********************************************************************************************************
RES_03 = rankMktCap(mktCaps = mktCaps, topN=3, freq = "W")
RES_05 = rankMktCap(mktCaps = mktCaps, topN=5, freq = "W")
RES_10 = rankMktCap(mktCaps = mktCaps, topN=10, freq = "W")
RES_15 = rankMktCap(mktCaps = mktCaps, topN=15, freq = "W")
BM     = xts(coredata(wRETS[,1]), order.by = as.Date(index(wRETS[,1])))
rets = merge(RES_03[[1]], RES_05[[1]],RES_10[[1]],RES_15[[1]],BM)
colnames(rets) = c("Top03","Top05","Top10","Top15","BM")
charts.PerformanceSummary(rets, geometric = FALSE)

unique(RES_03[[2]]$Tickers)
unique(RES_05[[2]]$Tickers)
unique(RES_10[[2]]$Tickers)
unique(RES_15[[2]]$Tickers)
# *********************************************************************************************************
#                                                     MONTHLY
# *********************************************************************************************************
RES_03 = rankMktCap(mktCaps = mktCaps, topN=3, freq = "M")
RES_05 = rankMktCap(mktCaps = mktCaps, topN=5, freq = "M")
RES_10 = rankMktCap(mktCaps = mktCaps, topN=10, freq = "M")
RES_15 = rankMktCap(mktCaps = mktCaps, topN=15, freq = "M")
BM     = xts(coredata(mRETS[,1]), order.by = as.Date(index(mRETS[,1])))
rets = merge(RES_03[[1]], RES_05[[1]],RES_10[[1]],RES_15[[1]],BM)
colnames(rets) = c("Top03","Top05","Top10","Top15","BM")
charts.PerformanceSummary(rets, geometric = FALSE)

unique(RES_03[[2]]$Tickers)
unique(RES_05[[2]]$Tickers)
unique(RES_10[[2]]$Tickers)
unique(RES_15[[2]]$Tickers)
# *********************************************************************************************************
#                                             get Next Set of Tickers
# *********************************************************************************************************
# mktCaps: mktCaps data.frame 
# topN   : number of stocks to include
# freq   : 'D' for Daily, 'W' for Weekly, 'M' for Monthly
getNextTickers = function(mktCaps,topN,freq){

  DAYS   = unique(index(mktCaps))
  if(freq=="D"){
      RETS = dRETS
      ii = length(DAYS)
      idx = DAYS[ii]
      # extract marketCaps for that day
      df = as.data.frame(t(mktCaps[idx]))
      # sort
      #df = data.frame(df[order(df[,1],decreasing = TRUE),],row.names = row.names(df))
      df = df[order(df[,1],decreasing = TRUE),,drop=FALSE]
      NOMS = row.names(df)[1:topN]
      df = cbind(paste(idx),paste(NOMS,collapse = "-"))
      df
    }
  if(freq=="W"){
    DAYS =  as.Date(DAYS[weekdays(DAYS) == "Friday"])
    RETS = wRETS
    # subset mktCaps to 'Weekly'
    mktCaps = mktCaps[as.Date(index(wRETS))]
    ii= length(DAYS)
    idx = DAYS[ii]
    # extract marketCaps for that day
    df = 
    # sort
    #df = data.frame(df[order(df[,1],decreasing = TRUE),],row.names = row.names(df))
    df = df[order(df[,1],decreasing = TRUE),,drop=FALSE]
    NOMS = row.names(df)[1:topN]
    df = cbind(paste(idx),paste(NOMS,collapse = "-"))
    df
    }
  
  if(freq=="M"){
    RETS = mRETS
    DAYS   = unique(index(RETS))
    # subset mktCaps to 'Monthly'
    #mktCaps = mktCaps[as.Date(index(mRETS))]
    mktCaps = mktCaps[.endpoints(mktCaps, on = "months"),]
    mktCaps = mktCaps["202102::"]
    ii = length(DAYS)
    idx = DAYS[ii]
    
    # extract marketCaps for that day
    df = as.data.frame(t(mktCaps[index(mktCaps)[ii]]))
    # sort
    #df = data.frame(df[order(df[,1],decreasing = TRUE),],row.names = row.names(df))
    df = df[order(df[,1],decreasing = TRUE),,drop=FALSE]
    NOMS = row.names(df)[1:topN]
    df = cbind(paste(idx),paste(NOMS,collapse = "-"))
    df
  }
  
  # format column names
  colnames(df) = c("Date","Tickers")
  
  # return list
  df

}


getNextTickers(mktCaps = mktCaps, topN = 10, freq = "D")
getNextTickers(mktCaps = mktCaps, topN = 10, freq = "W")
getNextTickers(mktCaps = mktCaps, topN = 10, freq = "M")

