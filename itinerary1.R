library(dplyr)
library(lubridate)
library(tidyr)
library(readr)
library(stringi)
library(gdata)

# Creación itinerario -----------------------------------------------------

it<-read_csv("SSIM_12.csv")
it<-it[,-c(1)]
it<-distinct(it)
it<-it[,-c(1,2,4,5,6,13)] 
it <- it %>% mutate(DepartureTime=format(strptime(DepartureTime, "%I:%M:%S %p"),
                                         format="%H:%M:%S"))
it <- it %>% mutate(ArrivalTime=format(strptime(ArrivalTime, "%I:%M:%S %p"),
                                       format="%H:%M:%S"))
it <- it %>% mutate(DepartureTime=as.POSIXct(DepartureTime, format="%H:%M:%S"))
it <- it %>% mutate(ArrivalTime=as.POSIXct(ArrivalTime, format="%H:%M:%S"))


# Función GetLinea --------------------------------------------------------

getlinea <- function(dia)
{
  d <- it %>% filter(date==dia) 
  
  d$Seq <- 0
  d$Linea <-0
  
  l=0 
  
  i<-which(d$Linea==0)[1]
  
  while(!is.na(i))
  {
    l=l+1
    s=1 
    i<-which(d$Linea==0)[1] 
    d$Seq[i]<-s 
    d$Linea[i]<-l 
    idP=d$flight_number_[i]  #indice pasado
    fnn<-which(d$flight_number==idP) #indice actual
    fn2<-which(d$flight_number==d$flight_number_[fnn]) #indice siguiente
    if (!identical(fn2,integer(0)) & !identical(fnn,integer(0)) &
        !(identical(abs(difftime(d$DepartureTime[fn2],
                                 d$ArrivalTime[fnn],units="hours"))<2,logical(0)) | 
          identical(abs(difftime(d$DepartureTime[fn2],
                                 d$ArrivalTime[fnn],units="hours"))<2,FALSE)))
    {
      while (!identical(fnn,integer(0)) &
             !(identical(abs(difftime(d$DepartureTime[fn2],
                                      d$ArrivalTime[fnn],units="hours"))<2,logical(0)) | 
               identical(abs(difftime(d$DepartureTime[fn2],
                                      d$ArrivalTime[fnn],units="hours"))<2,FALSE)))
      {
        fnn<-which(d$flight_number==idP) 
        d$Seq[fnn]<-s+1
        s=s+1
        idP<-d$flight_number_[fnn]
        d$Linea[fnn]<-l
        fn2<-which(d$flight_number==d$flight_number_[fnn])
      }
    } else {
      d$Seq[fnn]<-s+1
      s=s+1
      idP<-d$flight_number_[fnn]
      d$Linea[fnn]<-l
    }
  }
  return(d)
}


# Creación itinerario 1 ---------------------------------------------------


dia1 <- it$date[1] # dia1
IT <- getlinea(dia1)

for (idia in 3:8) # idia=3
{
  dia <- sort(unique(it$date))[idia]
  LD<-getlinea(dia)
  IT<-rbind2(IT,LD)
}

# Hora central -> Hora local ----------------------------------------------

IT <- IT %>% mutate(DepartureTime=as.POSIXct(DepartureTime)
                    +utc_local_time_variation_departure*60*60)
IT <- IT %>% mutate(DepartureTime=format(as.POSIXct(DepartureTime),format="%H:%M:%S"))
IT <- IT %>% mutate(DepartureTime=hms(DepartureTime))
IT <- IT %>% mutate(ArrivalTime=as.POSIXct(ArrivalTime)
                    +utc_local_time_variation_arrival*60*60)
IT <- IT %>% mutate(ArrivalTime=format(as.POSIXct(ArrivalTime),format="%H:%M:%S"))
IT <- IT %>% mutate(ArrivalTime=hms(ArrivalTime))


# Data histórica ----------------------------------------------------------

{myServer <- "vbmxsvrinfomgt.database.windows.net"
myUser <- "usrAxeleratum"
myPassword <- "Mut4r3l3xA+"
myDatabase <- "vbmxods-infomgt"
myDriver <- "SQL Server" 

connectionString <- paste0(
  "Driver=", myDriver, 
  ";Server=", myServer, 
  ";Database=", myDatabase, 
  ";Uid=", myUser, 
  ";Pwd=", myPassword)
conn <- odbcDriverConnect(connectionString)
}
flights <- " 
SELECT TOP (220000) [IdF]
      ,[SectorKey]
      ,[Flight]
      ,convert(nvarchar(19), Dep, 21)Dep
      ,[ActFrom]
      ,[ActTo]
      ,convert(nvarchar(19), Start, 21)Start
      ,[Rego]
      ,[Rango Demora 015 OTP]
  FROM [dbo].[vwFlights_Ax] WHERE [START] IS NOT NULL AND 
[Tipo de Servicio]='Scheduled Service' ORDER BY Dep DESC" 

f <- sqlQuery(conn, flights)

f<-f%>%
  mutate(Dep=as.POSIXct(Dep,format='%Y-%m-%d %H:%M:%S'))
f<-f%>%
  mutate(Start=as.POSIXct(Start,format='%Y-%m-%d %H:%M:%S'))

f <- f %>% drop_na() #drop NAs rows
f<-f%>% mutate(ActFrom=stri_replace_all_fixed(ActFrom, " ", ""))

times<-read.csv("times.csv")
f<-merge(f,times,by.x="ActFrom",by.y="Origen",all.x = TRUE)
f <- f%>%mutate(Dep=Dep+DifMex*60*60) 
f <- f%>%mutate(Start=Start+DifMex*60*60) 

delays <- 
  "SELECT TOP (190000) [IdF]
      ,[SectorKey]
      ,[DelayGroup]
      ,[Airlineidentifier]
      ,[Code]
      ,[Tipo]
      ,[minutos]
  FROM [dbo].[vwDelay_Ax] WHERE [Tipo de Demora] = 'NO CONTROLABLE' AND 
  [Tipo] = 'OTP'AND [Code]=87 OR [Code] =183 OR [Code]=184
ORDER BY Dep DESC" 

d <- sqlQuery(conn, delays)

#hasta aquí
# No warnings -------------------------------------------------------------

d<-d%>% mutate(SectorKey=as.character(SectorKey))
f<-f%>% mutate(SectorKey=as.character(SectorKey))

delFin<-inner_join(d,f) # gets Tipo de Servicio = "Scheduled Service"
                         # y añade Origen (ActFrom)

# DataFiltered ------------------------------------------------------------

delFin<-delFin%>%
  mutate(Dia=yday(Dep)) 
delFin<-delFin%>%
  mutate(Hora=hour(Dep)) 
delFin<-delFin%>%
  rename(Origen=ActFrom,Destino=ActTo)

delFin <- delFin %>% filter((Dia>=242 & Dia<=350) | (Dia<=184 & Dia>=10))

delFin<-delFin%>%
  mutate(Horario=Hora)
n<-nrow(delFin)
for(i in 1:n){
  if(delFin$Hora[i]>=0 && delFin$Hora[i]<=9){
    delFin$Horario[i]=1
  }else if (delFin$Hora[i]>=10 && delFin$Hora[i]<=11){
    delFin$Horario[i]=3
  }else if (delFin$Hora[i]>=12 && delFin$Hora[i]<=14){
    delFin$Horario[i]=4
  }else if (delFin$Hora[i]>=15 && delFin$Hora[i]<=16){
    delFin$Horario[i]=5
  }else if (delFin$Hora[i]>=17 && delFin$Hora[i]<=19){
    delFin$Horario[i]=6
  }else if (delFin$Hora[i]>=20 && delFin$Hora[i]<=23){
    delFin$Horario[i]=7
  }
}

# Preparación data histórica flights ----------------------------------------------

f<-f%>%
  mutate(Dia=yday(Dep)) 
f<-f%>%
  mutate(Hora=hour(Dep)) 
f<-f%>%
  rename(Origen=ActFrom,Destino=ActTo)

f<-f%>%
  mutate(Horario=Hora)
n<-nrow(f)
for(i in 1:n){
  if(f$Hora[i]>=0 && f$Hora[i]<=9){
    f$Horario[i]=1
  }else if (f$Hora[i]>=10 && f$Hora[i]<=11){
    f$Horario[i]=3
  }else if (f$Hora[i]>=12 && f$Hora[i]<=14){
    f$Horario[i]=4
  }else if (f$Hora[i]>=15 && f$Hora[i]<=16){
    f$Horario[i]=5
  }else if (f$Hora[i]>=17 && f$Hora[i]<=19){
    f$Horario[i]=6
  }else if (f$Hora[i]>=20 && f$Hora[i]<=23){
    f$Horario[i]=7
  }
}

f$minutos <- f$Dep
f$minutos <- difftime(f$Start,f$Dep,units = c("mins"))

# Fin preparación Itinerario ----------------------------------------------

it<-rename(IT)

it<-it%>%
  rename(Origen=departure_station,Destino=arrival_station)

it<-it%>%
  mutate(Hora=hour(DepartureTime))
it<-it[,-c(4,7)]

# Categorías de horarios --------------------------------------------------

it<-it%>%
  mutate(Horario=Hora)
n<-nrow(it)
for(i in 1:n){
  if(it$Hora[i]>=0 && it$Hora[i]<=9){
    it$Horario[i]=1
  }else if (it$Hora[i]>=10 && it$Hora[i]<=11){
    it$Horario[i]=3
  }else if (it$Hora[i]>=12 && it$Hora[i]<=14){
    it$Horario[i]=4
  }else if (it$Hora[i]>=15 && it$Hora[i]<=16){
    it$Horario[i]=5
  }else if (it$Hora[i]>=17 && it$Hora[i]<=19){
    it$Horario[i]=6
  }else if (it$Hora[i]>=20 && it$Hora[i]<=23){
    it$Horario[i]=7
  }
}

fl <- rename(delFin)

it <- it[order(it$date, it$Linea, it$Seq),]

# Asignar base ------------------------------------------------------------

it$Base <- "BASE"
s=1 #índice de la línea 
r=1 #renglón
i=2
iseq=5 # it$Seq de la línea 
while (!identical(iseq!=1,NA)&r<=nrow(it)) {#i<=nrow(it)
  it$Base[r]= it$Origen[which(it$Seq==1)][s] #base n, corresponde al n de línea 
  iseq=it$Seq[r]+1
  while (identical(!is.na(iseq),iseq!=1) &(r+1)<=nrow(it)) {
    it$Base[i]= it$Origen[which(it$Seq==1)][s]
    iseq=it$Seq[i+1]#iseq+1
    i=i+1 #índice de la siguiente línea
  }
  r=i
  s=s+1
}

# # Does it land in MEX? ---------------------------------------------------

it$MEX <- 2 
rs=1 
while (rs<=nrow(it)) 
{ 
  iseq=5                  
  Origenes=c()
  ns=0
  while (iseq!=1 & rs<=nrow(it)) #calcula ns=length(Seq) (is 1 or NA)
  {
    iseq=it$Seq[rs+1]
    ns=ns+1
    rs=rs+1
  }
  r=1
  for (i in (rs-ns):(rs-1)) { 
    Origenes[r]=it$Origen[i]
    r=r+1
  }
  for (ro in (rs-ns):(rs-1)) {
    if ("MEX" %in% Origenes)
    {
      it$MEX[ro]=1
    } else {
      it$MEX[ro]=0
    }  
  }
}

# Firewalls ---------------------------------------------------------------

it$firewall<-3.14159

for(r in 1:n)
{
  O <-it$Origen[r]
  H <- it$Horario[r]
  hO <- delFin %>% 
    filter(Origen==O & Horario==H)
  NO <- nrow(hO)
  
  if(NO==0)
  {
    it$firewall[r]=0
  } else 
  {
    # h16 <- hO 
    if(nrow(hO)!=0) 
    {
      Q3<-quantile(hO$minutos,probs = .85) 
      # outliers must lie outside these boundaries
      sinoutliers<-hO %>% filter(minutos <= Q3)
      sd<-sd(sinoutliers$minutos)
      if (is.na(sd)) {
        sd=0}
      pd <- mean(sinoutliers$minutos) + sd
    } else{
      pd <-0
    }
    
    it$firewall[r]=pd
  }
} 

# Suma de Firewall x línea --------------------------------------------------------

it$fXl <- 0 
# r=1
rs=1 #rs=1478
while (rs<=nrow(it)) 
{ 
  iseq=5                  
  sf=c()
  ns=0
  while (iseq!=1 & rs<=nrow(it)) #calcula ns=length(Seq) (is 1 or NA)
  {
    iseq=it$Seq[rs+1]
    ns=ns+1
    rs=rs+1
  }
  r=1
  sf=sum(it$firewall[(rs-ns):(rs-1)])
  for (i in (rs-ns):(rs-1)) {
    it$fXl[i]=sf
    r=r+1
  }
}
