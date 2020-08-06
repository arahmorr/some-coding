#1.Se cuentan los votos V de c/partido P.
#2.Todos los partidos tienen 0 escaños s asignados. Calculamos para cada partido V(P)/(s+1) con s=0.
#3.Seleccionamos el partido con un cociente más alto y le asignamos un escaño.
#como este partido tendrá un escaño, le calculamos V(P)/(s+1) con s=1.
#4.Seleccionamos el partido con un cociente más alto y le asignamos un escaño.
#si ahora este tiene s escaños, le calculamos V(P)/(s+1) con ese valor de s. 
#repetimos este paso hasta que no queden escaños para repartir.

library(xlsx)
library(dplyr)
library(tidyverse)

df <- read.csv("Votos.csv") #Base

Escanios <- matrix(rep(0,16*52),nrow = 52, ncol = 16) #Forma matriz para llenar con escaños
colnames(Escanios) <- colnames(df[4:19]) #nombre de las columnas: de la base de datos, columnas con *número de votos* por partido


eXp <- function(E,prov, Escanios){
  V<-df$Votos_validos[prov] #guarda los votos válidos de la 1era prov
  p <- (df[prov,4:19]/V)[which(df[prov,4:19] >= 0.03)] #guarda los partidos que tengan votos/votos válidos = porcentaje mayor o igual a 3%
  S = c(rep(0,length(p))) #vector de estaños
  C = c(rep(0,length(p))) #vector de cocientes
  P <- colnames(p) #guarda los nombres de esos partidos que podrían obtener escaños
  if(length(p)==0){ #si no hay partidos que junten el 3% de votos:
    for (i in seq_along(S)){
      Escanios[[  prov, P[[i]]  ]] <-  0 #se asignan 0 escaños a cada uno de los partidos en la matriz "Escaños"
    }
  }
  else { #si hay por lo menos un partido con más del 3%:
    for (party in 1:length(p)){  #para c/u de esos partidos:
      C[party]=df[,colnames(df)[party+3]][prov] /(S[party]+1) #calcula el cociente de cada uno de los partidos. 
    } #party+i indica que los partidos empiezan en la columna i+1
    while (sum(S) < E){ #mientras la suma sea menor que el número de diputados:
      j <- which.max(C) #j: partido con el máx cociente
      S[j] = S[j]+1 #aumenta 1 estaño al partido j
      C[j] = df[,colnames(df)[j+3]][prov]/(S[j]+1) #se actualiza el cociente del partido j
    }

    for (i in seq_along(S)){
      Escanios[[  prov, P[[i]]  ]] <-  S[i] #fila prov, columna Partido i, le asigna esos escaños
    }
  }
  return(Escanios)
}


for (prov in 1:52){ #esto corre la función por cada una de las provincias (o municipios)
  dip<-df$Diputados[prov] #toma, de la columna "Diputados" el núm de dip por provincia (o municipio)
  Escanios = eXp(dip,prov,Escanios) #llama la función tomando (núm de dip, prov,escaños)
}


prov=1
dip<-df$Diputados[prov]
V<-df$Votos_validos[prov]
p <- (df[prov,125:140]/V)[which(df[prov,125:140] >= 0.03)]
S = c(rep(0,length(p)))
C = c(rep(0,length(p)))
P <- colnames(p)

  for (party in 1:length(p)){  
    #party=2 E=dip
    C[party]=df[,colnames(df)[party+124]][prov] /(S[party]+1)
  }
  while (sum(S) < E){
    j <- which.max(C)
    S[j] = S[j]+1
    C[j] = df[,colnames(df)[j+124]][prov]/(S[j]+1)
  }
  
  for (i in seq_along(S)){
    i=1
    Escanios[[  prov, P[[i]]  ]] <-  S[i]
  }

Partidos <- c("PODEMOS","PP","PSOE","ECP","IU","Cs","VOX","EQUO","COMPROMIS","PNV","CCa","EH_Bildu","ANOVA","UPyD","PACMA","P_LIB")

Escanios <- as.data.frame(Escanios)

colnames(Escanios) <- Partidos

write.csv(Escanios,"EscaniosXPartido.csv" )
