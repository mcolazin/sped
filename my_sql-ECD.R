# Código para declaração trimestral
# Code for trimestral balance

library(RMySQL)
library(dplyr)
library(xlsx)
library(sqldf)
options(scipen = 999)

# Reading all DBs
# criando conexão com o Mysql DB.
conn <- dbConnect(MySQL(), port=3340, 
                  unix.socket="spedcontabil3340", 
                  username = "spedfiscal",
                  password = "spedfiscal", 
                  host="localhost", dbname="master")

all_dbs = dbReadTable(conn, name = "escrituracao")

all_dbs_filtered = all_dbs %>%
  select(cnpj, nome_contribuinte, dataInicial, nomeBD)

rm(all_dbs)

dbDisconnect(conn)


#######################
#############



bd_name = as.character(readline(prompt = "Digite o nome do Banco de Dados: "))



# criando conexão com o Mysql DB.
conn <- dbConnect(MySQL(), port=3340, 
                  unix.socket="spedcontabil3340", 
                  username = "spedfiscal",
                  password = "spedfiscal", 
                  host="localhost", dbname=bd_name)

# listar nomes de tabelas
dbListTables(conn)

# Import data from DRE
dre <- dbReadTable(conn, "reg_j150")

# Import Diario
diario <- dbReadTable(conn, "reg_i250")

# import all mensal balancetes - ID_PAI representa os meses
balancetes_mensais <- dbReadTable(conn, "reg_i155")

# import nome das contas nos planos de contas - KEY com COD_CTA
plano_contas <- dbReadTable(conn, "reg_i050")
plano_contas <- plano_contas[,-2]


# JOIN de tabelas: Balancetes mensais com plano de contas:
balancetes_verificacao <- left_join(plano_contas, balancetes_mensais, by = "COD_CTA") %>%
  select(ID_PAI, COD_CTA, CTA, VL_DEB,IND_DC_FIN ,VL_CRED)


#
#balancetes_verificacao$VL_DEB <- if_else(balancetes_verificacao$IND_DC_FIN=="D",
#                                         -1*balancetes_verificacao$VL_DEB,
#                                         balancetes_verificacao$VL_DEB)
#balancetes_verificacao$VL_CRED <- if_else(balancetes_verificacao$IND_DC_FIN=="D",
#                                         -1*balancetes_verificacao$VL_CRED,
#                                         balancetes_verificacao$VL_CRED)
#

################TRIMESTRAL
# agrregate trimestral DRE
#dre_aggregate = dre %>%
#  select(COD_AGL, DESCR_COD_AGL, VL_CTA_FIN, IND_DC_CTA_FIN) %>%
#  group_by(COD_AGL, DESCR_COD_AGL) %>%
#  summarise(valor_total = sum(VL_CTA_FIN))



################MENSAL
###########################################
# aggredate monthlys DREs with signal correction (for debits and credits)
# signal correction:
dre_corrected = dre
for(i in (1:length(dre_corrected$ID))){
  print(i)
  if(dre_corrected$IND_DC_CTA_FIN[i] == "D"){
    dre_corrected$VL_CTA_FIN[i] = dre_corrected$VL_CTA_FIN[i] * -1
  } else {
    dre_corrected$VL_CTA_FIN[i] = dre_corrected$VL_CTA_FIN[i]
  }
}
dre_aggregate = as.data.frame(dre_corrected %>%
  select(COD_AGL, DESCR_COD_AGL, VL_CTA_FIN, IND_DC_CTA_FIN) %>%
  group_by(COD_AGL, DESCR_COD_AGL) %>%
  summarise(valor_total = sum(VL_CTA_FIN)))
dre_aggregate$valor_total = round(dre_aggregate$valor_total, digits = 2)
#####################################



# Disconnect database
dbDisconnect(conn)

# CLICAR EM LIBERAR A BIBLIOTECA RMysql
detach("package:RMySQL", unload = TRUE)


# Gerar balancetes mensais para os 12 meses
#cta_min = 30000000000
#cta_max = 40000000000

#cria_balancete <- function(i, cta_min, cta_max){
#  sqldf("SELECT ID_PAI, COD_CTA,CTA,SUM(VL_DEB), SUM(VL_CRED), (SUM(VL_CRED)-SUM(VL_DEB)) AS total_month  
#      FROM balancetes_verificacao
#  WHERE ID_PAI == $i AND COD_CTA > $cta_min AND COD_CTA < $cta_max
#  GROUP BY COD_CTA")
#
#}
#
#i = 0
#for(i in 1:12){
#  bal_i = cria_balancete(i, cta_min, cta_max)
#  i = i+1
#  print(i)
#}




bal_1 <- sqldf(
      "SELECT ID_PAI, COD_CTA,CTA,SUM(VL_DEB), SUM(VL_CRED), (SUM(VL_CRED)-SUM(VL_DEB)) AS total_month  
      FROM balancetes_verificacao
      WHERE ID_PAI == 1
      GROUP BY COD_CTA")


bal_2 <- sqldf(
  "SELECT ID_PAI, COD_CTA,CTA,SUM(VL_DEB), SUM(VL_CRED), (SUM(VL_CRED)-SUM(VL_DEB)) AS total_month  
  FROM balancetes_verificacao
  WHERE ID_PAI == 2
  GROUP BY COD_CTA")


bal_3 <- sqldf(
  "SELECT ID_PAI, COD_CTA,CTA,SUM(VL_DEB), SUM(VL_CRED), (SUM(VL_CRED)-SUM(VL_DEB)) AS total_month  
  FROM balancetes_verificacao
  WHERE ID_PAI == 3
  GROUP BY COD_CTA")

bal_4 <- sqldf(
  "SELECT ID_PAI, COD_CTA,CTA,SUM(VL_DEB), SUM(VL_CRED), (SUM(VL_CRED)-SUM(VL_DEB)) AS total_month  
  FROM balancetes_verificacao
  WHERE ID_PAI == 4
  GROUP BY COD_CTA")

bal_5 <- sqldf(
  "SELECT ID_PAI, COD_CTA,CTA,SUM(VL_DEB), SUM(VL_CRED), (SUM(VL_CRED)-SUM(VL_DEB)) AS total_month  
  FROM balancetes_verificacao
  WHERE ID_PAI == 5
  GROUP BY COD_CTA")

bal_6 <- sqldf(
  "SELECT ID_PAI, COD_CTA,CTA,SUM(VL_DEB), SUM(VL_CRED), (SUM(VL_CRED)-SUM(VL_DEB)) AS total_month  
  FROM balancetes_verificacao
  WHERE ID_PAI == 6
  GROUP BY COD_CTA")

bal_7 <- sqldf(
  "SELECT ID_PAI, COD_CTA,CTA,SUM(VL_DEB), SUM(VL_CRED), (SUM(VL_CRED)-SUM(VL_DEB)) AS total_month  
  FROM balancetes_verificacao
  WHERE ID_PAI == 7
  GROUP BY COD_CTA")

bal_8 <- sqldf(
  "SELECT ID_PAI, COD_CTA,CTA,SUM(VL_DEB), SUM(VL_CRED), (SUM(VL_CRED)-SUM(VL_DEB)) AS total_month  
  FROM balancetes_verificacao
  WHERE ID_PAI == 8
  GROUP BY COD_CTA")

bal_9 <- sqldf(
  "SELECT ID_PAI, COD_CTA,CTA,SUM(VL_DEB), SUM(VL_CRED), (SUM(VL_CRED)-SUM(VL_DEB)) AS total_month  
  FROM balancetes_verificacao
  WHERE ID_PAI == 9
  GROUP BY COD_CTA")

bal_10 <- sqldf(
  "SELECT ID_PAI, COD_CTA,CTA,SUM(VL_DEB), SUM(VL_CRED), (SUM(VL_CRED)-SUM(VL_DEB)) AS total_month  
  FROM balancetes_verificacao
  WHERE ID_PAI == 10
  GROUP BY COD_CTA")

bal_11 <- sqldf(
  "SELECT ID_PAI, COD_CTA,CTA,SUM(VL_DEB), SUM(VL_CRED), (SUM(VL_CRED)-SUM(VL_DEB)) AS total_month  
  FROM balancetes_verificacao
  WHERE ID_PAI == 11
  GROUP BY COD_CTA")

bal_12 <- sqldf(
  "SELECT ID_PAI, COD_CTA,CTA,SUM(VL_DEB), SUM(VL_CRED), (SUM(VL_CRED)-SUM(VL_DEB)) AS total_month  
  FROM balancetes_verificacao
  WHERE ID_PAI == 12
  GROUP BY COD_CTA")



# escrever XLS
#write.xlsx(dre_aggregate, "dre_anual.xlsx")
write.csv(x = dre_aggregate, sep = ";", dec = ",", file = "dre_agregado.csv")

write.xlsx(bal_1, "bal_1.xlsx")
write.xlsx(bal_2, "bal_2.xlsx")
write.xlsx(bal_3, "bal_3.xlsx")
write.xlsx(bal_4, "bal_4.xlsx")
write.xlsx(bal_5, "bal_5.xlsx")
write.xlsx(bal_6, "bal_6.xlsx")
write.xlsx(bal_7, "bal_7.xlsx")
write.xlsx(bal_8, "bal_8.xlsx")
write.xlsx(bal_9, "bal_9.xlsx")
write.xlsx(bal_10, "bal_10.xlsx")
write.xlsx(bal_11, "bal_11.xlsx")
write.xlsx(bal_12, "bal_12.xlsx")



write.xlsx(dre, "dre_12_acumulado_anual.xlsx")


write.xlsx(diario, "diario_marco_2017.xlsx")







  
  




