library(RMySQL)
library(dplyr)
library(xlsx)
library(sqldf)

# Reading all DBs
# criando conexão com o Mysql DB.
conn <- dbConnect(MySQL(), port=3341, 
                  unix.socket="spedfiscal3341", 
                  username = "spedfiscal",
                  password = "spedfiscal", 
                  host="localhost", dbname = "master")

all_dbs = dbReadTable(conn, name = "escrituracao")

all_dbs_filtered = all_dbs %>%
  select(cnpj, nome_contribuinte, dataInicial, nomeBD)

rm(all_dbs)

dbDisconnect(conn)


#######################
#############

db_name = "bd20210430015527"

# criando conexão com o Mysql DB.
conn <- dbConnect(MySQL(), port=3341, 
                  unix.socket="spedfiscal3341", 
                  username = "spedfiscal",
                  password = "spedfiscal", 
                  host="localhost", dbname=db_name)

# listar nomes de tabelas
dbListTables(conn)

# Importar Hash codes das ECDs relacionadas: reg_c040
hash <- dbReadTable(conn, "reg_c040")
hash_edit <- hash %>%
  select(ID, HASH_ECD, DT_INI)



# Import data from DRE
dre <- dbReadTable(conn, "reg_e355")
# import nome das contas nos planos de contas - KEY com COD_CTA
plano_contas <- dbReadTable(conn, "reg_c050")
# JOIN de tabelas: dre com plano de contas:
tabela_geral <- left_join(plano_contas, dre, by = "COD_CTA")


# selecione somente os IDs Pai de 1 à 13
tabela_geral_novo <- tabela_geral %>%
  select(ID_PAI.y, COD_CTA, CTA, VL_SLD_FIN, IND_VL_SLD_FIN) %>%
  filter(ID_PAI.y > 0)



####  Quando o ECD não está na versão trimestral use este código: 
# selecione os IDs Pai = 1 que são os saldos anuais
tabela_anual <- tabela_geral_novo %>%
  select(ID_PAI.y, COD_CTA, CTA, VL_SLD_FIN, IND_VL_SLD_FIN) %>%
  filter(ID_PAI.y == 1)
  # filter(COD_CTA < 5000000000) 


#### Se o ECD for trimestral, use este código aqui:
tabela_anual = tabela_geral_novo %>%
  select(ID_PAI.y, COD_CTA, CTA, VL_SLD_FIN, IND_VL_SLD_FIN) %>%
  group_by(COD_CTA, CTA) %>%
  summarise(valor_total = sum(VL_SLD_FIN))





# Disconnect database
dbDisconnect(conn)

# CLICAR EM LIBERAR A BIBLIOTECA RMysql
detach("package:RMySQL", unload = TRUE)



############
### manipulação do SQLDF para reescrever a tabela anual



### inversao de sinais para laçamentos do tipo débito D:
tabela_anual$VL_SLD_FIN <- if_else(tabela_anual$IND_VL_SLD_FIN=="D", -1*tabela_anual$VL_SLD_FIN, 
                                   tabela_anual$VL_SLD_FIN)

tabela_anual$COD_CTA <- as.numeric(tabela_anual$COD_CTA)

tabela_dre_final <- sqldf(
  "SELECT COD_CTA,CTA, SUM(VL_SLD_FIN)
  FROM tabela_anual 
  GROUP BY COD_CTA")


###############
##### Exportações



# exportar para excell
write.xlsx(tabela_dre_final, "DRE_anual_ECF.xlsx")

# para ECD TRIMESTRAL:
write.xlsx(tabela_anual, "DRE_anual_ECF.xlsx")
write.csv(x = tabela_anual, file = "dre_anual_ecf.csv")


# tabela hash:
write.xlsx(hash_edit, "Tabela_hash_exportada_ECF.xlsx")













  
  




