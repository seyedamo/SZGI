library(dplyr)
library(RODBC)

con <- odbcConnect(dsn="EBIDB_DEV", uid="ebiadm", "XXX")
setwd("D:/SAP-C/DownStream/Master data and Transactional data/test data/")

compareNA <- function(v1,v2) {
  # This function returns TRUE wherever elements are the same, including NA's,
  # and false everywhere else.
  same <- (v1 == v2)  |  (is.na(v1) & is.na(v2))
  same[is.na(same)] <- FALSE
  return(same)
}

compare <- function(c) {
  table_N = c[1]
  file_N = c[2]
  print(table_N)
  query = paste0(c("select * from SRC."), table_N)
  #query = paste0(c("select * from SRC."), table_N, " order by gfcostelm, dateto")  
  rs <- sqlQuery(con, query)
  fp <- read.table(file_N, header = F, sep = "|", stringsAsFactors = F, strip.white = T, quote = "",na.strings = "", comment.char="")
  #fp_o <- arrange(fp, V3, V5)
  n = dim(fp)[2]
  rs = rs[,1:n]
#  rs = as.data.frame(lapply(rs,function(x) if(is.character(x)|is.factor(x)) gsub("-","",x) else x))
  foo <- lapply(1:n, FUN = function(i) {
    if(is(rs[,i], "Date")) {
      rs[,i] <<- format(rs[,i], "%Y%m%d")
    }
  })
  if (all(dim(rs) == dim(fp)) == F) {
    print("row count")
  } else if(all(compareNA(fp,rs)) == F) {
    print("content")
  } else {
    print("same")
  }
  print("###")
  return(tryCatch)
}

testFunction <- function (c) {
  return(tryCatch(compare(c), error=function(e) NULL))
}

l=list(c('SAPBW_CHART_OF_ACCOUNTS_TEXT','ChrtAccts_TextF.csv'),
       c('SAPBW_COMPANY_CODE_ATTR','ComCd_AttrF.csv'),
       c('SAPBW_COMPANY_CODE_TEXT','ComCd_TextF.csv'),
       c('SAPBW_CORE_DIM_1_TEXT','CoreDim1_TextF.csv'),
       c('SAPBW_COST_CENTER_ATTR','CostCnt_AttrF.csv'),
       c('SAPBW_COST_CENTER_TEXT','CostCnt_TextF.csv'),
       c('SAPBW_COST_ELEMENT_ATTR','CostElm_AttrF.csv'),
       c('SAPBW_COST_ELEMENT_TEXT','CostElm_TextF.csv'),
       c('SAPBW_DOCUMENT_TYPE_TEXT','DocTy_TextF.csv'),
       c('SAPBW_ACCOUNT_ATTR','GLAcco_AttrF.csv'),
       c('SAPBW_ACCOUNT_TEXT','GLAcco_TextF.csv'),
       c('SAPBW_GROUP_ACCOUNT_TEXT','GroupAcco_TextF.csv'),
       c('SAPBW_GROUP_LOB_TEXT','GLoB_TextF.csv'),
       c('SAPBW_INTERNAL_ORDER_ATTR','Order_AttrF.csv'),
       c('SAPBW_INTERNAL_ORDER_TEXT','Order_TextF.csv'),
       c('SAPBW_NATURE_OF_BUSINESS_TEXT','NoB_TextF.csv'),
       c('SAPBW_PROFIT_CENTER_ATTR','ProCtr_AttrF.csv'),
       c('SAPBW_PROFIT_CENTER_TEXT','ProCtr_TextF.csv'),
       c('SAPBW_TRADING_PARTNER_TEXT','TradPart_AttrF.csv'),
       c('SAPBW_MOVEMENT_TYPE_TEXT','ToM_TextF.csv'),
       c('SAPBW_VALUE_TYPE_TEXT','ValTyp_TextF.csv'),
       c('SAPBW_VERSION_TEXT','Version_TextF.csv'))

foo<-lapply(l, testFunction)
