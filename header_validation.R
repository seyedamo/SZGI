library(dplyr)
library(RODBC)

con <- odbcConnect(dsn="EBIDB_DEV", uid="ebiadm", "XXX")
setwd("D:/SAP-C/DownStream/Master data and Transactional data/Open Hubs Demo")
setwd("D:/SAP-C/DownStream/Master data and Transactional data/metadata")
setwd("D:/SAP-C/DownStream/Master data and Transactional data/metadata for trans")
setwd("D:/SAP-C/DownStream/Master data and Transactional data/Acct_Hier_Header")
files = list.files(pattern = '*.csv')

insert <- function(sql) {
  print(sql)
  rs <- sqlQuery(con, sql)
}

aQuote <- function (string) {
  paste0("'",string,"'")
}

load_metadata <- function (fileName) {
  f <- read.table(fileName, header = T, sep = "|", skip = 5, strip.white = T, na.strings = "", stringsAsFactors=F)
  f[is.na(f)] <- 'NULL'
  temp = mutate(f, FIELDNAME = aQuote(FIELDNAME), KEY = ifelse(KEY=='X', aQuote(KEY), KEY), TYPE = aQuote(TYPE))
  temp = mutate(temp, fileName = aQuote(fileName), createdDt =  aQuote(Sys.Date()), createdBy = "'SAPBW'", updateDt = 'NULL')
  values = mutate(temp, value = paste(fileName,COLUMN,FIELDNAME,KEY,TYPE,LENGTH,OUTPUTLEN,DECIMALS,createdDt,createdBy,updateDt, sep = ","))
  sql = mutate(values, value = paste0("insert into ods.sapbw_file_metadata values(", value, ")"))
  rc <- lapply(as.list(sql$value), insert)
}

rc <- lapply(as.list(files), load_metadata)
