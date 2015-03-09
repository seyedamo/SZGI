setwd("D:/SAP-C/DownStream/HIERARCHY/FULL_DDL")

createDDL <- function(t, template) {
  sql <- gsub("<TABLE_NAME>", t, template)
  filename = paste0(t, ".sql")
  write.table(sql, filename, quote=F, row.names=F, col.names=F)
  print(filename)
}


createObjects <- function(object, template) {
  object_file = paste0("./Objects/",object,".txt")
  table_list = read.table(object_file)
  template_fileName <- paste0("SQL_templates/",template)
  template_sql <- readChar(template_fileName, file.info(template_fileName)$size)
  rc<-lapply(table_list[,1], createDDL, template_sql)
}


object_list = c('Account', 'CostCentre','CostElement','LLOB','ProfitCentre','Segment')
template_list = c('Account_template.sql','CostCentre_template.sql','CostElement_template.sql','LLOB_template.sql',
                  'ProfitCentre_template.sql','Segment_template.sql')

object_list = c('LLOB')
template_list = c('LLOB_template.sql')

object_list = c('NOB','CounterParty')
template_list = c('NOB_template.sql','CounterParty_template.sql')

Map(createObjects, object_list, template_list)
