library(RODBC)
library(data.table)

con <- odbcConnect(dsn="EBIPOC", uid="ebietl", "XXXXXX")

## rs <- sqlQuery(con, model.id.query)

model.client.id.list = c('6610008','6075536','6454371','6759426','7311816','6807999','6758047','6639062','6400548','6410515'
                         ,'7552831','7059441','6962358','7668317')
# additional list
# model.client.id.list = c('6359031','6602400','6754379')

model.party.bank.id.list = c('5576331', '10813701', '5571877')

ebiadm.client.id.list = c('6467643','6850876','6610884','6694944','6980424','7111857','6510863','6857457','6445657')

model.id.query = c("select
  distinct 
	pc.client_party_id as client_id
	, pc.pol_id
	, cl.claim_id 
from model.dim_policy_client pc
left join model.dim_gi_claim cl
	on pc.pol_id = cl.pol_id
where pc.client_party_id = '<client.id>'
order by 1,2,3")

ebiadm.id.query = c("select
  distinct
  pc.client_id
	, pc.pol_id
	, cl.claim_id
	, pr.rsk_id
from ebiadm.dim_gi_policy_client pc
left join ebiadm.dim_gi_claim cl
on cl.pol_id = pc.pol_id
left join model.dim_policy_risk_sk pr
on pc.pol_id = pr.pol_id
where pc.client_id = '<client.id>'
order by 1,2,3,4")

get.all.ids <- function(client.id, query) {
  query <- sub("<client.id>", client.id, query)
  rs <- sqlQuery(con, query)
}

model.ids <- rbindlist(lapply(model.client.id.list, get.all.ids, model.id.query))
ebiadm.ids <- rbindlist(lapply(ebiadm.client.id.list, get.all.ids, ebiadm.id.query))

# model.ids <- rbindlist(lapply(temp.client.id.list, get.all.ids, model.id.query))

model.client.ids <- unique(na.omit(model.ids$CLIENT_ID))
model.policy.ids <- unique(na.omit(model.ids$POL_ID))
model.claim.ids <- unique(na.omit(model.ids$CLAIM_ID))
model.party.bank.ids <- unique(na.omit(model.party.bank.id.list))

ebiadm.client.ids <- unique(na.omit(ebiadm.ids$CLIENT_ID))
ebiadm.policy.ids <- unique(na.omit(ebiadm.ids$POL_ID))
ebiadm.claim.ids <- unique(na.omit(ebiadm.ids$CLAIM_ID))
model.rsk.ids <- unique(na.omit(ebiadm.ids$RSK_ID))

model.client.column.dt = setNames(data.table(matrix(c('MODEL.DIM_CLIENT','RNWL_INVITATION_DELIVERY_EMAIL_ADDR','CLIENT_PARTY_ID','TEST_UPDATE'
                                                      ,'MODEL.DIM_PARTY','BUSN_PHONE_NUM','PARTY_ID','123456789'
                                                      ,'MODEL.DIM_PARTY','EMAIL_ADDR','PARTY_ID','TEST_UPDATE'
                                                      ,'MODEL.DIM_PARTY','FAX_NUM','PARTY_ID','123456789'
                                                      ,'MODEL.DIM_PARTY','FIRST_NAME','PARTY_ID','TEST_UPDATE'
                                                      ,'MODEL.DIM_PARTY','MOBILE_PHONE_NUM','PARTY_ID','123456789'
                                                      ,'MODEL.DIM_PARTY','OTHER_NAME','PARTY_ID','TEST_UPDATE'
                                                      ,'MODEL.DIM_PARTY','PRIVATE_PHONE_NUM','PARTY_ID','123456789'
                                                      ,'MODEL.DIM_PARTY','TAX_FILE_NUM','PARTY_ID','123456789')
                                                    , ncol = 4, byrow = TRUE))
                                  ,c("TABLE","COLUMN","TABLE_ID", "VALUE"))

model.party.bank.column.dt = setNames(data.table(matrix(c('MODEL.DIM_PARTY_BANK_DETAILS','ACCT_NUM','CLIENT_PARTY_ID','123456789'
                                                      ,'MODEL.DIM_PARTY_BANK_DETAILS','ACCT_NAME','CLIENT_PARTY_ID','TEST_UPDATE'
                                                      ,'MODEL.DIM_PARTY_BANK_DETAILS','ACCT_BSB','CLIENT_PARTY_ID','123456789')
                                                    , ncol = 4, byrow = TRUE))
                                  ,c("TABLE","COLUMN","TABLE_ID", "VALUE"))

model.claim.column.dt = setNames(data.table(matrix(c('MODEL.DIM_GI_CLAIM_EMPLOYMENT','EMPLOYER_CONTACT_NUMBER','CLAIM_ID','123456789'
                                                      ,'MODEL.DIM_GI_CLAIM_EMPLOYMENT','EMPLOYER_CONTACT_NAME','CLAIM_ID','TEST_UPDATE'
                                                      ,'MODEL.DIM_GI_CLAIM_EMPLOYMENT','TRADING_NAME','CLAIM_ID','TEST_UPDATE')
                                                    , ncol = 4, byrow = TRUE))
                                  ,c("TABLE","COLUMN","TABLE_ID", "VALUE"))

model.rsk.column.dt = setNames(data.table(matrix(c('MODEL.DIM_GI_VEHICLE_DRIVER','FIRST_NAME','RSK_ID','TEST_UPDATE'
                                                      ,'MODEL.DIM_GI_VEHICLE_DRIVER','LAST_NAME','RSK_ID','TEST_UPDATE'
                                                      ,'MODEL.DIM_GI_VEHICLE_DRIVER','DOB','RSK_ID','1999-01-01'
                                                      ,'MODEL.DIM_GI_VEHICLE_RISK','YOUNGEST_DRIVER_DOB','RSK_ID','1999-01-01'
                                                      ,'MODEL.DIM_GI_VEHICLE_RISK','DRIVER_DOB','RSK_ID','1999-01-01')
                                                    , ncol = 4, byrow = TRUE))
                                  ,c("TABLE","COLUMN","TABLE_ID", "VALUE"))

ebiadm.client.column.dt = setNames(data.table(matrix(c('EBIADM.DIM_GI_CLIENT','CLIENT_NAME_2','CLIENT_ID','TEST_UPDATE'
                                                       ,'EBIADM.DIM_GI_CLIENT','CONTACT_NAME','CLIENT_ID','TEST_UPDATE')
                                                     , ncol = 4, byrow = TRUE))
                                   ,c("TABLE","COLUMN","TABLE_ID","VALUE"))

update.query <- function(id, dt) {
  query.vector <- dt[, paste0("update ", TABLE," set ", COLUMN, " = '", VALUE, "' where ", TABLE_ID, " = ", id)]
}

update.query.vector <- unlist(lapply(model.client.ids, update.query, model.client.column.dt))
write.table(update.query.vector, "D:/Privacy_Test/Update_selectexd_test_cases/update.model.client.additional.sql")
# rs <- lapply(update.query.vector, function(q) sqlQuery(con, q))

update.query.vector <- unlist(lapply(model.claim.ids, update.query, model.claim.column.dt))
write.table(update.query.vector, "D:/Privacy_Test/Update_selectexd_test_cases/update.model.claim.additional.sql")
# rs <- lapply(update.query.vector, function(q) sqlQuery(con, q))

update.query.vector <- unlist(lapply(model.rsk.ids, update.query, model.rsk.column.dt))
write.table(update.query.vector, "D:/Privacy_Test/Update_selectexd_test_cases/update.model.rsk.sql")
# rs <- lapply(update.query.vector, function(q) sqlQuery(con, q))

update.query.vector <- unlist(lapply(model.party.bank.id.query, update.query, model.party.bank.column.dt))
write.table(update.query.vector, "D:/Privacy_Test/Update_selectexd_test_cases/update.model.party.bank.sql")
# rs <- lapply(update.query.vector, function(q) sqlQuery(con, q))

update.query.vector <- unlist(lapply(ebiadm.client.ids, update.query, ebiadm.client.column.dt))
write.table(update.query.vector, "D:/Privacy_Test/Update_selectexd_test_cases/update.ebiadm.client.sql")
# rs <- lapply(update.query.vector, function(q) sqlQuery(con, q))
