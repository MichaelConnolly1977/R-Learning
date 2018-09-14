#packages
install.packages("tidyverse")
install.packages("lubridate")
install.packages("odbc")
install.packages("reshape2")
require(tidyverse)
require(lubridate)
require(odbc)
require(reshape2)

#################################################
# Make connection - We are connecting to Netezza & making a ref as opposed to a direct SQL query to the database
con <- dbConnect(odbc::odbc(), "IBM Netezza")

PSERS <- tbl(con, "MC_OFSH_000_PSERS")
head(PSERS)

# Find out how many rows are in the query
PSERS %>% tally()
str(PSERS)

##################################################
# Change data format
#PSERS$SBM_DT <- as.Date(PSERS$SBM_DT)

----------------------------------------------------------------------------------------------------------------
  #                                             1A. Pre Price Change.
  #----------------------------------------------------------------------------------------------------------------


# Creating a dataset that is pre the last price change at 2018-05-12
PSERS_PreChange <- PSERS %>% filter(SBM_DT < as.Date('2018-06-30'))
str(PSERS_PreChange)

PSERS_PreChange %>% pull(SBM_DT) %>% summary()
head(PSERS_PreChange)

# Group Category by RX / AWP / IC / Guar / Position
PSERS_Pre <- PSERS_PreChange%>%
  filter(EXCLUSIONS == 'Not Excluded')%>%
  group_by(PAMS_CATEGORY, EXCLUSIONS)%>%
  summarise(RX = sum(CLAIM_CNTR) ,AWP =sum(POST_AWP), IC = sum(CLT_INGRED_COST_PAID))%>%
  data.frame()

unique(PSERS_Pre$EXCLUSIONS)
str(PSERS_Pre)

# This can ouytput the SQL if you need it
#t %>% show_query()

# Collect the data for local use before you close the connection
PSERS_Pre <- PSERS_Pre %>% collect()

# This allows you right the reference to a temp table in the Sandbox
#PSERS %>% compute(temporary = T, "LS_001_TEST")

# Disconnect from database 

#----------------------------------------------------------------------------------------------------------------
#                                             1B. Pre Price Change Dispense Fees
#----------------------------------------------------------------------------------------------------------------

# Group Category by RX / Total Dispense Fees / Dispense Fee Per RX / Contract Dispense Fee Per RX / Position

PSERS_PreDispense <- PSERS_PreChange%>%
  filter(EXCLUSIONS == 'Not Excluded')%>%
  group_by(PAMS_CATEGORY)%>%
  summarise(RX = sum(CLAIM_CNTR), Total_DispFee = sum(CLT_DISPENSING_FEE))%>%
  data.frame()

str(PSERS_PreDispense)

PSERS_PreDisp <- PSERS_PreDispense %>% collect()

#----------------------------------------------------------------------------------------------------------------
#                                             2A. Post Price Change.
#----------------------------------------------------------------------------------------------------------------

# Creating a dataset that is after the last price change at 2018-05-12
PSERS_PostChange <- PSERS %>% filter(SBM_DT > as.Date('2018-06-30'))
str(PSERS_PostChange)

PSERS_Post = PSERS_PostChange%>%
  filter(EXCLUSIONS == 'Not Excluded')%>%
  group_by(PAMS_CATEGORY)%>%
  summarise(RX = sum(CLAIM_CNTR), AWP =sum(POST_AWP), IC = sum(CLT_INGRED_COST_PAID))%>%
  data.frame()

PSERS_Post <- PSERS_Post %>% collect()

#----------------------------------------------------------------------------------------------------------------
#                                             2B. Pos Price Change Dispense Fees
#----------------------------------------------------------------------------------------------------------------

# Group Category by RX / Total Dispense Fees / Dispense Fee Per RX / Contract Dispense Fee Per RX / Position

PSERS_PostDispense = PSERS_PostChange%>%
  filter(EXCLUSIONS == 'Not Excluded')%>%
  group_by(PAMS_CATEGORY)%>%
  summarise(RX = sum(CLAIM_CNTR), Total_DispFee = sum(CLT_DISPENSING_FEE))%>%
  data.frame()

str(PSERS_PostDispense)

PSERS_PostDisp <- PSERS_PostDispense %>% collect()



dbDisconnect(con)


#################################################

Discount_Rates <- read.csv("I:/Finance/Team/Ireland Projects/Custom MAC/PSERS/Source Data/Discount_Rates.csv", stringsAsFactors = F)
Dispense_Fees <- read.csv("I:/Finance/Team/Ireland Projects/Custom MAC/PSERS/Source Data/Dispense_Fees.csv", stringsAsFactors = F)

PSERS_Pre
PSERS_PreDisp
PSERS_Post
PSERS_PostDisp

# Join in Discounts & Complete Calcultions
# Join in Dispense Fees & Complete Calculations


----------------------------------------------------------------------------------------------------------------
  #                                             1A. Pre Price Change.
  #----------------------------------------------------------------------------------------------------------------


# Join Discount Rates to the main dataset using Category as the primary key
joinedDt <- PSERS_Pre %>%
  left_join(Discount_Rates, by = c("PAMS_CATEGORY" = "Category")) 

head(joinedDt)

# Add in new rows - mutuate gives us the new rows
PSERS_PreChange <- joinedDt %>%
  rowwise() %>%
  mutate(GUAR = AWP * (1-Discount)) %>%
  mutate(POSITION = GUAR-IC) %>%
  mutate(EFFDISCOUNT = (1-IC/AWP))

head(PSERS_PreChange)

PSERS_PreChange<-PSERS_PreChange[c("PAMS_CATEGORY", "RX", "AWP", "IC", "EFFDISCOUNT", "Discount", "GUAR", "POSITION")]
head(PSERS_PreChange)

target <- c("Retail_Brand30", "Retail_Brand90", "Retail_Generic30", "Retail_Generic90", "Mail_Brand", "Mail_Generic", "Spec_Agg")
PSERS_PreChange <- PSERS_PreChange[match(target, PSERS_PreChange$PAMS_CATEGORY),]
head(PSERS_PreChange)


#----------------------------------------------------------------------------------------------------------------
#                                             1B. Pre Price Change Dispense Fees
#----------------------------------------------------------------------------------------------------------------

# Join Fee Rates to the Grouped Dataset
PSERS_PreDispense <- PSERS_PreDisp %>%
  left_join(Dispense_Fees, by = c("PAMS_CATEGORY" = "Category")) 

head(PSERS_PreDispense)

# Calculate Dispense Fee Per RX
PSERS_PreDispense = PSERS_PreDispense%>%
  mutate(DispFee_PerRX = Total_DispFee/RX)

head(PSERS_PreDispense)

# Calculate Over / Under
PSERS_PreDispense = PSERS_PreDispense%>%
  mutate(Perf = (Disp_Fees-DispFee_PerRX)*RX)

head(PSERS_PreDispense)

target <- c("Retail_Brand30", "Retail_Brand90", "Retail_Generic30", "Retail_Generic90", "Mail_Brand", "Mail_Generic", "Spec_Agg")
PSERS_PreDispense <- PSERS_PreDispense[match(target, PSERS_PreDispense$PAMS_CATEGORY),]

head(PSERS_PreDispense)

#----------------------------------------------------------------------------------------------------------------
#                                             2A. Post Price Change.
#----------------------------------------------------------------------------------------------------------------


# Join Discount Rates to the main dataset using Category as the primary key
joinedDt <- PSERS_Post %>%
  left_join(Discount_Rates, by = c("PAMS_CATEGORY" = "Category")) 

head(joinedDt)

# Add in new rows - mutuate gives us the new rows
PSERS_PostChange <- joinedDt %>%
  rowwise() %>%
  mutate(GUAR = AWP * (1-Discount)) %>%
  mutate(POSITION = GUAR-IC) %>%
  mutate(EFFDISCOUNT = (1-IC/AWP))

head(PSERS_PostChange)

PSERS_PostChange<-PSERS_PostChange[c("PAMS_CATEGORY", "RX", "AWP", "IC", "EFFDISCOUNT", "Discount","GUAR","POSITION")]
head(PSERS_PostChange)

target <- c("Retail_Brand30", "Retail_Brand90", "Retail_Generic30", "Retail_Generic90", "Mail_Brand", "Mail_Generic", "Spec_Agg")
PSERS_PostChange <- PSERS_PostChange[match(target, PSERS_PostChange$PAMS_CATEGORY),]
head(PSERS_PostChange)

#----------------------------------------------------------------------------------------------------------------
#                                             2B. Pos Price Change Dispense Fees
#----------------------------------------------------------------------------------------------------------------
# Join Fee Rates to the Grouped Dataset
PSERS_PostDispense <- PSERS_PostDisp %>%
  left_join(Dispense_Fees, by = c("PAMS_CATEGORY" = "Category")) 

head(PSERS_PostDispense)

# Calculate Dispense Fee Per RX
PSERS_PostDispense = PSERS_PostDispense%>%
  mutate(DispFee_PerRX = Total_DispFee/RX)

head(PSERS_PostDispense)

# Calculate Over / Under
PSERS_PostDispense = PSERS_PostDispense%>%
  mutate(Perf = (Disp_Fees-DispFee_PerRX)*RX)

head(PSERS_PostDispense)

target <- c("Retail_Brand30", "Retail_Brand90", "Retail_Generic30", "Retail_Generic90", "Mail_Brand", "Mail_Generic", "Spec_Agg")
PSERS_PostDispense <- PSERS_PostDispense[match(target, PSERS_PostDispense$PAMS_CATEGORY),]

head(PSERS_PostDispense)

#----------------------------------------------------------------------------------------------------------------
#                                             Export to Excel
#----------------------------------------------------------------------------------------------------------------

# creatework book
wb <- xlsx::createWorkbook()
# set wb name
wbFileName <- paste0(today() %>% str_replace_all("-", ""), "_MC_PSERS_Analysis.xlsx" )
# save workbook to a location
xlsx::saveWorkbook(wb, file = paste0("I:/Finance/Team/Ireland Projects/Custom MAC/PSERS/Output/Sept", wbFileName))

# Can I just write different files to different sheets in the same output file?
xlsx::write.xlsx(PSERS_PreChange, file = paste0("I:/Finance/Team/Ireland Projects/Custom MAC/PSERS/Output/Sept", wbFileName), sheetName = "PSERS_Pre")
xlsx::write.xlsx(PSERS_PreDispense, file = paste0("I:/Finance/Team/Ireland Projects/Custom MAC/PSERS/Output/Sept", wbFileName), sheetName = "PSERS_PreDisp", append = TRUE)
xlsx::write.xlsx(PSERS_PostChange, file = paste0("I:/Finance/Team/Ireland Projects/Custom MAC/PSERS/Output/Sept", wbFileName), sheetName = "PSERS_Post", append = TRUE)
xlsx::write.xlsx(PSERS_PostDispense, file = paste0("I:/Finance/Team/Ireland Projects/Custom MAC/PSERS/Output/Sept", wbFileName), sheetName = "PSERS_PostDisp", append = TRUE)







