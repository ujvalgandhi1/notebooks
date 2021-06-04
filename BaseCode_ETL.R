
library(data.table)
library(plyr)
library(sqldf)
library(haven)
library(esquisse)
library(DataExplorer)

setwd('C:/Work/CMS_RArchitecture/Data/inpatientclaims')

#Read all the Inpatient claims file
df_inpatient <- ldply(list.files(), read.csv, header=TRUE)


#Red all the Beneficiary Files
setwd('C:/Work/CMS_RArchitecture/Data/BeneficiaryData')

df_b <- ldply(list.files(), read.csv, header=TRUE)
df_b <- sqldf("SELECT DISTINCT DESYNPUF_ID, BENE_BIRTH_DT, BENE_DEATH_DT, BENE_SEX_IDENT_CD, BENE_RACE_CD, 
SP_STATE_CODE, BENE_COUNTY_CD from df_b")

#Using Windowing functions to isolate the readmission days
df_inpatient <- transform(df_inpatient, CLM_ADMSN_DT = as.Date(as.character(CLM_ADMSN_DT), "%Y%m%d"))
df_inpatient$AdmitCode <- as.numeric(substr(df_inpatient$ADMTNG_ICD9_DGNS_CD, 1, 3))

#Adding synthetic data for Star Rating
df_providerrating <- sqldf("Select distinct PRVDR_NUM from df_inpatient")
df_providerrating$StarRating <- round(runif(2895, 1, 5),2)

df_inpatient <- sqldf("
select 
*, 
CLM_ADMSN_DT - LAG(CLM_ADMSN_DT) OVER(PARTITION BY DESYNPUF_ID ORDER BY CLM_ADMSN_DT) AS ReadmissionDays,
 case
            when AdmitCode <= 239 then 'Neoplasms'
            when AdmitCode <= 279 then 'Endo/Immune'
            when AdmitCode <= 289 then 'Blood'
            when AdmitCode <= 319 then 'Mental'
            when AdmitCode <= 389 then 'Nervous'
            when AdmitCode <= 459 then 'Circulatory'
            when AdmitCode <= 519 then 'Respiratory'
            when AdmitCode <= 579 then 'Digestive'
            when AdmitCode <= 629 then 'Genitourinary'
            when AdmitCode <= 679 then 'Pregnancy'
            when AdmitCode <= 709 then 'Skin'
            when AdmitCode <= 739 then 'Musculoskeletal'
            when AdmitCode <= 759 then 'Congenital'
            when AdmitCode <= 779 then 'Perinatal'
            when AdmitCode <= 799 then 'Ill-Defined'
            when AdmitCode <= 999 then 'InjuryPoisoning'
            else 'Not Defined'
            end AdmitCategory
FROM
df_inpatient
order by DESYNPUF_ID, CLM_ADMSN_DT
")

#County Data
statelookup <- read.csv('C:/Work/CMS_RArchitecture/Data/supportingdata/StateMapping.csv',
                         header=TRUE)

#Join Inpatient Claims with Beneficiary Data

#Using age groups from https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3825015/
#Using gen x classifications from https://www.weforum.org/agenda/2015/09/how-different-age-groups-identify-with-their-generational-labels/


df <- sqldf("select a.*, 
b.BENE_BIRTH_DT, b.BENE_DEATH_DT, b.BENE_SEX_IDENT_CD, b.BENE_RACE_CD, b.SP_STATE_CODE, b.BENE_COUNTY_CD,
case
  when b.BENE_SEX_IDENT_CD = 1 THEN 'Male'
  when b.BENE_SEX_IDENT_CD = 2 then 'Female'
  else 'Not Available' end as Gender,
case
  when b.BENE_RACE_CD = 1 then 'WHITE'
  when b.BENE_RACE_CD = 2 THEN 'BLACK'
  when b.BENE_RACE_CD = 3 THEN 'OTHER'
  when b.BENE_RACE_CD = 4 then 'ASIAN/PACIFIC ISLANDER'
  WHEN b.BENE_RACE_CD = 5 then 'HISPANIC'
  when b.BENE_RACE_CD = 6 THEN 'NORTH AMERICAN NATIVE'
  ELSE 'Unknown' end as Race, 
case
  when (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) <= 2 then '0-2'
  when (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) <= 5 THEN '3-5'
  when (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) <= 13 THEN '6-13'
  when  (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) <= 18 THEN '14-18'
  when (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) <= 33 THEN '19-33'
  when (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) <= 48 THEN '34-48'
  when (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) <= 64 THEN '49-64'
  when (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) <= 78 THEN '65-78'
  when (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) <= 98 THEN '79-98'
  else 'NA' end as NIH_AgeBuckets, 
case
  when (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) >= 9 and (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) <= 24 then 'Gen Z'
  when (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) >= 25 and (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) <= 40 THEN 'Millennials'
  when (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) >= 41 and  (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) <= 56 THEN 'Gen X'
  when (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) >= 57 and   (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) <= 66 THEN 'Boomers I'
  when (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) >= 67 and  (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) <= 75 THEN 'Boomer II'
  when (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) >= 76 and  (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) <= 93 THEN 'Post War'
  when (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) >= 94 and  (strftime('%Y', Date()) - LEFTSTR(b.BENE_BIRTH_DT,4)) <= 99 THEN 'WW II'
  else 'NA' end as Census_AgeBuckets, 
case 
  when b.BENE_DEATH_DT IS NULL THEN 'Not Dead' else 'Dead' end as DeathIndicator,
c.State, c.Population, 
d.StarRating
from df_inpatient a
left join (SELECT DISTINCT DESYNPUF_ID, BENE_BIRTH_DT, BENE_DEATH_DT, BENE_SEX_IDENT_CD, BENE_RACE_CD, 
           SP_STATE_CODE, BENE_COUNTY_CD
           from df_b)  b on a.DESYNPUF_ID = b.DESYNPUF_ID
left join statelookup c on b.SP_STATE_CODE = c.StateCode
left join df_providerrating d on a.PRVDR_NUM = d.PRVDR_NUM

            ") 


#Writing to a csv for Tableau Analysis
fwrite(df, 'C:/Work/CMS_RArchitecture/Data/df.csv')



#Exploratory Data Analysis

#Need to rest the working directory here
setwd('C:/Work/CMS_RArchitecture/Data')

plot_missing(df)
#create_report(df)   #Creates comprehensive report on every column
#create_report(df, y = "AdmitCode")

#Use basic report 
introduce(df)
plot_intro(df)

plot_bar(df, with = "AdmitCode")

# Visualization with Esquisse
#Create aggregated data sets for Esquisse to perform faster
df1 <- sqldf("Select ReadmissionDays, AdmitCategory, State, 
                  Gender, Race, NIH_AgeBuckets, Census_AgeBuckets, 
                  case when ReadmissionDays <=30 then 'Red' else 'Green' end ReadmissionDaysGroup, 
                  count(distinct DESYNPUF_ID) AS InpatientCount
                    from df
                where ReadmissionDays >= 0
                    group by ReadmissionDays, AdmitCategory, State, 
                            Gender, Race, NIH_AgeBuckets, Census_AgeBuckets")


##Use Esquisser to build the bar chart of Inpatient Count by Readmission Days
esquisser()


##Basic ML 

library(alookr)
library(dplyr)
library(dlookr)
library(rpart.plot)
library(rpart)

df2 <- sqldf("Select Gender, Race, NIH_AgeBuckets, AdmitCategory, State, 
                  case when ReadmissionDays <=30 then 'Red' else 'Green' end ReadmissionDaysGroup, 
                  count(distinct DESYNPUF_ID) AS InpatientCount
                    from df
                where ReadmissionDays >= 0 
                    group by ReadmissionDaysGroup, AdmitCategory, State, 
                            Gender, Race, NIH_AgeBuckets")

summary(df2)

sb <- df2 %>%
  split_by(target = ReadmissionDaysGroup)

tmp <- df2 %>%
  split_by(ReadmissionDaysGroup, ratio = 0.6)

summary(sb)

train_smote <- sb %>%
  sampling_target(seed = 1234L, method = "ubSMOTE")

table(train_smote$ReadmissionDaysGroup)

train <- train_smote %>%
  cleanse

test <- sb %>%
  extract_set(set = "test")

test$AdmitCategory <- as.factor(test$AdmitCategory)
test$State <- as.factor(test$State)
test$Gender <- as.factor(test$Gender)
test$Race <- as.factor(test$Race)
test$NIH_AgeBuckets <- as.factor(test$NIH_AgeBuckets)
test$ReadmissionDaysGroup <- as.factor(test$ReadmissionDaysGroup)

result <- train %>%
  run_models(target = "ReadmissionDaysGroup", positive = "Green")

result

pred <- result %>%
  run_predict(test)

perf <- run_performance(pred) 
perf

performance <- perf$performance
names(performance) <- perf$model_id
performance

model <- as.data.frame(sapply(performance, "c"))

comp_perf <- compare_performance(pred)
comp_perf$recommend_model

plot_performance(pred)




fit <- rpart(ReadmissionDaysGroup~., data = train, method = 'class')
rpart.plot(fit)






