setwd('/Users/parulgaba/Desktop/Capstone-Ethos/ConfidentialData/csvdata/')
getwd()

library(VIM)
library('dplyr')
library('naniar')

item_master <- read.csv("item_master_for_imputation.csv")
count_null <- item_master['strap_color'] == ''
sum(count_null)
str(item_master)
summary(item_master)
#?kNN


#replace null with NA
item_master <-  item_master %>% replace_with_na(replace = list(strap_color = ''))
item_master <-  item_master %>% replace_with_na(replace = list(case_size_range = ''))
item_master <-  item_master %>% replace_with_na(replace = list(glass = ''))
item_master <-  item_master %>% replace_with_na(replace = list(dial_color = ''))
item_master <-  item_master %>% replace_with_na(replace = list(strap_type = ''))
item_master <-  item_master %>% replace_with_na(replace = list(case_shape = ''))
item_master <-  item_master %>% replace_with_na(replace = list(material = ''))
item_master <-  item_master %>% replace_with_na(replace = list(case_size = ''))
item_master <-  item_master %>% replace_with_na(replace = list(movement = ''))

sum(is.na(item_master$strap_color))

item_master <- kNN(item_master, variable = c("strap_color","case_size_range","glass",
                                             "dial_color","strap_type","case_shape",
                                             "material","case_size","movement"), k=10)

sum(is.na(item_master$case_size_range))
is_na(item_master)
sum(is.na(item_master$material))
#filter(item_master,item_no==5126964)

#update gender
count_null <- item_master['gender'] == ''
item_master <- within(item_master, gender[case_size > 38] <- 'Men')
item_master$gender[item_master$gender == ''] <- 'Women'
filter(item_master,item_no==5190050)

write.csv(item_master,'item_master_imputed.csv')





#https://datascienceplus.com/missing-value-treatment/
#https://rpubs.com/harshaash/KNN_imputation

#library(DMwR)
#library(knnImputation)
#?knnImputation
#knnOutput <- knnImputation(item_master[, names(item_master) %in% "strap_color"])  # perform knn imputation.
