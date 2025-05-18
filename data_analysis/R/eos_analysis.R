library(ggplot2)
library(dplyr)
library(tidyr)
library(gridExtra)
library(grid)

setwd('FILEPATH')

#data is with column name
#check put column names in the first row
data <- read.csv('FILENAME', na.strings = c("", "NA"))




#creator rank
y = count(data, creator)

y <- y[order(-y$n),] 

y2 <- subset(y, n < 100000 & n > 5200)


#creator rank
ggplot(y2, aes(x=reorder(creator, n), y=n))+
  geom_bar(stat = "identity", fill = "#42b9f4") +
  xlab('') +
  ylab('accounts created') +
  theme_minimal() +
  theme(axis.text.x = element_text(colour="grey20",size=14),
        axis.title.x = element_text(margin = margin(t = 20, r = 20, b = 10, l = 10)),
        axis.text.y = element_text(colour="grey20",size=15),
        text = element_text(size=15)) +
  coord_flip()





#growth rate (excluding genesis)

data2 = subset(data, group1 != "genesis" | is.na(group1))

data2$Date_adj = as.POSIXlt(data2$date_created, 
                                          format="%Y-%m-%d")

data2$Date_m = format(data2$Date_adj, "%m")

data2$Date_m = as.integer(as.character(data2$Date_m))
#factor
data2$Date_mf = as.factor(data2$Date_m)


data2$Date_m.bucket <- cut(
  data2$Date_m, c(6, 7, 8, 9, 10, 11))


ggplot(aes(x = Date_mf), data = subset(data2, Date_m < 11)) +
  geom_bar(stat = 'count', fill = "#42b9f4") +
  xlab('2018') +
  ylab('accounts created') +
  theme(text = element_text(size=14),
        axis.title.y = element_text(margin = margin(t = 0, r = 20, b = 0, l = 0)))
  



ggplot(aes(x = Date_mf), data = subset(data2, Date_m < 11)) +
  geom_bar(stat = 'count', fill = "#42b9f4") +
  xlab('June to October 2018') +
  ylab('') +
  theme_minimal() +
  theme(axis.text.x = element_text(colour="grey20",size=14),
        axis.title.x = element_text(margin = margin(t = 20, r = 20, b = 10, l = 10)),
        axis.text.y = element_text(colour="grey20",size=14),
        text = element_text(size=14))





#voter percentage
bp<- ggplot(data, aes(x="",  fill=group2))+
  geom_bar(width = 100, stat = "count") +
  scale_y_continuous(labels = scales::comma)
bp

pie <- bp + coord_polar("y", start=0)
pie







#premium breakdown

pacct = subset(data, group1 == "p")

pacct$domain <- sub('.*\\.', '', pacct$name)

pacct <- within(pacct, 
                   domain <- factor(domain, 
                                      levels=names(sort(table(domain), 
                                                        decreasing=FALSE))))



bp<- ggplot(pacct, aes(x="",  fill=domain))+
  geom_bar(width = 1, stat = "count") +
  scale_y_continuous(labels = scales::comma)
bp

pie <- bp + coord_polar("y", start=0)
pie




ggplot(aes(x = domain), data = pacct) +
  geom_histogram(stat = "count", fill = "#42b9f4") +
  theme_minimal() +
  theme(axis.text.x = element_text(colour="grey20",size=13),
        axis.text.y = element_text(colour="grey20",size=13)) +
  xlab('') +
  ylab('') + 
  coord_flip()




#creator frequency

y3 <- subset(y, n < 100000 )

#1-10
ggplot(y3, aes(n)) +
  geom_histogram(binwidth = 1, fill = "#42b9f4")+
  scale_x_continuous(limits = c(-0.1, 11), breaks = seq(0, 11)) +
  xlab('Accounts created per creator') +
  ylab('Count') +
  theme_minimal() +
  theme(axis.text.x = element_text(colour="grey20",size=14),
        axis.title.x = element_text(margin = margin(t = 20, r = 20, b = 10, l = 10)),
        axis.text.y = element_text(colour="grey20",size=15),
        text = element_text(size=15))




#10 - 100
ggplot(y3, aes(n)) +
  geom_histogram(binwidth = 1, fill = "#42b9f4")+
  scale_x_continuous(limits = c(10, 100), breaks = seq(10, 100, by=10)) +
  xlab('Accounts created per creator') +
  ylab('Count') +
  theme_minimal() +
  theme(axis.text.x = element_text(colour="grey20",size=14),
        axis.title.x = element_text(margin = margin(t = 20, r = 20, b = 10, l = 10)),
        axis.text.y = element_text(colour="grey20",size=15),
        text = element_text(size=15))

