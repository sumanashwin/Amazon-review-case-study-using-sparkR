#---------------------------------------------------------------------------------------------------------------
#                                       AMAZON REVIEW CASE STUDY
#----------------------------------------------------------------------------------------------------------------
# 1. Business Understanding
# 2. Data Understanding and problem solving methodology
# 3. Data Preparation
# 4. Exploratory data analysis
# 5. Conclusion
#-----------------------------------------------------------------------------------------------------------------

#1. Business Understanding

# Problem Statement:
#--------------------
#Being a manager at media analytics firm , want to start a new product line to boost revenue.
# There are three options of product categories to choose from - CDs and Vinyl, Movies_TV and Ebooks (Kindle).

# Goal of the case study:
#-----------------------
# Specifically want to use a gigantic data set to identify:
# 
# Which product category has a larger market size
# Which product category is likely to be purchased heavily
# Which product category is likely to make the customers happy after the purchase

#2. Data Understanding and problem solving methodology

#Data source:
#-----------
#. Here is the home page of the data[http://jmcauley.ucsd.edu/data/amazon/]. We will be using the 5-core data, where each user and product have at least 5 reviews.
# Each of the files in the above link is a .JSON file which will have to be exported to AWS S3 bucket with HDFS commands.
# There are a total of 9 attributes namely reviewerID,asin,reviewerName,helpful,reviewText,overall,summary,unixReviewTime,reviewTime

#Please note that the data was first imported to the master node HDFS , unzipped and then exported to the S3 bucket on AWS
#using the instructions provided by Upgrad.

#-------------------------------------------------------------------------------------------------------------------
#                                             DATA SOURCING , DATA PREPARATION AND  DATA UNDERSTANDING
#-------------------------------------------------------------------------------------------------------------------

# Set AWS S3 access credentials
Sys.setenv("AWS_ACCESS_KEY_ID" = "AKIAIT22DATGNGAJWA2Q", "AWS_SECRET_ACCESS_KEY" = "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", "AWS_DEFAULT_REGION" = "us-west-2")


#Loading the required packages

library(SparkR)
library(sparklyr)
library(ggplot2)
install.packages("formattable")
library(formattable)
install.packages("varhandle")
library(varhandle)


#Initializing the spark session and storing it in the  sc variable

sc = sparkR.session(master='local')

#Loading the data into Spark DF from S3
Movies_TV_df <- SparkR::read.df("s3a://amazonreviewsuman/reviews_Movies_and_TV_5.json",header = T,"json")
kindle_df <- SparkR::read.df("s3a://amazonreviewsuman/reviews_Kindle_Store_5.json",header = T,"json")
cds_df    <- SparkR::read.df("s3a://amazonreviewsuman/reviews_CDs_and_Vinyl_5.json",header = T,"json")


#Understanding the dimensionality of the datasets
#-------------------------------------------------------
head(Movies_TV_df)
head(kindle_df)
head(cds_df)

str(Movies_TV_df)

# SparkDataFrame': 9 variables:
#  $ asin          : chr "0005019281" "0005019281" "0005019281" "0005019281" "0005019281" "0005019281"
#  $ helpful       : arr list(0, 0) list(0, 0) list(0, 0) list(0, 0) list(0, 0) list(0, 0)
#  $ overall       : num 4 3 3 5 4 5
#  $ reviewText    : chr "This is a charming version of the classic Dicken's tale.  Henry Winkler makes a good showing as 
# $ reviewTime    : chr "02 26, 2008" "12 30, 2013" "12 30, 2013" "02 13, 2008" "12 22, 2013" "11 6, 2013"
# $ reviewerID    : chr "ADZPIG9QOCDG5" "A35947ZP82G7JH" "A3UORV8A9D5L2E" "A1VKW06X1O2X7V" "A3R27T4HADWFFJ" "A2L0G56BNOTX
#  $ reviewerName  : chr "Alice L. Larson "alice-loves-books"" "Amarah Strack" "Amazon Customer" "Amazon Customer "Softmil
#  $ summary       : chr "good version of a classic" "Good but not as moving" "Winkler's Performance was ok at best!" "It'
# $ unixReviewTime: num 1203984000 1388361600 1388361600 1202860800 1387670400 1383696000


str(kindle_df)

# 'SparkDataFrame': 9 variables:
#   $ asin          : chr "B000F83SZQ" "B000F83SZQ" "B000F83SZQ" "B000F83SZQ" "B000F83SZQ" "B000F83SZQ"
# $ helpful       : arr list(0, 0) list(2, 2) list(2, 2) list(1, 1) list(0, 1) list(0, 0)
# $ overall       : num 5 4 4 5 4 4
# $ reviewText    : chr "I enjoy vintage books and Movies_TV so I enjoyed reading this book.  The plot was unusual.  Don't t
#  $ reviewTime    : chr "05 5, 2014" "01 6, 2014" "04 4, 2014" "02 19, 2014" "03 19, 2014" "05 26, 2014"
#  $ reviewerID    : chr "A1F6404F1VG29J" "AN0N05A9LIJEQ" "A795DMNCJILA6" "A1FV0SX13TWVXQ" "A3SPTOKDG7WBLN" "A1RK2OCZDSGC6
# $ reviewerName  : chr "Avidreader" "critters" "dot" "Elaine H. Turley "Montana Songbird"" "Father Dowling Fan" "ubavka 
#  $ summary       : chr "Nice vintage story" "Different..." "Oldie" "I really liked it." "Period Mystery" "Review"
#  $ unixReviewTime: num 1399248000 1388966400 1396569600 1392768000 1395187200 1401062400

str(cds_df)
# 'SparkDataFrame': 9 variables:
#   $ asin          : chr "0307141985" "0307141985" "0307141985" "0307141985" "0307141985" "073890015X"
# $ helpful       : arr list(14, 15) list(2, 2) list(38, 38) list(15, 16) list(11, 12) list(0, 0)
# $ overall       : num 5 4 5 5 5 5
# $ reviewText    : chr "I don't know who owns the rights to this wonderful production, but considering all the other Ran
#  $ reviewTime    : chr "10 6, 2005" "11 23, 2011" "07 14, 2003" "11 6, 2003" "03 1, 2006" "09 11, 2013"
#  $ reviewerID    : chr "A3IEV6R2B7VW5Z" "A2H3ISQ4QB95XN" "A6GMEO3VRY51S" "A3E102F6LPUF1J" "A2JP0URFHXP6DO" "A31GBCW6YPY9
# $ reviewerName  : chr "J. Anderson" "Joseph Brando" "microjoe" "Richard J. Goldschmidt "Rick Goldschmidt"" "Tim Janson"
# $ summary       : chr "LISTEN TO THE PUBLIC!!!" "Rankin/Bass Does Thanksgiving!!" "Thanksgiving Holiday fun from Rankin
#  $ unixReviewTime: num 1128556800 1322006400 1058140800 1068076800 1141171200 1378857600

# examine the size
nrow(Movies_TV_df) # 1697533 
ncol(Movies_TV_df) # 9

nrow(kindle_df) # 982619 
ncol(kindle_df) # 9 

nrow(cds_df)  #  1097592
ncol(cds_df)  #  9

# lets first reduce the number of observations of the  data set using the helpfulness metric. 
#The dataset presents this score in the form of a tuple, where the first integer is the number that voted as helpful and the second one is the total number of votes. For example, if the helpfulness score reads "10 of 15 people found this helpful", the tuple will be (10, 15). For a given tuple, the helpfulness score will be (10/15). A good start is to consider those reviews where at least 10 people have voted (i.e., the 2nd number in the tuple is greater than 10).
#We will also remove columns which are not required for our analysis like the summary, reviewText and reviewerName
# #IMPORTANT POINT# In addition to the above points. We have also taken into account the Business environment in which the product
#investment has to be made. The data available for the Kindle product category is from  year2010 ONWARDS till 2013, where as 
#the other 2 product categories have data from year 1999 onwards till 2013

#In order to provide a level playing field for the Kindle, we have filtered the data for all 3 product categories
# for the year greater than or equal 2010. We beleive this is a crucial observation in our analysis.


# Register as table
createOrReplaceTempView(Movies_TV_df,"Movies_TV") #Converting the Movies_TV_df to Movies_TV table
createOrReplaceTempView(kindle_df,"Kindle")  #Converting the kindle_df to kindle table
createOrReplaceTempView(cds_df,"CD_Vinyl")   #Converting the CD_Vinyl to CD_Vinyl table


#Creating flattened dataframe with all the useful metrics and having number of votes >10
amazon_Movies_TV <- SparkR::sql("select asin, overall, reviewTime, reviewerID, unixReviewTime, helpful[0] as helpful_positive_vote, helpful[1] as helpful_total_vote, helpful[0]/helpful[1] as helpfulness_score, length(reviewText) as reviewlength, length(summary) as summarylength from Movies_TV where helpful[1]>=10 and year(from_unixtime(unixreviewtime))>=2010 ")
amazon_CD_Vinyl <- SparkR::sql("select asin, overall, reviewTime, reviewerID, unixReviewTime, helpful[0] as helpful_positive_vote, helpful[1] as helpful_total_vote, helpful[0]/helpful[1] as helpfulness_score, length(reviewText) as reviewlength, length(summary) as summarylength from CD_Vinyl where helpful[1]>=10 and year(from_unixtime(unixreviewtime))>=2010")
amazon_Kindle <- SparkR::sql("select asin, overall, reviewTime, reviewerID, unixReviewTime, helpful[0] as helpful_positive_vote, helpful[1] as helpful_total_vote, helpful[0]/helpful[1] as helpfulness_score, length(reviewText) as reviewlength, length(summary) as summarylength from Kindle where helpful[1]>=10 and year(from_unixtime(unixreviewtime))>=2010")

#For the helfulness score analyis, lets derive a column to bin the length of review text in size of 1000 letters/bin
amazon_Movies_TV$reviewTextBin <- round(amazon_Movies_TV$reviewlength/1000,0)
amazon_CD_Vinyl$reviewTextBin <- round(amazon_CD_Vinyl$reviewlength/1000,0)
amazon_Kindle$reviewTextBin <- round(amazon_Kindle$reviewlength/1000,0)

#---------------------------------------------------------------------------
#Understanding the summary statistics for all 3 product categories
#---------------------------------------------------------------------------
#For Movies_TV
#----------------

summarystat_Movies_TV <- describe(amazon_Movies_TV, "overall", "reviewTime", "helpful_positive_vote","helpful_total_vote", "reviewlength","helpfulness_score")
showDF(summarystat_Movies_TV)

# +-------+------------------+----------+---------------------+------------------+------------------+------------------+
#   |summary|           overall|reviewTime|helpful_positive_vote|helpful_total_vote|      reviewlength| helpfulness_score|
#   +-------+------------------+----------+---------------------+------------------+------------------+------------------+
#   |  count|             51204|     51204|                51204|             51204|             51204|             51204|
#   |   mean|2.9978517303335677|      null|    18.66815092570893|29.834094992578706|1594.6743418482931|0.5758747341995673|
#   | stddev|1.6814665386876013|      null|     52.9702963755903|61.859288081271984|1843.0755882861379|0.3179796374420037|
#   |    min|               1.0|01 1, 2010|                    0|                10|                 0|               0.0|
#   |    max|               5.0|12 9, 2013|                 6084|              6510|             32299|               1.0|
#   +-------+------------------+----------+---------------------+------------------+------------------+------------------+

#For CD_Vinyl
#-----------------

summarystat_CD_Vinyl <- describe(amazon_CD_Vinyl, "overall", "reviewTime", "helpful_positive_vote","helpful_total_vote", "reviewlength","helpfulness_score")
showDF(summarystat_CD_Vinyl)
# +-------+------------------+----------+---------------------+------------------+------------------+------------------+
#   |summary|           overall|reviewTime|helpful_positive_vote|helpful_total_vote|      reviewlength| helpfulness_score|
#   +-------+------------------+----------+---------------------+------------------+------------------+------------------+
#   |  count|             20483|     20483|                20483|             20483|             20483|             20483|
#   |   mean|   3.4064834252795|      null|    16.50129375579749| 24.64863545379095| 1766.828491920129|0.6455789642510293|
#   | stddev|1.5889414213250426|      null|   24.629581469380955|27.460287460821668|1841.7023784373305|0.3094997273294354|
#   |    min|               1.0|01 1, 2010|                    0|                10|                 0|               0.0|
#   |    max|               5.0|12 9, 2013|                  991|              1005|             31981|               1.0|
#   +-------+------------------+----------+---------------------+------------------+------------------+------------------+
# 

#For Kindle
#----------------

summarystat_Kindle <- describe(amazon_Kindle, "overall", "reviewTime", "helpful_positive_vote","helpful_total_vote", "reviewlength","helpfulness_score")
showDF(summarystat_Kindle)

# +-------+------------------+----------+---------------------+------------------+------------------+-------------------+
#   |summary|           overall|reviewTime|helpful_positive_vote|helpful_total_vote|      reviewlength|  helpfulness_score|
#   +-------+------------------+----------+---------------------+------------------+------------------+-------------------+
#   |  count|             21219|     21219|                21219|             21219|             21219|              21219|
#   |   mean|3.7849097506951317|      null|    16.99344926716622|20.168763843724964| 1106.437485272633| 0.8322482177484966|
#   | stddev|1.4975445403724423|      null|    32.64957755006587| 34.94758383123548|1084.7460195550532|0.21375856029862392|
#   |    min|               1.0|01 1, 2010|                    0|                10|                 0|                0.0|
#   |    max|               5.0|12 9, 2013|                 2350|              2537|             17830|                1.0|
#   +-------+------------------+----------+---------------------+------------------+------------------+-------------------

#----------------------------------------------------------------------------------
#Lets convert the above dataframes to tables for our further analysis
createOrReplaceTempView(amazon_Movies_TV,"amazon_Movies_TV")
createOrReplaceTempView(amazon_CD_Vinyl,"amazon_CD_Vinyl")
createOrReplaceTempView(amazon_Kindle,"amazon_Kindle")

#-------------------------------------------------------------------------------------------------------------------
#                                     PROBLEM SOLVING[EDA] BEGINS HERE
#-------------------------------------------------------------------------------------------------------------------
#Intuition about problem solving
#Now that we have got a hang of the data , lets proceed with the key proxy metrics to determine
# 1.number of reviews' as a proxy for the number of products sold (i.e. the ratio of number of reviews of two product categories will reflect, approximately, the ratio of the number of units sold - you are not trying to estimate the absolute numbers anyway, you want to compare the metrics across categories).
# 2. to estimate the market size, you can use the number of reviewers as a proxy.
# 3. you can define some metrics to proxy customer satisfaction as well. For instance, if a customer has written a long review and rated the product 5, it indicates that he/she is quite happy with the product.




#PROBLEM STATEMENT 1: Which product category has a larger market size
#--------------------------------------------------------------------

#Lets use the number of unique reviewers as the proxy metric to determine the market size for each of the product category


# Number if unique reviewers for the CD_Vinyl product category

CDVinyl_totalreviewer <- collect(SparkR::sql("SELECT count(distinct reviewerID) as CD_Vinyl_reviewer from amazon_CD_Vinyl"))
CDVinyl_totalreviewer

# CD_Vinyl_reviewer
# 1              8776

# Number if unique reviewers for the Movies_TV product category

Movies_TV_totalreviewer <- collect(SparkR::sql("SELECT count(distinct reviewerID) as Movies_TV_reviewer from amazon_Movies_TV"))
Movies_TV_totalreviewer

# Movies_TV_reviewer
# 1          21341

# Number if unique reviewers for the Kindle product category

Kindle_totalreviewer <- collect(SparkR::sql("SELECT count(distinct reviewerID) as Kindle_reviewer from amazon_Kindle"))
Kindle_totalreviewer

# Kindle_reviewer
# 1          12091

amazon_reviewer_dataframe <- t(cbind(CDVinyl_totalreviewer,Movies_TV_totalreviewer,Kindle_totalreviewer))
amazon_reviewer_dataframe <- data.frame(cbind(Product_category=rownames(amazon_reviewer_dataframe),amazon_reviewer_dataframe)) #creating a dataframe containing total reviewers 
amazon_reviewer_dataframe$X1 <- unfactor(amazon_reviewer_dataframe$X1)
View(amazon_reviewer_dataframe)
ggplot(amazon_reviewer_dataframe,aes(x =reorder(Product_category,-as.numeric(X1)),y=X1))+geom_bar(stat="identity",position = "dodge")+ylab("Total reviewers")+xlab("Product category")+ggtitle("Total reviewers for all product categories")

#CONCLUSION 1: Movies_TV product categories have a total of 21341 total unique reviewers and is the highest among the 3 product categories



#PROBLEM STATEMENT 2: Which product category is likely to be purchased heavily
#-----------------------------------------------------------------------------

#Lets use the number of products reviews  as a proxy metric to see the customers buying intention(number of products sold)

#Number of products sold for the CD_Vinyl product category

CDVinyl_totalreviews <- collect(SparkR::sql("SELECT count(*) as CD_Vinyl_totalreviews from amazon_CD_Vinyl"))
CDVinyl_totalreviews

# CD_Vinyl_totalreviews
# 1                 20483

#Number of products sold for the Movies_TV product category
Movies_TV_totalreviews <- collect(SparkR::sql("SELECT count(*) as Movies_TV_totalreviews from amazon_Movies_TV"))
Movies_TV_totalreviews

# Movies_TV_totalreviews
# 1              51204

#Number of products sold for the Kindle product category
Kindle_totalreviews <- collect(SparkR::sql("SELECT count(*) as Kindle_totalreviews from amazon_Kindle"))
Kindle_totalreviews

# Kindle_totalreviews
# 1                21219


amazon_reviews_dataframe <- t(cbind(CDVinyl_totalreviews,Movies_TV_totalreviews,Kindle_totalreviews))
amazon_reviews_dataframe <- data.frame(cbind(Product_category=rownames(amazon_reviews_dataframe),amazon_reviews_dataframe)) #creating a dataframe containing total reviews
amazon_reviews_dataframe$X1 <- unfactor(amazon_reviews_dataframe$X1)
View(amazon_reviews_dataframe)
ggplot(amazon_reviews_dataframe,aes(x=reorder(Product_category,-X1),y=X1))+geom_bar(stat="identity",position = "dodge")+ylab("Total reviews")+xlab("Product category")+ggtitle("Total reviewes for all product categories")

#CONCLUSION 2: Movies_TV product categories have a total of 51204 total reviews and implying its is the highest sold product  among the 3 product categories


# PROBLEM STATEMENT 3:Which product category is likely to make the customers happy after the purchase
#---------------------------------------------------------------------------------------------------

#There are couple of aspects to determine which product delights the customer. Lets tackle them one by one

#a) Let's investigate the correlation between average review length and product rating and also correlation between product rating and positive sentiments percentage

#For CD_Vinyl product category
#--------------------------------

CD_Vinyl_rating <- collect(SparkR::sql("SELECT COUNT(overall) as total_Ratings, AVG(reviewlength) as num_of_letters , overall as overall_Rating
                                       FROM amazon_CD_Vinyl
                                       GROUP BY overall"))
CD_Vinyl_rating$positive_sentiments_percent <- formattable::percent(CD_Vinyl_rating$total_Ratings/CDVinyl_totalreviews[[1]])

CD_Vinyl_rating
# total_Ratings      num_of_letters overall_Rating positive_sentiments_percent
# 1          4212       886.3435              1                      20.56%
# 2          2911      2427.5490              4                      14.21%
# 3          2611      1825.8062              3                      12.75%
# 4          2553      1323.2981              2                      12.46%
# 5          8196      2104.0159              5                      40.01%

ggplot(CD_Vinyl_rating,aes(x=overall_Rating,y=num_of_letters))+geom_bar(stat="identity",position = "dodge")

#Clearly , higher ratings have a longer reviews which indicates positive sentiments
# There are total of 8196   (highest) ratings rated for 5 for CD_Vinyl product with an average review length of 2104.0159 letters
# Positive sentiments percentage as calculated by total_Ratings/total_reviews for this product is  40.01% for a rating of "5"

# For Movies_TV category
#-----------------------------
Movies_TV_rating <- collect(SparkR::sql("SELECT COUNT(overall) as total_Ratings, AVG(reviewlength) as num_of_letters , overall as overall_Rating
                                        FROM amazon_Movies_TV
                                        GROUP BY overall"))
Movies_TV_rating$positive_sentiments_percent <- formattable::percent(Movies_TV_rating$total_Ratings/Movies_TV_totalreviews[[1]])
Movies_TV_rating

#       total_Ratings num_of_letters overall_Rating positive_sentiments_percent
# 1         16739       962.2697              1                      32.69%
# 2          6618      2266.4577              4                      12.92%
# 3          5625      1936.5851              3                      10.99%
# 4          5898      1606.2947              2                      11.52%
# 5         16324      1848.7895              5                      31.88%

ggplot(Movies_TV_rating,aes(x=overall_Rating,y=num_of_letters))+geom_bar(stat="identity",position = "dodge")

#Clearly , higher ratings have a longer reviews which indicates positive sentiments
# There are total of  16739 (highest) ratings rated for 1  for Movies_TV product with an average review length of 962.2letters
# Positive sentiments percentage as calculated by total_Ratings/total_reviews for this product is 32.69%% for a rating of "1"

# For Kindle category
#-----------------------------
Kindle_rating <- collect(SparkR::sql("SELECT COUNT(overall) as total_Ratings, AVG(reviewlength) as num_of_letters , overall as overall_Rating
                                     FROM amazon_Kindle
                                     GROUP BY overall"))
Kindle_rating$positive_sentiments_percent <- formattable::percent(Kindle_rating$total_Ratings/Kindle_totalreviews[[1]])
Kindle_rating

# total_Ratings num_of_letters overall_Rating positive_sentiments_percent
# 1          2951       876.0335              1                      13.91%
# 2          2996      1285.5975              4                      14.12%
# 3          2124      1313.6596              3                      10.01%
# 4          2245      1227.1051              2                      10.58%
# 5         10903      1054.3527              5                      51.38%

ggplot(Kindle_rating,aes(x=overall_Rating,y=num_of_letters))+geom_bar(stat="identity",position = "dodge")

#Clearly , higher ratings have a longer reviews which indicates positive sentiments
# There are total of    10903(highest) ratings rated for 5 for Kindle product with an average review length of 1054.3 letters
# Positive sentiments percentage as calculated by total_Ratings/total_reviews for this product is 51.38%  for a rating of "5"


#Conclusion 3a: Kindle product category has the highest positive sentiment percentage of 51.52% among the 3 product categories which is correlated with higher customer statisfaction
# if people are writing more reviews chances are that product is good for sales by virtue of higher rating. 

#-----------------------------------------------------------------------------------------------------------------------------
#b. You may also divide the reviews into bins of increasing length (of review text). Now, find the average helpfulness score for all reviews in that particular bin. Can you spot a trend? Are longer reviews more helpful on an average? Should you give a higher weightage to longer reviews?

#We have already created a column by name "reviewTextBin" in the data preparation phase. We will now find the average helpfulness score and average overall rating in each of these bins


#For CD_Vinyl product category
#--------------------------------
CD_Vinyl_helpful <-  collect(SparkR::sql("SELECT  round(Avg(helpfulness_score),2) as avg_helpful_score ,round(avg(overall),2) as average_rating,reviewTextBin   
                                         FROM amazon_CD_Vinyl
                                         GROUP BY reviewTextBin
                                         ORDER BY avg_helpful_score DESC"))

CD_Vinyl_helpful
head(CD_Vinyl_helpful)

# avg_helpful_score average_rating reviewTextBin
# 1              0.98           5.00            28
# 2              0.94           5.00            21
# 3              0.90           5.00            29
# 4              0.90           4.50            32
# 5              0.88           4.22            13
# 6              0.86           5.00            27

ggplot(CD_Vinyl_helpful,aes(x=reviewTextBin,y=avg_helpful_score))+geom_point() + geom_smooth(method = "lm")

#There is a gradual increase in the increment of the helpful score as the text bin size increases

ggplot(CD_Vinyl_helpful,aes(x=reviewTextBin,y=average_rating))+geom_point() + geom_smooth(method = "lm")

#Clearly higher ratings are correlated with longer review length


#For Movies_TV product category
#--------------------------------

Movies_TV_helpful <-  collect(SparkR::sql("SELECT  round(Avg(helpfulness_score),2) as avg_helpful_score ,round(avg(overall),2) as average_rating,reviewTextBin   
                                          FROM amazon_Movies_TV
                                          GROUP BY reviewTextBin
                                          ORDER BY avg_helpful_score DESC"))

Movies_TV_helpful
head(Movies_TV_helpful)

#     avg_helpful_score     average_rating  reviewTextBin
# 1              1.00           4.00            27
# 2              1.00           5.00            26
# 3              0.97           5.00            21
# 4              0.88           4.00            31
# 5              0.83           4.67            29
# 6              0.79           4.00            16

ggplot(Movies_TV_helpful,aes(x=reviewTextBin,y=avg_helpful_score))+geom_point() + geom_smooth(method = "lm")

#There is a gradual increase in the increment of the helpful score as the text bin size increases

ggplot(Movies_TV_helpful,aes(x=reviewTextBin,y=average_rating))+geom_point() + geom_smooth(method = "lm")

#Clearly higher ratings are correlated with longer review length

#For Kindle product category
#--------------------------------

Kindle_helpful <-  collect(SparkR::sql("SELECT  round(Avg(helpfulness_score),2) as avg_helpful_score ,round(avg(overall),2) as average_rating,reviewTextBin   
                                       FROM amazon_Kindle
                                       GROUP BY reviewTextBin
                                       ORDER BY avg_helpful_score DESC"))
Kindle_helpful
head(Kindle_helpful)


# avg_helpful_score average_rating reviewTextBin
# 1              0.94           4.00            10
# 2              0.88           3.74             3
# 3              0.88           3.87             2
# 4              0.88           3.90             5
# 5              0.86           3.86             1
# 6              0.86           3.69             4

ggplot(Kindle_helpful,aes(x=reviewTextBin,y=avg_helpful_score))+geom_point() + geom_smooth(method = "lm")

#Contrary to the other product categories, for the Kindle, the overall trend is downward , as in lower helpful score has
#longer reviews

ggplot(Kindle_helpful,aes(x=reviewTextBin,y=average_rating))+geom_point() + geom_smooth(method = "lm")

#Contrary to the other product categories, for the Kindle, the overall trend is downward. Lower ratings are correlated with longer reviews

#Conclusion 3b
# 1. Kindle being e-Books have a targeted audience who read/write books. a) The collection on Kindle is too good and people have less complaints. b) The folks who come to Kindle are refined, knowledgeable and know what they need to read and perhaps the authors as well. They only complain and write a lot (negative review) when they don;t get that they expected.
# 2. Movies, CDs are something that people get engaged on when they play/go through the entire CD/Movies. the chances are people who like them are the ones who watch it fully and have lot of good points to write in the review (e.g the differnt plots, climax in the movie vs audio synch, pitch ratio and so on) . the folks who dont like make a choice in the first few minutes and disengage themselves. 
#So, they dont have much to write about it which are also not helpful to new folks who did not watch the CD/Movies yet

#---------------------------------------------------------------------------

#c. Lets examine the average helpfulness score over the 5 different rating levels, and explore if the helpfulness scores vary. 

#For CD_Vinyl product category
#--------------------------------
CD_Vinyl_helpful_rating <-  collect(SparkR::sql("SELECT  round(Avg(helpfulness_score),2) as avg_helpful_score ,overall as rating_level  
                                                FROM amazon_CD_Vinyl
                                                GROUP BY rating_level
                                                ORDER BY avg_helpful_score DESC"))

CD_Vinyl_helpful_rating

#       avg_helpful_score rating_level
# 1              0.85            5
# 2              0.80            4
# 3              0.56            3
# 4              0.43            2
# 5              0.32            1
ggplot(CD_Vinyl_helpful_rating,aes(rating_level,avg_helpful_score))+geom_bar(stat="identity",position="dodge")

#Evidently, higher ratings are positively correlated with higher helpful scores

#For Movies_TV product category
#--------------------------------
Movies_TV_helpful_rating <-  collect(SparkR::sql("SELECT  round(Avg(helpfulness_score),2) as avg_helpful_score ,overall as rating_level  
                                                 FROM amazon_Movies_TV
                                                 GROUP BY rating_level
                                                 ORDER BY avg_helpful_score DESC"))

Movies_TV_helpful_rating

#       avg_helpful_score rating_level
# 1              0.79            4
# 2              0.79            5
# 3              0.58            3
# 4              0.43            2
# 5              0.34            1
ggplot(Movies_TV_helpful_rating,aes(rating_level,avg_helpful_score))+geom_bar(stat="identity",position="dodge")

#Evidently, higher ratings are positively correlated with higher helpful scores


#For Kindle product category
#--------------------------------
Kindle_helpful_rating <-  collect(SparkR::sql("SELECT  round(Avg(helpfulness_score),2) as avg_helpful_score ,overall as rating_level  
                                              FROM amazon_Kindle
                                              GROUP BY rating_level
                                              ORDER BY avg_helpful_score DESC"))

Kindle_helpful_rating

#     avg_helpful_score rating_level
# 1              0.91            5
# 2              0.90            4
# 3              0.82            3
# 4              0.70            2
# 5              0.60            1
ggplot(Kindle_helpful_rating,aes(rating_level,avg_helpful_score))+geom_bar(stat="identity",position="dodge")

#Evidently, higher ratings are positively correlated with higher helpful scores

#Conclusion 3c : Higher ratings having a higher helpful score.

#CONCLUSION 3: Considering the conclusions from 3a,3b and 3c , kindle is likely to make customers happy after purchase


#-----------------------------------------------------------------------------------------------------------
#                                     OVERALL SUMMARY AND RECOMMENDATIONS
#-----------------------------------------------------------------------------------------------------------


#  To put it all into perspective from our detailed analysis above

# 1. Which product category has a larger market size 
#(From CONCLUSION 1) : Its the "Movies and TV" product category. 

# 2. Which product category is likely to be purchased heavily
#(From CONCLUSION 2) : Its the "Movies and TV" product category.

# 3. Which product category is likely to make the customers happy after the purchase
#(From CONCULSION 3)  : Its the "Kindle" product category

#WHICH PRODUCT CATEGORY SHOULD THE STORE MANAGER INVEST IN 

# In summary, as movies stand out from the rest of the product in all 3 categories the media company if it's risk-averse should focus on movies. 
# As Kindle also has shown a promising upward trend in terms of customer satisfaction, the media company could invest in small portion of this product line


#RECOMMENDATIONS:
#1. If the media company wishes to invest in Kindle product category in addition to the "Movies and TV" product category
# they should design compelling ad campaigns for Kindle , as the publicity is inherent in the Movies product line and its handles by production house
#Authors typically do not do any self campaigning. Also to promote the purchase of Kindle, free e books can be given to motivate the buyer

#2.  We recommend the media company empower and encourage its buyers to write reviews (good or bad) as that seems to boost sales of products. 


#--------------------------------------------END OF OUR ANALYSIS-------------------------------------------------------------------------------------------------------------------