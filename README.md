# IS3107 Project Group 9 - Stock Price Prediction Data Pipeline
<br />

## Objective
The stock market is a complex and ever-changing environment, influenced by many factors ranging from global economic events to opinions of people on social media, which makes it challenging to predict stock prices. Traditionally, investors have relied on technical analysis to make investment decisions, but these methods have their limitations and may not provide accurate predictions. Aside from industry trends, economic information and news regarding the company, there are other external factors that can affect changes in a stock’s price. Nowadays, social media have become a prevalent part of our lives and it is clear that people’s opinions and emotions expressed online can have a significant impact on market movements. The short squeeze on GameStop’s GME stock in 2021 is a good example. It was initially triggered by the r/WallStreetBets subreddit, where traders shared their bullish opinions and encouraged others to join in. Our project aims to build a data pipeline to extract financial data and perform sentiment analysis on popular social media platforms like Twitter to aid in predicting future stock prices and make more informed decisions on selling and buying stocks.


## Tools & Technologies
- Cloud - [Google Cloud Platform](https://cloud.google.com/free/?utm_source=google&utm_medium=cpc&utm_campaign=japac-SG-all-en-dr-BKWS-all-pkws-trial-EXA-dr-1605216&utm_content=text-ad-none-none-DEV_c-CRE_649077641201-ADGP_Hybrid%20%7C%20BKWS%20-%20EXA%20%7C%20Txt%20~%20GCP_General_gcp_main-KWID_43700075274235034-aud-970366092687%3Akwd-42926176582&userloc_9062542-network_g&utm_term=KW_cloud%20platform%20google&gclid=CjwKCAjw__ihBhADEiwAXEazJjiZ_yJ9aCxMALA9XniM1WcMpsVMAZ_ugUE9ozmS6xx3Ccs662clTxoCZd8QAvD_BwE&gclsrc=aw.ds)
- Data Storage - [BigQuery](https://cloud.google.com/bigquery)
- Workflow Management - [Apache Airflow](https://airflow.apache.org/)
- Language - [Python](https://www.python.org/)

## Data
- Yfinance for financial data
- Twitter for sentiment analysis data
<img src="/images/data preprocessing.jpg" width="900" height="330">

## Data Pipeline Architecture
<img src="/images/data pipeline architecture.jpg" width="900" height="441">

## Airflow DAGs
- Financial data DAG
<img src="/images/financial dag.png" width="900" height="250">

- Twitter data DAG
<img src="/images/twitter dag.png" width="900" height="400">

## Setup Guide
The setup guide can be found [here](https://docs.google.com/document/d/14zmkT6gOtd6UsKgw3HpDL7UJ8Ex2qINbNhAmDCfanO0/edit)

## Authors
- Elijah Koh Zhi Yong - [elijahkzy](https://github.com/elijahkzy)
- Hsin Zheng Wei - [HsinZhengWei](https://github.com/HsinZhengWei)
- Joshua Tan Zhi Yi - [joshualah](https://github.com/joshualah)
- Lee Jie Yi Estella - [eseutella](https://github.com/eseutella)
- Mark Lim Bo-Yang - [Marklim99](https://github.com/Marklim99)
