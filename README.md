# That's My Spot\
![parking](https://github.githubassets.com/images/icons/emoji/unicode/1f17f.png?v8)\
A hassle-free look up for a free parking spot\
[Project Presentation](https://docs.google.com/presentation/d/e/2PACX-1vRx3JwcXMyzI5oMRg-U0YlYKli9a701LnyJgkUt_VIyiQj46o3NX4LrTNz-cjQkATUSKNnCJUELir70/pub?start=false&loop=false&delayms=3000)


## Introduction
This project aims at building a data pipeline using historical on-street parking sensor data to analyze and suggest the probability of getting a free parking spot at any given day and hour for the city of Melbourne. The data set contains events with fields like Device id, location, arrival time and departure time of the vehicle. This historical data has events from 2011-2017. Data transformations run on Spark cluster and stored in PostgreSql. And the final dashboard is published using Amazon Quicksight.


## Problem
A study says 30% of cars on a road are looking for parking availability. It makes it all the more difficult to find a free spot in a congested city like Melbourne. This leads to many problems like traffic congestion, frustration for the drivers and incalculable amounts of wasted fuel and carbon emissions. It is necessary to have a parking aid for drivers that suggests probability of getting a free parking spot at any given day and time.

## Solution
A visualization application that suggests the probability of getting an available spot using color code from Green to red, Green meaning high probability of getting a spot and Red meaning very low.
![Demo](https://github.com/aaggarwal2805/InsightProject/blob/master/docs/Selection_011.png)

## Data Pipeline
![Process](https://github.com/aaggarwal2805/InsightProject/blob/master/docs/Selection_010.png)

Data is downloaded from [City of Melbourne](https://data.melbourne.vic.gov.au/browse?limitTo=datasets&q=parking+sensor&sortBy=relevance) and saved to Amazon S3 buckets. Data is processed and transformed using pyspark dataframes and hive tables. The transformed data is written to a table in PostgreSql and visualized using Amazon Quicksight.
