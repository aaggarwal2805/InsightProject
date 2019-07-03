# That's My Spot

---
###Introduction
This project aims at building a data pipeline using historical on-street parking sensor data to analyze and suggest the probability of getting a free parking spot at any given day and hour for the city of Melbourne. The data set contains events with fields like Device id, location, arrival time and departure time of the vehicle. This historical data has events from 2011-2017. Data transformations run on Spark cluster and stored in PostgreSql. And the final dashboard is published using Amazon Quicksight.

---
###Problem
A study says 30% of cars on a road are looking for parking availability. It makes it all the more difficult to find a free spot in a congested city like Melbourne. This leads to many problems like traffic congestion, frustration for the drivers and incalculable amounts of wasted fuel and carbon emissions. It is necessary to have a parking aid for drivers that suggests probability of getting a free parking spot at any given day and time.

---
###Data Pipeline
