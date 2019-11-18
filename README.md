# Sample Works
**A few of the more interesting tasks I've undertaken**

## 1. Month-On-Month User Spends from month of 1st purchase
`1_FO.py`

This was a manual calculation performed every month to understand how users who had their first transaction in a month performed every month after. As mixpanel retention views only shows the number of users who "Did Something and THEN Did Something Else", we had to depend on the DB data combined with cohort info from mixpanel, followed by spreadsheet analysis to get the right view. After studying the requirement, I wrote this script to connect to Mixpanel, query for the right user IDs, connect to the DB, query the same users for their user info, query the transactions table to get their orders, and perform aggregations to arrive at the final report, as demonstrated in the file `1_SampleResultOfFO.xlsx`

## 2. Dynamic Threshold calculator for certain metrics to set Alarms in unusual cases
`2_ThresholdAnalysis.pdf`

We have occasional cases where some users may discover loopholes / errors in codes or constructs that lead to rather large amounts of losses in very short durations in time. To counter this, I set up a system that monitors the cumulative GMV generated / Discounts used / Credits rewarded etc. across an hour and compares this against an allowable threshold, alerting key persons in cases of overages. This file is the study conducted based on which an 80% figure was selected for the thresholds for each hour.
#### Process:
1. Get all unit TXNs across a 2 month period
2. Divide all TXNs up till 15 as FirstHalf and latter as SecondHalf (As most campaigns run in the first half of the month, there will be differences in TXN behaviour)
3. Aggregate metrics for each hour of each half of the month separately (This gives us 2x24 aggregations per month per metric)
4. Explore the data points of each hour to identify what value 80% of values fall under
5. Save thresholds as a csv with HalfOfMonth, Hour, Metric, Threshold as the columns

## 3. The Daily Jumble Game (Personal Project)
[Github Link](https://github.com/staanz/string_exercises/blob/master/puzzle_builder.py)

This came about as I enjoy word puzzles, and so, while automating a task to check no english words in chat messages, I came across the idea of building a web version of the popular newspaper game, Jumble. The specific component of the game, as linked above, was a piece of logic that was fun and interesting to develop. The actual web version of the game can be found [here](http://string-exercises.herokuapp.com)
