# Project Plan

## Title
<!-- Give your project a short title. -->
Are the infamous NBA 2k ratings matching their stats and salaries

## Main Question

<!-- Think about one main question you want to answer based on the data. -->
1. Given advanced stats and salaries are NBA player matching their NBA 2k rating published prior to the season.

## Description

<!-- Describe your data science project in max. 200 words. Consider writing about why and how you attempt it. -->
The NBA is one of the most stats heavy professional sport league in the world. The teams invest highly in analytics and tracking of different metrics, which results in a bunch of different available stats. One fan favourite statistic is the publication of the NBA 2k ratings at the beginning of each season for the eponymous video game. While only relevant for the video game itself, the ratings tend to be heavily discussed by fans and TV. But how accurate are these ratings in hindsight with respect to advanced stats, and to add something extra to this project, to salary. We want to look at the NBA 2k 2020 players stats and analyze them using the stats from the subsequent season.

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource1: NBA 2k20 player dataset
* Metadata URL: https://www.kaggle.com/datasets/isaienkov/nba2k20-player-dataset
* Data URL: https://www.kaggle.com/datasets/isaienkov/nba2k20-player-dataset/download?datasetVersionNumber=9
* Data Type: CSV
* License: CC0: Public Domain

The NBA 2k ratings for each player for NBA 2k20, which means prior to the 2020/2021 NBA season.

### Datasource2: 1991-2021 NBA Stats
* Metadata URL: https://www.kaggle.com/datasets/vivovinco/19912021-nba-stats 
* Data URL: https://www.kaggle.com/datasets/vivovinco/19912021-nba-stats/download?datasetVersionNumber=4
* Data Type: CSV
* License: CC BY 4.0 DEED

Player Information, Game Statistics, Shooting Effiency and Advanced Statistics for every player for the seasons 1991 to 2021.

## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. Create data download [i1]
2. Build data pipeline and preprocess data [i2]
3. Write test shell script [i3]
4. Compute the metrics [i4]
5. Compare metrics, salary with NBA 2k rating [i5]
6. Generate the plots [i6]
7. (Optional) Check own metrics by also including the previous season [i7]
7. (Optional) Add python test script to check for duplicates etc. 

[i1]: https://github.com/Christoph-Jung/made-ws2324/issues/1 
[i2]: https://github.com/Christoph-Jung/made-ws2324/issues/2 
[i3]: https://github.com/Christoph-Jung/made-ws2324/issues/3 