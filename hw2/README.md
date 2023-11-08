# Project 2 - Average Temps
JEH190000
9/12/2023

Implementations of the 4 requested mapreduce programs for the 2nd project.

### Run A
hadoop jar AvgRegionalTemps.jar AvgRegionalTemps <input-file> /avgRegionalTempsOut

### Run B
hadoop jar AvgYearlyTemps.jar AvgYearlyTemps <input-file> /avgYearlyTempsOut

### Run C
hadoop jar AvgSpainTemps.jar AvgSpainTemps <input-file> /avgSpainTempsOut

### Run D (order of input files is important)
hadoop jar AvgTempsJoin.jar AvgTempsJoin <input-file-temperatures> <input-file-country-list> /avgTempsJoinOut