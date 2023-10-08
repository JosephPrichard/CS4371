# Project 1 - WordCount
JEH190000
9/12/2023

Implementations of the 3 requested mapreduce programs for the first project.
Keep in mind the programs use a normalization function so words of special characters or different casing are treated as the same.

### Run Word Count
hadoop jar WordCount.jar WordCount <input-file> /outputWordCount

### Run Word Search
hadoop jar WordSearch.jar WordSearch <input-file> /outputWordSearch

### Run Top Word Search
hadoop jar TopWordSearch.jar TopWordSearch <input-file> /outputTopWordSearch