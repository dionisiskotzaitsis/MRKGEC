This folder contains the files to create the .csv files for uploading to the Neo4j database. 

1) The lat_long_expansion.py, breaks up the review date for the Day, Month, Season nodes and their relationships
2) The datetime_expansion.py, does the same thing for the review location.
3) You can split you data according to your needs to training and testing sets and create negative sampling via negativeSampling.py.
4) The json2csv.py, uses the dataset to create the needed csv files to be uploaded.
