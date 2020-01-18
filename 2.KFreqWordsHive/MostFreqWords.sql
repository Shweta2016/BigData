DROP DATABASE IF EXISTS WordFreq CASCADE;

CREATE DATABASE WordFreq;

USE WordFreq;

CREATE TABLE docs(words string);

LOAD DATA INPATH '/user/bigdata07/data_32GB.txt' INTO TABLE docs;

CREATE TABLE wordCount AS
SELECT word, count(*) AS count FROM
(SELECT explode(split(words, '\\W+')) AS word FROM docs) w
GROUP BY word;

SELECT * FROM wordCount 
ORDER BY count DESC
limit 100;

SELECT * FROM wordCount 
WHERE LENGTH(word) > 3
ORDER BY count DESC
limit 100;

DROP TABLE IF EXISTS wordCount;
DROP TABLE IF EXISTS docs;
