
# The database world_layoff consist of one dataset, layoffs.
# This is a dataset gotten from alex the analyse github page, it doesn't have a specific trends or questions to answer, This is purely trends finding on analysing.
# Lets dive in and analyze!!

SELECT * FROM layoffs;
# this table has nine columns and 2037 rows. 
# for best practice, regardless of the standard of the data, data cleaning is non- neogiable. 
# so lets clean up the data!
# first, lets start by removng duplicates but first! we need to make copy of the dataset,
# it best practice to never use the actual dataset for ruuning raw analysis.

CREATE TABLE layoffs_testing 
LIKE layoffs;

SELECT * FROM layoffs_stagging;

INSERT layoffs_stagging
SELECT * FROM layoffs;
# A copy of the orginal dataset is created. next, we find and remove duplicates

SELECT *,
ROW_NUMBER () OVER (PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised)
FROM layoffs_stagging;
# To make the query more readable, lets break t down with a CTE.

WITH duplicate_cte as
(
SELECT *,
ROW_NUMBER () OVER (PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised) as row_numbering
FROM layoffs_stagging
)
SELECT * FROM  duplicate_cte
WHERE row_numbering  > 1; 
# okay! we just confirmed the presence of duplicates! so lets remove them
# we wll start by making a replica table and adding the extra column, then insert the datas

CREATE TABLE `layoffs_stagging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised` double DEFAULT NULL,
  `row_numbering`INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

# created table, lets insert into the table

INSERT INTO layoffs_stagging2
SELECT *,
ROW_NUMBER () OVER (PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised) as row_numbering
FROM layoffs_stagging;

delete FROM layoffs_stagging2
WHERE row_numbering > 1;
# this removes the duplicates.
# next, lets standardise the data.

SELECT company, trim(company) from layoffs_stagging2;
UPDATE layoffs_stagging2
SET company = trim(company);

SELECT *
FROM layoffs_stagging2
WHERE industry LIKE 'crypto%';

UPDATE layoffs_stagging2
SET industry = 'crypto'
WHERE industry like 'crypto%';

SELECT DISTINCT country, trim(TRAILING '.' FROM country)
from layoffs_stagging2
ORDER BY 1;

UPDATE layoffs_stagging2
SET country = trim(TRAILING '.' FROM country)
WHERE country like 'united%';

select `date`,
str_to_date(`date`,'%m/%d/%Y')
FROM layoffs_stagging2;

UPDATE layoffs_stagging2
SET `date` = str_to_date(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_stagging2
MODIFY COLUMN `date` DATE;

#Removing and filling up null values.

SELECT t1.industry,t2.industry
FROM layoffs_stagging2 t1
JOIN  layoffs_stagging2 t2
	using (company)
WHERE t1 IS NULL 
AND t2 IS NOT NULL;

UPDATE layoffs_stagging2 t1
JOIN  layoffs_stagging2 t2
	using (company)
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry ='')
AND t2.industry IS NOT NULL;

# at this point the data is standardised and clean.



