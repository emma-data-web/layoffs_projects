# Using the layoffs Dataset, lets find trends and analyse the data!
SELECT * FROM layoffs_stagging2;
# from the dataset, lets take a dig into the columns total_laid_off and percentage_laid_off

SELECT MAX(total_laid_off) , MAX(percentage_laid_off) 
FROM layoffs_stagging2;

SELECT * FROM layoffs_stagging2
WHERE percentage_laid_off >= 1
ORDER BY company;

SELECT MIN(`DATE`) FROM layoffs_stagging2;

SELECT industry, SUM(percentage_laid_off)
FROM layoffs_stagging2
GROUP BY industry
ORDER BY 2 DESC;
# check the total_laid_off using their years

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT YEAR(`date`), SUM(percentage_laid_off)
FROM layoffs_stagging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT substring(`date`,6,2) AS MONTH, SUM(total_laid_off)
FROM layoffs_stagging2
WHERE substring(`date`,6,2) IS NOT NULL
GROUP BY substring(`date`,6,2);

# create a cte named rolling_total to get the number of laid off workers per month
WITH rolling_total AS 
(
SELECT substring(`date`,6,2) AS MONTH, SUM(total_laid_off ) AS total_laid
FROM layoffs_stagging2
WHERE substring(`date`,6,2) IS NOT NULL
GROUP BY substring(`date`,6,2)
)
SELECT MONTH, total_laid, SUM(total_laid) OVER (ORDER BY MONTH)
FROM rolling_total;

SELECT company, SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY company
ORDER BY 2 DESC;

SELECT company,YEAR(`date`), SUM(total_laid_off)
FROM layoffs_stagging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;
# write cte to get the company total_lad off work per year
WITH company_year AS
(
SELECT company,YEAR(`date`) AS COM_YEAR, SUM(total_laid_off) as total
FROM layoffs_stagging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC
)
SELECT *, dense_rank() OVER(PARTITION BY  COM_YEAR ORDER BY total DESC)
FROM company_year;
