-- Exploratory Data Analysis

SELECT  *
FROM	layoffs_staging2;

SELECT	MAX(total_laid_off), MAX(percentage_laid_off)
FROM	layoffs_staging2;

SELECT	*
FROM	layoffs_staging2
where	percentage_laid_off = 1
order by funds_raised_millions DESC;

SELECT	company, SUM(total_laid_off)
FROM	layoffs_staging2
GROUP BY company
order by 2 DESC;

SELECT	MIN(`date`), MAX(`date`)
FROM 	layoffs_staging2;

SELECT	industry, SUM(total_laid_off)
FROM	layoffs_staging2
GROUP BY industry
order by 2 DESC;

SELECT	country, SUM(total_laid_off)
FROM	layoffs_staging2
GROUP BY country
order by 2 DESC;

SELECT	Year(`date`), SUM(total_laid_off)
FROM	layoffs_staging2
GROUP BY Year(`date`)
order by 1 DESC;

SELECT	stage, SUM(total_laid_off)
FROM	layoffs_staging2
GROUP BY stage
order by 2 DESC;

SELECT	company, AVG(percentage_laid_off)
FROM	layoffs_staging2
GROUP BY company
order by 2 DESC;

SELECT 	SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off)
FROM	layoffs_staging2
where 	SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
order by 1 asc;

WITH Rolling_Total AS
(
SELECT 	SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) as Total_off
FROM	layoffs_staging2
where 	SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
order by 1 asc
)
SELECT `MONTH`, 
		total_off,
		SUM(total_off) OVER(ORDER BY `MONTH`) AS Rolling_total
FROM 	Rolling_Total;

SELECT	company, SUM(total_laid_off)
FROM	layoffs_staging2
GROUP BY company
order by 2 DESC;

SELECT	company, Year(`date`), SUM(total_laid_off)
FROM	layoffs_staging2
GROUP BY company, Year(`date`)
Order by 3 DESC;

WITH Company_Year (company, years, total_laid_off) AS
(
SELECT	company, Year(`date`), SUM(total_laid_off)
FROM	layoffs_staging2
GROUP BY company, Year(`date`)
), Company_Year_Rank AS
(
SELECT	*, dense_rank() over (partition by years order by total_laid_off DESC) AS Ranking
FROM	Company_Year
where years is not null
)
SELECT *
From 	Company_Year_Rank
where	Ranking <= 5;