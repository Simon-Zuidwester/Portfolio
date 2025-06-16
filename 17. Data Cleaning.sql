-- Data Cleaning

SELECT	*
FROM	layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove Any Columns or Rows

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT	*
FROM 	layoffs_staging;

INSERT layoffs_staging
SELECT	*
FROM	layoffs;

SELECT	*,
row_number() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM 	layoffs_staging;

WITH	duplicate_cte AS
(
SELECT	*,
row_number() OVER(
PARTITION BY company, location, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM 	layoffs_staging
) 
SELECT	*
FROM duplicate_cte
where row_num > 1;

SELECT	*
FROM	layoffs_staging
WHERE	company = 'Casper';

WITH	duplicate_cte AS
(
SELECT	*,
row_number() OVER(
PARTITION BY company, location, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM 	layoffs_staging
)  
FROM duplicate_cte
where row_num > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT	*
FROM 	layoffs_staging2
WHERE	row_num > 1;

INSERT INTO layoffs_staging2
SELECT	*,
row_number() OVER(
PARTITION BY company, location, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM 	layoffs_staging;

DELETE
FROM 	layoffs_staging2
WHERE	row_num > 1;

-- Standardizing data
SELECT	ls.company, TRIM(ls.company)
FROM	layoffs_staging2 ls;

update layoffs_staging2
SET company = TRIM(company);

SELECT	distinct(industry)
FROM	layoffs_staging2;

SELECT	*
FROM	layoffs_staging2
WHERE	industry like '%Crypto%';

UPDATE	layoffs_staging2
SET industry = 'Crypto'
WHERE	industry like '%Crypto%';

SELECT	distinct(location)
FROM	layoffs_staging2
order by location;

SELECT	distinct(country), TRIM(TRAILING '.' FROM country)
FROM	layoffs_staging2
order by country;

SELECT	distinct(country)
FROM	layoffs_staging2
WHERE	country LIKE 'United States%';

update layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE	country LIKE 'United States%';

SELECT	`date`,
str_to_date(`date`, '%m/%d/%Y')
FROM 	layoffs_staging2;

UPDATE layoffs_staging2
SET  `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT	*
FROM layoffs_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT	*
FROM layoffs_staging2
where industry IS NULL
OR industry = '';

SELECT	*
FROM layoffs_staging2
where	company = 'Airbnb';

SELECT	t1.industry, t2.industry
FROM 	layoffs_staging2 t1
JOIN	layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE	(t1.industry IS NULL OR t1.industry = '')
AND		(t2.industry IS NOT NULL);

UPDATE layoffs_staging2
SET industry = null
where industry = '';

UPDATE layoffs_staging2 t1
JOIN	layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry 
WHERE	(t1.industry IS NULL OR t1.industry = '')
AND		(t2.industry IS NOT NULL);

SELECT 	* 
FROM 	layoffs_staging2;

SELECT	*
FROM layoffs_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT 	* 
FROM 	layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
