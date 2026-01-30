--- Data Cleaning

SELECT * FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove Any Columns 


-- Remove duplicates

CREATE TABLE layoffs_staging 
LIKE layoffs;

select * from layoffs_staging;

INSERT INTO layoffs_staging
select * from layoffs;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,date,stage, country, funds_raised_millions) AS row_num
fROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

select * 
from layoffs_staging
where company = 'Casper';

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

select * 
from layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,date,stage, country, funds_raised_millions) AS row_num
fROM layoffs_staging;

select * 
from layoffs_staging2
WHERE row_num > 1;

DELETE 
from layoffs_staging2
WHERE row_num > 1;

select * 
from layoffs_staging2;

-- Standardizing data

select company, TRIM(company)
from layoffs_staging2;

Update layoffs_staging2
SET company = TRIM(company);

select DISTINCT industry
from layoffs_staging2
ORDER BY industry;

select *
from layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry= 'Crypto'
WHERE industry LIKE 'Crypto%';

select distinct country
from layoffs_staging2
order by 1;

select distinct country
from layoffs_staging2
order by 1;

-- United States is repeated with the second one reading as 'United states.' So we solve that.alter

select DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country= TRIM(TRAILING '.' FROM country)
where country LIKE 'United States%';

select date
FROM layoffs_staging2;

select date,
STR_TO_DATE(date,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET date = STR_TO_DATE(date,'%m/%d/%Y');

select date
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN date DATE;

select *
FROM layoffs_staging2;

-- Null and Blank Values

select *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

UPDATE layoffs_staging2
SET industry= null
WHERE industry='';

select t1.industry,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company=t2.company
  WHERE (t1.industry IS NULL OR t1.industry = '')
  AND t2.industry IS NOT NULL;
  
  UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company=t2.company
  SET t1.industry=t2.industry
  WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;
  
  select * From layoffs_staging2;
  
  --- Remove Any Columns or Rows 
  
  select * from layoffs_staging2
  where total_laid_off IS NULL
  AND percentage_laid_off IS NULL;
  
  
  DELETE 
  from layoffs_staging2
  where total_laid_off IS NULL
  AND percentage_laid_off IS NULL;
  
    select * From layoffs_staging2;
    
    ALTER table layoffs_staging2
    DROP COLUMN row_num;
