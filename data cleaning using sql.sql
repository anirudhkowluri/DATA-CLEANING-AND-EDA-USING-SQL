-- data cleaning
-- remove duplicates
-- standardize the data
-- handle null values or missing values or blank values
 -- remove any irrelavant columns if needed

-- step 1 : creating a backup table and using it without using raw data
create table layoffs_1 like layoffs;
insert into layoffs_1 select * from layoffs;
select * from layoffs_1;
-- step 2 remove duplicates
use sql_project;
select * from layoffs_1;
-- using window function row_number to find duplicates
select *, row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,date,stage,country,funds_raised_millions) as row_num from layoffs_1;
-- use cte to make a standerdized subquery;
with duplicate_cte as 
(
select *, row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,date,stage,country,funds_raised_millions) as row_num from layoffs_1
)
select * from duplicate_cte where row_num > 1;
select * from layoffs_1 where company = 'Casper';

-- create a another table layoffs_2 to delete duplicate rows  by adding new column completely
 
CREATE TABLE `layoffs_2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
   `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_2;
insert into layoffs_2 
select *, row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,date,stage,country,funds_raised_millions) as row_num 
from layoffs_1;

select * from layoffs_2 where row_num > 1;
delete from layoffs_2 where row_num > 1;
select * from layoffs_2;
-- now the duplicate rows are removed

-- step3 standardizing the data( finding issues in data and fixing it)
-- standardizing company column values
select distinct company from layoffs_2;
select company, trim(company) from layoffs_2;
update layoffs_2 set company = trim(company);
select * from layoffs_2;
-- standardizing industry column values
select distinct industry from layoffs_2 order by 1;
select * from layoffs_2 where industry like 'Crypto%';
update layoffs_2 set industry = 'Crypto' where industry like 'Crypto%';
select * from layoffs_2;
-- standardizing location column values
select distinct location from layoffs_2 order by 1;
-- no need to fix location data
-- standardizing country column values
select distinct country from layoffs_2 order by 1;
-- remove '.' from united states row
select distinct country, Trim(trailing '.' from country) from layoffs_2  order by 1;
update layoffs_2 set country = Trim(trailing '.' from country) where country like 'united states%';
select distinct country from layoffs_2 order by 1;
-- change date datatype from text to date
select `date` from layoffs_2;
select `date`, str_to_date(`date`,'%m/%d/%Y') from layoffs_2;
update layoffs_2 set`date` = str_to_date(`date`,'%m/%d/%Y');
select * from layoffs_2;
alter table layoffs_2 modify  `date` date;
select * from layoffs_2;
-- step 4 removing null and blank values
-- removing null and blank values each coloumn wise
select * from layoffs_2 where total_laid_off is null
and percentage_laid_off is null;
-- remove blank values from industry column
select * from layoffs_2 where industry is null or industry = '';
select * from layoffs_2 where company = 'Airbnb';
-- fill blank values of airbnb
select * from layoffs_2 t1
join layoffs_2  t2 
on t1.company = t2.company  
where (t1.industry is null or t1.industry = '') 
and t2.industry is not null;
select t1.industry, t2.industry from layoffs_2 t1
join layoffs_2 t2 
on t1.company = t2.company 
where (t1.industry is null or t1.industry = '') and t2.industry is not null;
update layoffs_2 SET industry = null where industry = '';
update layoffs_2 t1
join layoffs_2 t2 
on t1.company = t2.company 
set t1.industry = t2.industry
where t1.industry is null and t2.industry is not null;
select * from layoffs_2 where company like 'Bally%';
-- remove null values from total laod off and percentage laid off coloumns
select * from layoffs_2 where total_laid_off is null
and percentage_laid_off is null;
delete from layoffs_2 where total_laid_off is null
and percentage_laid_off is null;
select * from layoffs_2;
-- step 5 remove irrelavant columns
alter table layoffs_2 drop column row_num;
select * from layoffs_2;

-- exploratory data analysis(eda)
select * from layoffs_2;
-- perform eda on total laid off and percentage laid off and funds raised millions columns
select max(total_laid_off), max(percentage_laid_off) from layoffs_2;
-- companies which laid off all the employees
select * from layoffs_2 where percentage_laid_off = 1;
select * from layoffs_2 where percentage_laid_off = 1 order by total_laid_off  desc;
select * from layoffs_2 where percentage_laid_off = 1 order by funds_raised_millions desc;
select company, sum(total_laid_off) from layoffs_2 group by company order by 2 desc;
select min( `date`), max(`date`) from layoffs_2;
select industry, sum(total_laid_off) from layoffs_2 group by industry order by 2 desc;
select country, sum(total_laid_off) from layoffs_2 group by country order by 2 desc;
select `date`, sum(total_laid_off) from layoffs_2 group by `date` order by 2 desc;
select YEAR(`date`), sum(total_laid_off) from layoffs_2 group by year(`date`) order by 1 desc;
select stage, sum(total_laid_off) from layoffs_2 group by stage order by 2 desc;
-- we are check the progression of layoffs from starting minimum to ending maxmimum layoffs called as rolling total layoffs
-- let us start with date
select substring(`date`,1,7) as month,sum(total_laid_off) from layoffs_2 where substring(`date`,1,7) is not null group by month order by 1 asc;
with Rolling_total as
(
select substring(`date`,1,7) as month,sum(total_laid_off) as total_off from layoffs_2 where substring(`date`,1,7) is not null group by month order by 1 asc
)
select month,total_off,sum(total_off) over(order by month) as rolling_total from Rolling_total;
select company, year(`date`),sum(total_laid_off) from layoffs_2 group by company,year(`date`) order by 3 desc;

-- ranking the total laid offs using years
with company_years (company,years,total_laid_off) as
(
select company, year(`date`),sum(total_laid_off) from layoffs_2 group by company,year(`date`)
),company_ranking as
(
select *, dense_rank() over(partition by years order by total_laid_off desc) as ranking
from company_years where years is not null 
)
select * from company_ranking where ranking<=5;

SELECT 
  COUNT(*) AS total_rows,
  COUNT(DISTINCT company) AS unique_companies,
  COUNT(DISTINCT industry) AS unique_industries,
  SUM(total_laid_off) AS total_laid_off,
  SUM(percentage_laid_off) AS total_percentage_laid_off
FROM layoffs_1;

SELECT industry, COUNT(*) AS count
FROM layoffs_1
GROUP BY industry
ORDER BY count DESC;

SELECT company, COUNT(*) AS count
FROM layoffs_1
GROUP BY company
ORDER BY count DESC;

SELECT 
  YEAR(`date`) AS year,
  MONTH(`date`) AS month,
  COUNT(*) AS count
FROM layoffs_1
GROUP BY year, month
ORDER BY year, month;

SELECT 
  CORR(total_laid_off, percentage_laid_off) AS correlation
FROM layoffs_1;