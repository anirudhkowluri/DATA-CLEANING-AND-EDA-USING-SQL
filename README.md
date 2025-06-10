# DATA-CLEANING-AND-EDA-USING-SQL
The SQL file contains various data cleaning techniques and exploratory data analysis (EDA) steps. 
Here’s a summary of the techniques and steps performed:
Data Cleaning Techniques:
Remove Duplicates: The process includes identifying and removing duplicate entries using SQL window functions.
Standardize Data: A common table expression (CTE) is used to create a standardized subquery for identifying duplicates.
Handle Null Values: The script addresses null or missing values, ensuring data integrity.
Remove Irrelevant Columns: Any unnecessary columns are removed to streamline the dataset.
EDA Steps:
Backup Table Creation: A backup table (layoffs_1) is created to work with the data without altering the raw dataset.
Duplicate Identification: The script uses the ROW_NUMBER() function to find duplicates based on multiple columns.
Aggregation: The total layoffs are aggregated by company and year, providing insights into trends over time.
Ranking: Companies are ranked based on the total layoffs, allowing for a comparative analysis.
Descriptive Statistics: The script calculates total rows, unique companies, and industries, as well as total layoffs and percentages.
Correlation Analysis: A correlation between total layoffs and percentage laid off is computed to understand relationships within the data.
This structured approach ensures that the dataset is clean and ready for further analysis. If you need specific details or visualizations from any of these steps, please let me know!
