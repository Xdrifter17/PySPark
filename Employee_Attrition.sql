-- Databricks notebook source
-- DBTITLE 1,Running Spark in SQL.
SELECT * FROM employee

-- COMMAND ----------

select sum(EmployeeCount) from employee

-- COMMAND ----------

-- DBTITLE 1,Find out attrition division:
select sum(EmployeeCount),attrition from employee
group by 2

-- COMMAND ----------

-- DBTITLE 1,Age analysis- Checking which age group has higher attrition.
select sum(EmployeeCount),Age from employee
where Attrition='Yes'
group by Age

-- COMMAND ----------

select sum(EmployeeCount),
case when age between 20 and 25 then '20-25' when age between 26 and 32 then '26-32' when age between 33 and 40 then '33-40' else '40+' end age_group 
from employee
where Attrition='Yes'
group by 2

-- COMMAND ----------

-- DBTITLE 1,attrition based on departments:
SELECT sum(EmployeeCount),department from employee
where attrition='Yes'
GROUP BY department

-- COMMAND ----------

-- DBTITLE 1,Attrition based on Education: 1-High School, 2-College, 3-Bachelor, 4-Masters, 5-Phd.
SELECT sum(EmployeeCount),
case when Education=1 then 'High school' when Education=2 then 'College' when Education='3' then 'Bachelors' when Education=4 then 'Masters' else 'Phd' end Degree
from employee
where attrition='Yes'
GROUP BY 2

-- COMMAND ----------

-- DBTITLE 1,Attrition based on team enviroment: 1-low, 2-medium, 3-satisfied, 4-Very satisfied:
SELECT sum(EmployeeCount),
EnvironmentSatisfaction
from employee
where attrition='Yes'
GROUP BY 2

-- COMMAND ----------

-- DBTITLE 1,Attrition based on travelling:
SELECT sum(EmployeeCount),
BusinessTravel
from employee
where attrition='Yes'
GROUP BY 2

-- COMMAND ----------

-- DBTITLE 1,Job Role with the Highest Attrition Rate:
SELECT JobRole, (COUNT(*) / SUM(COUNT(*)) OVER ()) AS AttritionRate
FROM employee
WHERE Attrition = 'Yes'
GROUP BY JobRole
ORDER BY AttritionRate DESC
LIMIT 1

-- COMMAND ----------

-- DBTITLE 1,Attrition based on Marital Status:
SELECT MaritalStatus, Attrition, COUNT(*) AS Count
FROM employee
GROUP BY MaritalStatus, Attrition


-- COMMAND ----------

-- DBTITLE 1,Average Years at Company for Attrited Employees:
SELECT AVG(YearsAtCompany) AS AvgYearsAtCompany
FROM employee
WHERE Attrition = 'Yes'


-- COMMAND ----------

-- DBTITLE 1,Average Monthly Income by Gender:
SELECT Gender, AVG(MonthlyIncome) AS AvgMonthlyIncome
FROM employee
GROUP BY Gender


-- COMMAND ----------

-- DBTITLE 1,Distribution of employee tenure among attrited and non-attrited employees:
SELECT
  Attrition,
  AVG(YearsAtCompany) AS AvgYearsAtCompany,
  MEDIAN(YearsAtCompany) AS MedianYearsAtCompany,
  MIN(YearsAtCompany) AS MinYearsAtCompany,
  MAX(YearsAtCompany) AS MaxYearsAtCompany
FROM employee
GROUP BY Attrition


-- COMMAND ----------

-- DBTITLE 1,Attrition by Work-Life Balance:
SELECT
  WorkLifeBalance,
  Attrition,
  COUNT(*) AS Count
FROM employee
GROUP BY WorkLifeBalance, Attrition

