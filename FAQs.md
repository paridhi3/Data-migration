# G-Migrate FAQs 

### 1. What is G-Migrate?
G-Migrate is a database migration tool which is used to migrate database objects like table DDL and its data, Views, Stored Procedures and User Defined Functions from Source DBMS to Target DBMS. 

### 2. Does G-Migrate connect to Source and Target Systems? 
Yes, G-Migrate makes a connection to Source and Target and then Migrates Database objects. 

### 3. Does G-Migrate Migrates only Table DDL or Table DDL including its Data? 
G-Migrate can Migrate both Table DDL and its Data. 

### 4. What are the main components of G-Migrate? 
The main components of G-Migrate are Analyze, Migrate and Validate 

### 5. What is the role of Table Analyze in G-Migrate? 
Table Analyzer helps in checking for compatible Data Types and Column Defaults in Source Tables. 

### 6. What is the role of Stored Procedure Analyzer in G-Migrate? 
Stored Procedure Analyzer helps in checking for non convertible keywords and gives a report mentioning non convertible keywords from Source Stored Procedure. 

### 7. Which all Target Systems does G-Migrate currently support? 
G-Migrate currently supports Migration to Snowflake, Databricks, S3 and Azure SQL Server. 

### 8. Which all source systems does G-Migrate currently support? 
G-Migrate currently supports SQL Server, Azure SQL Server, Oracle, S3, SAP Hana, AWS Redshift, AWS Glue Catalog, PostgreSQL, DB2 and Snowflake. 

### 9. Which all source systems support Snowflake as Target System? 
For Snowflake as Target System , G-Migrate supports SQL Sever, Azure SQL Server, Oracle, SAP Hana and AWS Redshift as Source. 

### 10. Which all source systems support Databricks as Target System? 
For Databricks as Target System, G-Migrate supports Snowflake, AWS Redshift, PostgreSQL, AWS S3 and AWS Glue as Source. 

### 11. Which all source systems support AWS S3 as Target System? 
For AWS S3 as Target System , G-Migrate supports DB2 as Source. 

### 12. Which all source systems support Azure SQL Server as Target System? 
For Azure SQL Server as Target System, G-Migrate supports Oracle as Source. 

### 13. What does Migration Report for G-Migrate contain? 
Migration Report indicates whether the Database Object migrated or not. Incase of Tables specifies the number of rows loaded and error message if any. 

### 14. Does G-Migrate create a Database in Target for Audit Reports? 
Yes, G-Migrate creates a separate database in Target System containing Audit Report details. 

### 15. Which all Database objects can G-Migrate migrate? 
G-Migrate can Migrate Tables (DDL and Data), Views, Stored Procedures and User Defined Functions. 

# G-Validate FAQs 
### 1. Which all methods does G-Validate support? 
G-Validate supports Cell by Cell Validation and Test Case Based Validation. 

### 2. What does Cell By Cell Validation mean? 
Cell by Cell Validation compares each data cell  from Source and corresponding data cell in Target and gives a report mentioning Row numbers for Columns where Data between Source and Target is mismatched. 

### 3. How does Test Case Based Validation work? 
Test Case Based Validation takes an  input CSV file containing SQL Test Cases along with Table name and executes those SQL Queries on Source and Target and compares their result and provides a report whether the result matches or not. 

### 5. Which Validation method performs faster Cell by Cell or Test Case Based? 
Test Case Based Validation generally performs faster than Cell By Cell Validation, since comparing each cell from a large table requires heavy compute and is time consuming.  
