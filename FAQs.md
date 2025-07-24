# 📦 G-Migrate FAQs

### 1. What is G-Migrate?
G-Migrate is a database migration tool used to transfer database objects such as table DDLs (and data), views, stored procedures, and user-defined functions from a source DBMS to a target DBMS.

### 2. Does G-Migrate connect to Source and Target systems?
Yes, G-Migrate establishes connections with both the source and target systems before migrating the database objects.

### 3. Does G-Migrate migrate only Table DDL or both DDL and Data?
G-Migrate supports migrating both table DDL and its data.

### 4. What are the main components of G-Migrate?
The key components of G-Migrate are:
- **Analyze**
- **Migrate**
- **Validate**

### 5. What is the role of Table Analyzer in G-Migrate?
Table Analyzer checks for compatible data types and column defaults in the source tables.

### 6. What is the role of Stored Procedure Analyzer in G-Migrate?
Stored Procedure Analyzer identifies non-convertible keywords and generates reports highlighting them from the source stored procedures.

### 7. Which target systems are supported by G-Migrate?
G-Migrate currently supports migration to:
- Snowflake
- Databricks
- AWS S3
- Azure SQL Server

### 8. Which source systems are supported by G-Migrate?
Supported source systems include:
- SQL Server
- Azure SQL Server
- Oracle
- AWS S3
- SAP HANA
- AWS Redshift
- AWS Glue Catalog
- PostgreSQL
- DB2
- Snowflake

### 9. Which source systems support Snowflake as the target?
For Snowflake, G-Migrate supports migration from:
- SQL Server
- Azure SQL Server
- Oracle
- SAP HANA
- AWS Redshift

### 10. Which source systems support Databricks as the target?
Databricks supports migration from:
- Snowflake
- AWS Redshift
- PostgreSQL
- AWS S3
- AWS Glue

### 11. Which source systems support AWS S3 as the target?
G-Migrate supports DB2 as the source for migrations to AWS S3.

### 12. Which source systems support Azure SQL Server as the target?
Oracle is supported as the source when Azure SQL Server is the target.

### 13. What does the Migration Report contain?
The Migration Report indicates:
- Whether the database object migrated successfully
- For tables: number of rows loaded and any error messages

### 14. Does G-Migrate create a database in the target for audit reports?
Yes, G-Migrate creates a separate database in the target system that stores audit report details.

### 15. Which database objects can G-Migrate handle?
G-Migrate supports migration of:
- Tables (DDL and data)
- Views
- Stored Procedures
- User Defined Functions

---

# ✅ G-Validate FAQs

### 1. What validation methods does G-Validate support?
G-Validate supports:
- **Cell-by-Cell Validation**
- **Test Case-Based Validation**

### 2. What is Cell-by-Cell Validation?
This method compares each individual data cell from the source to the corresponding cell in the target, generating a report that highlights mismatched rows and columns.

### 3. How does Test Case-Based Validation work?
It uses a CSV file containing SQL test cases and table names. These queries are executed on both source and target systems, and the results are compared to generate a pass/fail report.

### 4. Which validation method is faster?
**Test Case-Based Validation** is generally faster than Cell-by-Cell Validation because it avoids comparing every data cell, which is computationally intensive for large datasets.
