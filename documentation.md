## About the Asset 
### 1. Introduction 
GMigrate is a powerful tool designed to migrate data from legacy databases such as SQL Server, Oracle, Redshift, SAP HANA etc to the new and advanced architectures such as Snowflake and Databricks. ​ 
Genpact Accelerator for migration from legacy systems to Snowflake and Databricks to reduce manual efforts , cost and timeline​.

### 2. Key Features 
- Analyzer(DDLs/SPs)​ 
- Migrate selected Database, Schema or Tables, Views, SPs and UDFs​ 
- Data Validation​ 
- Enable schema, row count and field by field validation​ 
- Also support test case-based validation​ 
- Audit information for migrated tables​ 
- Migration status through progress bar  

### 3. Impact/Benefits 
- Reduces manual efforts , cost and timeline​ 
- Ensure more accurate migration​ 
- Risk free migration due to automated data validation feature​ 
- Almost 40-50% efforts can be reduced in migration​ 
- 60-65% effort can be reduced in data validation process.​ 

## Implementation
1. ANALYZER:- Provide report for how many objects can be converted automatically and how many need manual intervention ​ 
2. MIGRATE:- Get DDLs from source and convert it into target compatible format ,create it into target and load the source data into target 
3. VALIDATE:- Validate the data between source and target and send the validation reports to the identified leads​ 

### 1. ANALYZER STEPS 
- Makes connection with Source 
- Uses list of keywords(builtin functions,data types etc) in Source that are not compatible with Target 
- Extracts DDL from Source 
- Identifies whether DDL contains the keywords present in List 
- Creates Analyzer Report having details of whether DDL can be converted and issues if any 

### 2. MIGRATION STEPS 
- Makes Connection with Source 
- Makes Connection with Target 
- Uses Datatype Mapping between Source and Target 
- Extracts DDL from source 
- Extracts Table Data from Source 
- Converts DDL to target Compatible DDL using Datatype Mapping 
- Loads Converted DDL to Target 
- Loads Table Data to Target 
- Creates a Report having details of whether DDL and Data successfully loaded or not 

### 3. VALIDATION STEPS(CELL BY CELL COMPARISON) 
- Makes Connection with Source 
- Makes Connection with Target 
- Extracts Data from Source 
- Extracts Migrated Data from Target 
- Performs Cell by Cell Comparison Between Source and Target Data 
- Prepares Validator Report that gives details about whether Data is Matched or Mismatched 

### 4. VALIDATION STEPS(TEST CASE BASED VALIDATION) 
- Make Connection with Source 
- Make Connection with Target 
- Extract Test Cases to be executed 
- Execute Test Case SQL Query on Source 
- Execute Test Case SQL Query on Target 
- Check if result of Source and Target are matching 
- Prepare Test Case Validator Report that has details of whether the Source result and Target Result are matching or not 
 
