DROP DATABASE IF EXISTS DataWarehouse;

-- Create the DataWarehouse Database
CREATE DATABASE DataWarehouse;

-- Use the Database
/*
===================================================
		Create Databases
===================================================
		This script contains the database creation after checking the database exists or not 
        
		-- Warning --
			Running this script will drop the 'DataWarehouse' Database if exists.
            All data in your database will be permanently deleted.
*/
USE DataWarehouse;

-- Schema is a logical container
-- Create Schemas(it is same as database creation)
-- we cannot create schemas in same database.
DROP SCHEMA IF EXISTS bronze;
CREATE SCHEMA bronze;

DROP SCHEMA IF EXISTS silver;
CREATE SCHEMA silver;

DROP SCHEMA IF EXISTS gold;
CREATE SCHEMA gold;