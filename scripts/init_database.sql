/*
===============================================================================
Create Database and Schemas
===============================================================================

Script Purpose:
This script creates a new database name 'DataWareHouse' after checking if it already exits.
If the databse exists,it is dropped and recreated.Additionally,the script set up three schemas within the databse: 'Bronze','Silver' and 'Gold'.

********************************************************************************
WARNING:
Running this script will drop the entire 'DatawareHouse; database if it exists.
All data in the database will permanently deleted.Proceed with cautions and 
ensure you have proper backupsbefore running this scripts.
********************************************************************************

CODES:
-- Connect to the postgres administrative database for DB-level operations
\c postgres

-- 1. Drop the database if it exists, then create it a new
DO $$
BEGIN
   IF EXISTS (SELECT 1 FROM pg_database WHERE datname = 'datawarehouse') THEN
      PERFORM pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'datawarehouse';
      EXECUTE 'DROP DATABASE datawarehouse';
   END IF;
END$$;

CREATE DATABASE datawarehouse;

-- 2. Connect to the new database to create schemas
\c datawarehouse

-- 3. Create schemas for data layers
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
