/*
===========================================================================
Create Database and Schemas
===========================================================================
Script Purpose:
	This script checks if the database named 'DataWarehouse' already exists.
	If it exists it is dropped and recreated again.
	Additionnally, the script creates three schemas within the database named Bronze, Silver and Gold.

WARNING:
	Running this script will drop the entire database named 'DataWarehouse' if it exists.
	All the data in the database will be permanently deleted and replaced with the one that is contained in this script.
	Proceed with cautio and ensure you have proper backups before runing this script.
*/

USE MASTER;
GO

IF EXISTS(SELECT 1 FROM Sys.databases WHERE NAME = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' Database 
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA Bronze;
GO

CREATE SCHEMA Silver;
GO

CREATE SCHEMA Gold;
GO
