-- =====================================================
-- Banking Database Data Cleaning Script
-- This script performs data cleaning and transformation
-- on banking dataset tables in MySQL.
-- =====================================================

-- =====================================================
-- ACCOUNT TABLE CLEANING
-- =====================================================

-- Add foreign key relationship between account and district table
ALTER TABLE account
ADD FOREIGN KEY (district_id) REFERENCES district(District_Code);

-- Check records with specific frequency value
SELECT * 
FROM account
WHERE frequency = 'POPLATEK PO OBRATU';

-- Convert integer date format into proper DATE format
UPDATE account
SET `date` = STR_TO_DATE(
CONCAT(
CASE LEFT(`date`,2)
WHEN '93' THEN '2016'
WHEN '94' THEN '2017'
WHEN '95' THEN '2018'
WHEN '96' THEN '2019'
WHEN '97' THEN '2020'
END,
SUBSTRING(`date`,3,4)
),
'%Y%m%d'
);

-- Disable safe update mode to allow updates
SET SQL_SAFE_UPDATES = 0;

-- Change column datatype from INT to DATE
ALTER TABLE account
MODIFY COLUMN `date` DATE;

-- Standardize frequency values into readable English format
UPDATE account
SET frequency =
CASE
WHEN frequency = 'POPLATEK MESICNE' THEN 'Monthly Issuance'
WHEN frequency = 'POPLATEK TYDNE' THEN 'Weekly Issuance'
WHEN frequency = 'POPLATEK PO OBRATU' THEN 'Issuance After Transaction'
END;

-- Add new column for card assignment type
ALTER TABLE account
ADD COLUMN Card_Assigned VARCHAR(20);

-- Assign card types based on frequency
UPDATE account
SET Card_Assigned =
CASE
WHEN frequency = 'Monthly Issuance' THEN 'Silver'
WHEN frequency = 'Weekly Issuance' THEN 'Diamond'
WHEN frequency = 'Issuance After Transaction' THEN 'Gold'
END;


-- =====================================================
-- CARD TABLE CLEANING
-- =====================================================

-- Convert issued column into proper date format
UPDATE card
SET issued = STR_TO_DATE(
CONCAT(
CASE LEFT(issued,2)
WHEN '93' THEN '2016'
WHEN '94' THEN '2017'
WHEN '95' THEN '2018'
WHEN '96' THEN '2019'
WHEN '97' THEN '2020'
WHEN '98' THEN '2021'
END,
SUBSTRING(issued,3,4)
),
'%Y%m%d'
);

-- Disable safe update mode
SET SQL_SAFE_UPDATES = 0;

-- Change issued column datatype to DATE
ALTER TABLE card
MODIFY COLUMN issued DATE;

-- Fix invalid leap year date (0229 → 0228)
UPDATE card
SET issued = REPLACE(issued,'0229','0228')
WHERE SUBSTRING(issued,3,4)='0229';

-- Standardize card types
UPDATE card
SET type = 'Diamond'
WHERE type = 'Gold';

UPDATE card
SET type = 'Gold'
WHERE type = 'classic';

UPDATE card
SET type = 'Silver'
WHERE type = 'junior';

-- Check remaining unclean card types
SELECT *
FROM card
WHERE type IN ('classic','junior');


-- =====================================================
-- CLIENT TABLE CLEANING
-- =====================================================

-- Add foreign key relationship with district table
ALTER TABLE client
ADD FOREIGN KEY(district_id) REFERENCES district(District_Code);

-- Preview client table
SELECT * FROM client;

-- Extract gender information from birth_number
UPDATE client
SET sex =
CASE
WHEN SUBSTRING(birth_number,3,2) > 50 THEN 'Female'
ELSE 'Male'
END;

-- Remove birth_number column and add sex column
ALTER TABLE client
DROP COLUMN birth_number,
ADD COLUMN sex CHAR(10);

-- Convert encoded birth_number into actual birth_date
UPDATE client
SET birth_date = STR_TO_DATE(
CONCAT(
1900 + LEFT(birth_number,2),

LPAD(
CASE
WHEN SUBSTRING(birth_number,3,2) > 50
THEN SUBSTRING(birth_number,3,2) - 50
ELSE SUBSTRING(birth_number,3,2)
END,2,'0'),

RIGHT(birth_number,2)
),
'%Y%m%d'
);


-- =====================================================
-- DISTRICT TABLE CLEANING
-- =====================================================

-- Rename columns to meaningful names
ALTER TABLE district
RENAME COLUMN A16 TO no_of_committed_crime_2018;

-- Remove unnecessary column
ALTER TABLE district
DROP COLUMN A13;

-- Rename columns for better readability
ALTER TABLE district RENAME COLUMN A1 TO District_Code;
ALTER TABLE district RENAME COLUMN A2 TO District_Name;
ALTER TABLE district RENAME COLUMN A3 TO Region;
ALTER TABLE district RENAME COLUMN A4 TO No_of_inhabitants;
ALTER TABLE district RENAME COLUMN A5 TO No_of_municipalities_with_inhabitants_less_499;
ALTER TABLE district RENAME COLUMN A6 TO No_of_municipalities_with_inhabitants_500_btw_1999;
ALTER TABLE district RENAME COLUMN A7 TO No_of_municipalities_with_inhabitants_2000_btw_9999;
ALTER TABLE district RENAME COLUMN A8 TO No_of_municipalities_with_inhabitants_less_10000;
ALTER TABLE district RENAME COLUMN A9 TO No_of_cities;
ALTER TABLE district RENAME COLUMN A10 TO Ratio_of_urban_inhabitants;
ALTER TABLE district RENAME COLUMN A11 TO Average_salary;
ALTER TABLE district RENAME COLUMN A14 TO No_of_entrepreneurs_per_1000_inhabitants;
ALTER TABLE district RENAME COLUMN A15 TO No_committed_crime_2017;


-- =====================================================
-- LOAN TABLE CLEANING
-- =====================================================

-- Convert loan date to proper date format
UPDATE loan
SET date = STR_TO_DATE(
CONCAT(
CASE LEFT(date,2)
WHEN '93' THEN '2016'
WHEN '94' THEN '2017'
WHEN '95' THEN '2018'
WHEN '96' THEN '2019'
WHEN '97' THEN '2020'
WHEN '98' THEN '2021'
END,
SUBSTRING(date,3,4)
),
'%Y%m%d'
);

-- Change datatype from INT to DATE
ALTER TABLE loan
MODIFY date DATE;

-- Fix invalid leap year dates
UPDATE loan
SET date = REPLACE(date,'0229','0228')
WHERE SUBSTRING(date,3,4)='0229';

-- Disable safe update mode
SET SQL_SAFE_UPDATES = 0;

-- Convert status codes into meaningful descriptions
UPDATE loan
SET status =
CASE
WHEN status = 'A' THEN 'Contract Finished'
WHEN status = 'B' THEN 'Loan Not Paid'
WHEN status = 'C' THEN 'Running Contract'
WHEN status = 'D' THEN 'Client in Debt'
END;
