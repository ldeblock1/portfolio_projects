--Create the required tables with their data restrictions
CREATE TABLE "30_year_mortgage" (
"date" DATE NOT NULL,
"30y_mortgage" REAL NOT NULL
);

CREATE TABLE "va_house_permits"(
"date" DATE NOT NULL,
"house_permits" REAL NOT NULL);

CREATE TABLE "va_hpi"(
"date" DATE NOT NULL,
"hpi" REAL NOT NULL
);

CREATE TABLE "va_labor_force"(
"date" DATE NOT NULL,
"va_labor" INTEGER NOT NULL
);



SELECT * FROM "va_house_permits";
SELECT * FROM "va_hpi";
SELECT * FROM "va_labor_force";
SELECT * FROM "30_year_mortgage";

--Create a new table with the joined data

CREATE TABLE house_price_predict_data AS 
SELECT 
    COALESCE(mor.date, vhp.date, hpi.date, vlf.date) AS common_date, 
    mor."30y_mortgage", 
    vhp.house_permits, 
    hpi.hpi, 
    vlf.va_labor
FROM "30_year_mortgage" AS mor 
FULL OUTER JOIN "va_house_permits" AS vhp ON mor.date = vhp.date
FULL OUTER JOIN "va_hpi" AS hpi ON mor.date = hpi.date
FULL OUTER JOIN "va_labor_force" AS vlf ON mor.date = vlf.date;

--Delete the Null values from the new table

DELETE FROM house_price_predict_data AS hppd
WHERE hppd.common_date IS NULL OR hppd."30y_mortgage" IS NULL OR hppd.house_permits IS NULL OR hppd.hpi IS NULL OR hppd.va_labor IS NULL;

--Display the new table 
SELECT * FROM house_price_predict_data;






