/****** Script to take a full set of dwellings from the interim dwelling list, compare them to the current dwelling register,
		and update them into the following tables:

			- [Sandbox_PlaceIndex].[Final_PlaceIndex].[Dwelling_Register_Skinny]
			- [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Building_Reference]
			- [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Part_of_Building_Reference]

		Note: the table [Sandbox_PlaceIndex].[Final_PlaceIndex].[Scenario_Lookup] is expected to be updated manually, separate to this process, but should be up to date as well as this table.

		Author: Cambell Ritchie
		Last Modified Date: 04/04/2025
		Latest update message: Added Logging and introduced best practice and batching for insert/update section.
******/


/* -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	PLAN:
		1. Find changesets.
		2. Add appropriate effective start and end dates? Probably not
		3. Add appropriate systems start and end dates
		4. Update table
		5. Profit??

	Questions:
	-	Do the records on the old versions of the database need updating regardless of whether things change?
		e.g. if in v0.1 a building 1 is linked to a dwelling 1, and in v0.2 building 1 is still linked to dwelling 1
		I think the answer is no but should confirm.
	-	How much should the date system be worked through to accommodate the final design?  Especially if there's a 
		function in Databricks that already does it for us. Ask System Architects.

	Further detail:
	INSERTs:
	-	Take data from interim table and populate as a new record in the dwelling register.
	-	Needs some way of ensuring that the dwelling ID is unique (if this is not already covered).
	-	New system start date, no system end date. Effective dates stay as-is.

	DELETEs:
	-	Check date stuff with architects. Confusing.
	-	System end dates??? Could say yes, or just fill in sys_deleted_ind instead.
	-	Effective end dates? Of dwelling. Do they need updating? Probably not, it should be system dates.

	UPDATEs:
	-	Combo of INSERTs and DELETEs.
	-	Anything else to consider here? E.g. system dates. Do new records need to be inserted for the "old" records?


	Potential Improvements:
	-	Automate the version/scenario options (e.g. pick all scenarios with latest version)
	-	Add a setting for FULL dwelling set vs changeset to allow for changesets to be processed
			This would change the way the inserts, updates, and deletes are found.
			May need to consider adding a column to the interim table for the type of change (so deletes are also included).
	-	could try something like the below instead of complex joins:
			SELECT ID from SKINNY where ID not IN
				(SELECT ID from #temp_dwell)
		not very easy with composite keys (ID + date)
	-	Could use BETWEEN for dates
*/ -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


/* -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	Notes on how to use/test the code:

	When running this code, there are several variables at the start of the script that should be carefully chosen (for now).
	Start with a scenario number and version number. This lets you choose the exact set of dwellings you'd like to take from the interim table to the final table.
	If desired, choosing a system_create_date will allow you to transfer dwellings from further back in time. Default behaviour fetches the latest sys_create_date for the 
	scenario number and version number chosen.

	Quick example of how the initial parameters could be set up:
		DECLARE @version_number		FLOAT = 0;
		DECLARE @sys_create_date	DATETIME2(0); 
		DECLARE @sys_end_date		DATETIME2(0) = GETDATE();

		DECLARE @scenario_number TABLE (id INT); 
		INSERT @scenario_number(id) VALUES(0), (1), (2);





	If wanting to run a test on the code, search this program for TEST SECTION and uncomment the lines following them, then follow the steps below, each with the following parameters.
	Note that rerunning the same data should get no changes to the output tables (you can test this by rerunning step 1).
	The sys_end_date will likely have different times than those shown below, as the time you run the test will be taken for any deletes.

		STEP 1: Run the script with these parameters:
			
			DECLARE @version_number		FLOAT = 0;
			DECLARE @sys_create_date	DATETIME2(0) = '2023-07-24 21:46:59.000';

			The output of the test section should look like this:
			dwelling_id	scenario_no	effective_start_date	effective_end_date		sys_create_date			sys_end_date			sys_deleted_ind
				   0			0	1900-05-15 00:00:00.000	9999-01-01 00:00:00.000	2023-07-24 21:46:59.000	9999-01-01 00:00:00.000				N
				   0		   99	2021-03-12 00:00:01.000	2023-04-17 00:00:00.000	2023-07-24 21:46:59.000	9999-01-01 00:00:00.000				N
			99998888			0	1900-05-15 00:00:00.000	2021-03-12 00:00:00.000	2023-07-24 21:46:59.000	9999-01-01 00:00:00.000				N
			99998888			0	2021-03-12 00:00:01.000	2023-04-17 00:00:00.000	2023-07-24 21:46:59.000	9999-01-01 00:00:00.000				N
			99999999			0	1900-05-15 00:00:00.000	9999-01-01 00:00:00.000	2023-07-24 21:46:59.000	9999-01-01 00:00:00.000				N

			dwelling_id	building_id	effective_start_date	effective_end_date		sys_create_date			sys_end_date			sys_deleted_ind
				   0			0	1900-05-15 00:00:00.000	9999-01-01 00:00:00.000	2023-07-24 21:46:59.000	9999-01-01 00:00:00.000				N
				   0			0	2021-03-12 00:00:01.000	2023-04-17 00:00:00.000	2023-07-24 21:46:59.000	9999-01-01 00:00:00.000				N
			99998888			0	2021-03-12 00:00:01.000	2023-04-17 00:00:00.000	2023-07-24 21:46:59.000	9999-01-01 00:00:00.000				N
			99999999	999999999	1900-05-15 00:00:00.000	9999-01-01 00:00:00.000	2023-07-24 21:46:59.000	9999-01-01 00:00:00.000				N

			dwelling_id	part_of_building_id		effective_start_date	effective_end_date		sys_create_date			sys_end_date			sys_deleted_ind
			99998888	tst99999999999999test	1900-05-15 00:00:00.000	2021-03-12 00:00:00.000	2023-07-24 21:46:59.000	9999-01-01 00:00:00.000				N


		STEP 2: Run the script with these parameters:
			
			DECLARE @version_number		FLOAT = 0;
			DECLARE @sys_create_date	DATETIME2(0);

			The final output of the test section should look like this:
			dwelling_id	scenario_no	effective_start_date	effective_end_date		sys_create_date			sys_end_date			sys_deleted_ind
				   0			0	1900-05-15 00:00:00.000	9999-01-01 00:00:00.000	2023-07-24 21:46:59.000	2023-07-30 12:53:00.000				  N
				   0			0	1900-05-15 00:00:00.000	2021-03-12 00:00:00.000	2023-07-30 12:53:01.000	9999-01-01 00:00:00.000				  N
				   0		   99	2021-03-12 00:00:01.000	2023-04-17 00:00:00.000	2023-07-24 21:46:59.000	2023-07-30 12:53:00.000				  N
				   0			0	2021-03-12 00:00:01.000	2023-04-17 00:00:00.000	2023-07-30 12:53:01.000	9999-01-01 00:00:00.000				  N
				   0		   99	2023-04-17 00:00:01.000	9999-01-01 00:00:00.000	2023-07-30 12:53:01.000	9999-01-01 00:00:00.000				  N
			98765432		   99	2023-04-17 00:00:01.000	2024-08-07 00:00:00.000	2023-07-30 12:53:01.000	9999-01-01 00:00:00.000				  N
			98765432			0	2024-08-07 00:00:01.000	9999-01-01 00:00:00.000	2023-07-30 12:53:01.000	9999-01-01 00:00:00.000				  N
			99998888			0	1900-05-15 00:00:00.000	2021-03-12 00:00:00.000	2023-07-24 21:46:59.000	9999-01-01 00:00:00.000				  N
			99998888			0	2021-03-12 00:00:01.000	2023-04-17 00:00:00.000	2023-07-24 21:46:59.000	9999-01-01 00:00:00.000				  N
			99998888			0	2023-04-17 00:00:01.000	9999-01-01 00:00:00.000	2023-07-30 12:53:01.000	9999-01-01 00:00:00.000				  N
			99999999			0	1900-05-15 00:00:00.000	9999-01-01 00:00:00.000	2023-07-24 21:46:59.000	2024-09-09 13:44:00.000				  Y

			dwelling_id	building_id	effective_start_date	effective_end_date		sys_create_date			sys_end_date			sys_deleted_ind
				   0			0	1900-05-15 00:00:00.000	9999-01-01 00:00:00.000	2023-07-24 21:46:59.000	2023-07-30 12:53:00.000				  N
				   0			0	1900-05-15 00:00:00.000	2021-03-12 00:00:00.000	2023-07-30 12:53:01.000	9999-01-01 00:00:00.000				  N
				   0			0	2021-03-12 00:00:01.000	2023-04-17 00:00:00.000	2023-07-24 21:46:59.000	2023-07-30 12:53:00.000				  N
				   0			0	2021-03-12 00:00:01.000	2023-04-17 00:00:00.000	2023-07-30 12:53:01.000	9999-01-01 00:00:00.000				  N
				   0			0	2023-04-17 00:00:01.000	9999-01-01 00:00:00.000	2023-07-30 12:53:01.000	9999-01-01 00:00:00.000				  N
			98765432			0	2023-04-17 00:00:01.000	2024-08-07 00:00:00.000	2023-07-30 12:53:01.000	9999-01-01 00:00:00.000				  N
			98765432	998765432	2024-08-07 00:00:01.000	9999-01-01 00:00:00.000	2023-07-30 12:53:01.000	9999-01-01 00:00:00.000				  N
			99998888			0	1900-05-15 00:00:00.000	2021-03-12 00:00:00.000	2023-07-30 12:53:01.000	9999-01-01 00:00:00.000				  N
			99998888			0	2021-03-12 00:00:01.000	2023-04-17 00:00:00.000	2023-07-24 21:46:59.000	2023-07-30 12:53:00.000				  N
			99999999	999999999	1900-05-15 00:00:00.000	9999-01-01 00:00:00.000	2023-07-24 21:46:59.000	2024-09-09 13:44:00.000				  Y

			dwelling_id	part_of_building_id		effective_start_date	effective_end_date		sys_create_date			sys_end_date			sys_deleted_ind
			99998888	tst99999999999999test	1900-05-15 00:00:00.000	2021-03-12 00:00:00.000	2023-07-24 21:46:59.000	2023-07-30 12:53:00.000				N
			99998888	tst99999999999990test	2021-03-12 00:00:01.000	2023-04-17 00:00:00.000	2023-07-30 12:53:01.000	9999-01-01 00:00:00.000				N
			99998888	tst99999999999999test	2023-04-17 00:00:01.000	9999-01-01 00:00:00.000	2023-07-30 12:53:01.000	9999-01-01 00:00:00.000				N


		STEP 3:
			Run the following (this gets rid of the test set in the output tables):

			DELETE
			  FROM [Sandbox_PlaceIndex].[Final_PlaceIndex].[Dwelling_Register_Skinny]
			  where scenario_no = 0 or scenario_no = 99

			DELETE
			  FROM [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Building_Reference]
			  where building_id = 0 or building_id > 9999990

			DELETE
			  FROM [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Part_of_Building_Reference]
			  where part_of_building_id like 'tst%'

*/ -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



/* ------------------------------------------------------------------------------------------------------------------------------------------------
	Setting up variables
------------------------------------------------------------------------------------------------------------------------------------------------ */

DECLARE @version_number		FLOAT = 0.4;
DECLARE @sys_create_date	DATETIME2(0);	-- E.g. = '2023-07-24 21:46:59.000', or leave blank for max date;
DECLARE @sys_end_date		DATETIME2(0) = GETDATE();	-- so any end dates get added with the same time on them.

-- Choose specific scenario numbers to run on if needed.
DECLARE @scenario_number TABLE (id INT); 
--INSERT @scenario_number(id) VALUES(0);		-- Uncomment this line if you want to choose specific scenarios, e.g. INSERT @scenario_number(id) VALUES(0), (1), (2);

-- This automatically selects all scenario numbers available for the chosen version if no scenarios are specified above
IF NOT EXISTS(SELECT 1 FROM @scenario_number)
	INSERT INTO @scenario_number
	SELECT DISTINCT(scenario_no) FROM [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_List_Interim]
	WHERE version_no = @version_number
	AND scenario_no <> 999

	

-- Automatically find the latest date in the interim table with these parameters if there isn't one supplied
SELECT @sys_create_date = ISNULL(@sys_create_date, MAX( [sys_create_date]) )
		FROM [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_List_Interim]
		  WHERE scenario_no IN (SELECT id FROM @scenario_number)
		  AND version_no = @version_number



/* ------------------------------------------------------------------------------------------------------------------------------------------------
	Get records with the matching parameters
------------------------------------------------------------------------------------------------------------------------------------------------ */

IF OBJECT_ID('tempdb..#temp_dwell') IS NOT NULL
    DROP TABLE #temp_dwell


SELECT --TOP (1000) 
	   [dwelling_id]
      ,[scenario_no]
      ,[version_no]
      ,[building_id]
	  ,[part_of_building_id]
      ,[effective_start_date]
      ,[effective_end_date]
      ,[sys_create_date]
      ,'9999-01-01 00:00:00.000' AS [sys_end_date]
      ,'N' AS [sys_deleted_ind]
  INTO #temp_dwell
  FROM [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_List_Interim]
  WHERE scenario_no IN (SELECT id FROM @scenario_number)
  AND version_no = @version_number
  --AND sys_create_date = @sys_create_date -- DO WE WANT >= ? I think not, as that allows for more specificity in what we grab.
  --AND (sys_end_date IS NULL OR sys_end_date > '4999-01-01')



/* ------------------------------------------------------------------------------------------------------------------------------------------------
	Find deletes
------------------------------------------------------------------------------------------------------------------------------------------------ */

IF OBJECT_ID('tempdb..#dwell_deletes') IS NOT NULL
    DROP TABLE #dwell_deletes

SELECT
	dwell_reg.dwelling_id,
	999988880 AS building_id,
	'fake9999999999999test' AS part_of_building_id,
	dwell_reg.scenario_no,
	dwell_reg.effective_start_date,
	dwell_reg.effective_end_date,
	dwell_reg.sys_create_date,
	@sys_end_date AS sys_end_date,
	'Y' AS sys_deleted_ind,
	'DELETE' AS change
INTO #dwell_deletes
FROM [Sandbox_PlaceIndex].[Final_PlaceIndex].[Dwelling_Register_Skinny] AS dwell_reg
LEFT JOIN #temp_dwell AS temp
ON dwell_reg.dwelling_id = temp.dwelling_id AND dwell_reg.effective_start_date = temp.effective_start_date
WHERE temp.dwelling_id IS NULL
AND dwell_reg.scenario_no IN (SELECT id FROM @scenario_number)
AND (dwell_reg.sys_end_date IS NULL OR dwell_reg.sys_end_date > '4999-01-01')


/* ------------------------------------------------------------------------------------------------------------------------------------------------
	Find inserts
------------------------------------------------------------------------------------------------------------------------------------------------ */

IF OBJECT_ID('tempdb..#dwell_inserts') IS NOT NULL
    DROP TABLE #dwell_inserts

SELECT temp.*,
	'INSERT' AS change
INTO #dwell_inserts
FROM (SELECT * FROM [Sandbox_PlaceIndex].[Final_PlaceIndex].[Dwelling_Register_Skinny]
		WHERE (sys_end_date IS NULL OR sys_end_date > '4999-01-01')) AS dwell_reg
RIGHT OUTER JOIN #temp_dwell AS temp
ON dwell_reg.dwelling_id = temp.dwelling_id AND dwell_reg.effective_start_date = temp.effective_start_date
WHERE dwell_reg.dwelling_id IS NULL




/* ------------------------------------------------------------------------------------------------------------------------------------------------
	Find UPDATEs
------------------------------------------------------------------------------------------------------------------------------------------------ */

-- Find updates and add them to both inserts and deletes
INSERT INTO #dwell_deletes
SELECT 
	dwell_reg.dwelling_id,
	temp.building_id,		-- THIS LINE MIGHT NEED TO CHANGE IN FUTURE
	ISNULL(temp.part_of_building_id, 'delete101me'),
	dwell_reg.scenario_no,
	dwell_reg.effective_start_date,
	dwell_reg.effective_end_date,
	dwell_reg.sys_create_date,
	DATEADD(s, -1, temp.sys_create_date) AS sys_end_date, -- Takes the updated system date for the new record -1 second and uses it for the system end date.
	'N' AS sys_deleted_ind,
	'UPDATE' AS change
FROM [Sandbox_PlaceIndex].[Final_PlaceIndex].[Dwelling_Register_Skinny] AS dwell_reg
JOIN #temp_dwell AS temp
ON dwell_reg.dwelling_id = temp.dwelling_id AND dwell_reg.effective_start_date = temp.effective_start_date
WHERE (dwell_reg.effective_end_date <> temp.effective_end_date or dwell_reg.scenario_no <> temp.scenario_no)
AND dwell_reg.scenario_no IN (SELECT id FROM @scenario_number)
AND (dwell_reg.sys_end_date IS NULL OR dwell_reg.sys_end_date > '4999-01-01')


INSERT INTO #dwell_inserts
SELECT temp.*,
	'UPDATE' AS change
FROM [Sandbox_PlaceIndex].[Final_PlaceIndex].[Dwelling_Register_Skinny] AS dwell_reg
JOIN #temp_dwell AS temp
ON dwell_reg.dwelling_id = temp.dwelling_id AND dwell_reg.effective_start_date = temp.effective_start_date
WHERE (dwell_reg.effective_end_date <> temp.effective_end_date or dwell_reg.scenario_no <> temp.scenario_no)
AND dwell_reg.scenario_no IN (SELECT id FROM @scenario_number)
AND (dwell_reg.sys_end_date IS NULL OR dwell_reg.sys_end_date > '4999-01-01')



/* ------------------------------------------------------------------------------------------------------------------------------------------------
	Find Part of Building-Dwelling UPDATEs
	Will be used to update both Part of Building-Dwelling and Building-Dwelling tables as needed.
	Separate from Dwelling-only changes, as the dwelling (and its relevant dates) could stay the same.
------------------------------------------------------------------------------------------------------------------------------------------------ */

IF OBJECT_ID('tempdb..#PoB_Updates') IS NOT NULL
    DROP TABLE #PoB_Updates

-- Find changes from part of building-dwelling table
SELECT
	temp.dwelling_id, 
	pobr.part_of_building_id AS old_pob_id, -- useful for matching
	temp.part_of_building_id,
	temp.building_id,
	temp.effective_start_date,
	temp.effective_end_date,
	temp.sys_create_date, 
	temp.sys_end_date,
	'N' AS sys_deleted_ind	-- would by picked up by deletes if Y
INTO #PoB_Updates
FROM #temp_dwell temp
JOIN [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Part_of_Building_Reference] pobr
ON temp.dwelling_id = pobr.dwelling_id AND temp.effective_start_date = pobr.effective_start_date
WHERE (temp.part_of_building_id IS NULL OR temp.part_of_building_id <> pobr.part_of_building_id)
-- Don't need anything already covered by inserts or deletes, they will be updated appropriately
AND CONCAT(temp.dwelling_id, temp.effective_start_date) NOT IN (SELECT CONCAT(dwelling_id, effective_start_date) FROM #dwell_deletes)
AND CONCAT(temp.dwelling_id, temp.effective_start_date) NOT IN (SELECT CONCAT(dwelling_id, effective_start_date) FROM #dwell_inserts)
AND (pobr.sys_end_date IS NULL OR pobr.sys_end_date > '4999-01-01')


-- Find changes from building-dwelling table
INSERT INTO #PoB_Updates
SELECT
	temp.dwelling_id, 
	'fake9999999999999test' AS old_pob_id, -- useful for matching
	temp.part_of_building_id,
	dbr.building_id,
	temp.effective_start_date,
	temp.effective_end_date,
	temp.sys_create_date, 
	temp.sys_end_date,
	'N' AS sys_deleted_ind	-- would by picked up by deletes if Y
FROM #temp_dwell temp
JOIN [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Building_Reference] dbr
ON temp.dwelling_id = dbr.dwelling_id AND temp.effective_start_date = dbr.effective_start_date
WHERE temp.part_of_building_id IS NOT NULL
-- Don't need anything already covered by inserts or deletes, they will be updated appropriately
AND CONCAT(temp.dwelling_id, temp.effective_start_date) NOT IN (SELECT CONCAT(dwelling_id, effective_start_date) FROM #dwell_deletes)
AND CONCAT(temp.dwelling_id, temp.effective_start_date) NOT IN (SELECT CONCAT(dwelling_id, effective_start_date) FROM #dwell_inserts)
AND (dbr.sys_end_date IS NULL OR dbr.sys_end_date > '4999-01-01')



/* ------------------------------------------------------------------------------------------------------------------------------------------------
	Check / print records
------------------------------------------------------------------------------------------------------------------------------------------------ */

SELECT COUNT(*) AS total_considered_records FROM #temp_dwell


SELECT COUNT(*) AS dwell_deletes, change FROM #dwell_deletes
group by change order by change
--ORDER BY dwelling_id, building_id, scenario_no, effective_start_date

SELECT COUNT(*) AS dwell_inserts, change FROM #dwell_inserts
group by change order by change
--ORDER BY dwelling_id, building_id, scenario_no, effective_start_date

SELECT COUNT(*) AS PoB_changes FROM #PoB_Updates
--ORDER BY dwelling_id, effective_start_date, building_id, part_of_building_id, old_pob_id


--select count(*) AS dwell_deletes_maintenance, change, effective_end_date 
--from (select --case when effective_start_date > '2024-06-07' then effective_start_date
--				--	else '2024-05-24' end as changed_start_date, 
--					* 
--		FROM #dwell_deletes) d
--group by change, effective_end_date order by change, effective_end_date





/* ------------------------------------------------------------------------------------------------------------------------------------------------
	The following sections update the final tables.
------------------------------------------------------------------------------------------------------------------------------------------------ */

-- Set up best practice parameters for changing tables
-- This automatically rolls back any changes if any errors occur (so the tables aren't affected if an error happens)
SET XACT_ABORT ON;
GO
-- This sets up the whole set of inserts and updates to happen in one go.
BEGIN TRAN;

/* ------------------------------------------------------------------------------------------------------------------------------------------------
	TEST SECTION
------------------------------------------------------------------------------------------------------------------------------------------------ */
-- Get an idea of the total counts of tables before the operations on them happen
--SELECT COUNT(*) AS dwell_reg, sys_end_date
--FROM [Sandbox_PlaceIndex].[Final_PlaceIndex].[Dwelling_Register_Skinny]
--GROUP BY sys_end_date ORDER BY sys_end_date;

--SELECT COUNT(*) AS dwell_build_ref, sys_end_date
--FROM [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Building_Reference]
--GROUP BY sys_end_date ORDER BY sys_end_date;

--SELECT COUNT(*) AS dwell_PoB_ref, sys_end_date
--FROM [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Part_of_Building_Reference]
--GROUP BY sys_end_date ORDER BY sys_end_date;
--GO

/* ------------------------------------------------------------------------------------------------------------------------------------------------
	UPDATEs/DELETEs
------------------------------------------------------------------------------------------------------------------------------------------------ */

-- Update existing records in the dwelling register (from UPDATEs and DELETEs)
BEGIN TRY
	UPDATE drs
	SET drs.effective_end_date = d.effective_end_date,
		drs.sys_end_date = d.sys_end_date, drs.sys_deleted_ind = d.sys_deleted_ind
	FROM [Sandbox_PlaceIndex].[Final_PlaceIndex].[Dwelling_Register_Skinny] AS drs
	JOIN #dwell_deletes AS d
	ON drs.dwelling_id = d.dwelling_id AND drs.sys_create_date = d.sys_create_date AND drs.effective_start_date = d.effective_start_date;
END TRY
BEGIN CATCH
	-- Raise an error and stop the whole insert/update section
	THROW;
END CATCH;
GO


-- Update dwelling building reference table from dwelling changes
BEGIN TRY
	UPDATE dbr
	SET dbr.effective_end_date = d.effective_end_date,
		dbr.sys_end_date = d.sys_end_date,
		dbr.sys_deleted_ind = d.sys_deleted_ind
	FROM #dwell_deletes AS d
	JOIN [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Building_Reference] dbr
	ON d.dwelling_id = dbr.dwelling_id AND d.effective_start_date = dbr.effective_start_date
	WHERE (dbr.sys_end_date IS NULL OR dbr.sys_end_date > '4999-01-01')
	AND (d.building_id <> dbr.building_id OR d.effective_end_date <> dbr.effective_end_date OR d.sys_end_date <> dbr.sys_end_date);
END TRY
BEGIN CATCH
	-- Raise an error and stop the whole insert/update section
	THROW;
END CATCH;
GO


-- Update dwelling part of building reference table from dwelling changes
BEGIN TRY
	UPDATE dpobr
	SET dpobr.effective_end_date = d.effective_end_date,
		dpobr.sys_end_date = d.sys_end_date,
		dpobr.sys_deleted_ind = d.sys_deleted_ind
	FROM #dwell_deletes AS d
	JOIN [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Part_of_Building_Reference] dpobr
	ON d.dwelling_id = dpobr.dwelling_id AND d.effective_start_date = dpobr.effective_start_date
	WHERE (dpobr.sys_end_date IS NULL OR dpobr.sys_end_date > '4999-01-01')
	AND (d.part_of_building_id <> dpobr.part_of_building_id OR d.effective_end_date <> dpobr.effective_end_date OR d.sys_end_date <> dpobr.sys_end_date);
END TRY
BEGIN CATCH
	-- Raise an error and stop the whole insert/update section
	THROW;
END CATCH;
GO



-- Update dwelling building reference table from part of building changes
BEGIN TRY
	UPDATE dbr
	SET dbr.effective_end_date = PoBd.effective_end_date,
		-- Since updates and inserts are handled from the same table, the sys_end_date should be set here
		dbr.sys_end_date = DATEADD(s, -1, PoBd.sys_create_date),	-- Takes the updated system create date for the new record -1 second and uses it for the system end date.	
		dbr.sys_deleted_ind = PoBd.sys_deleted_ind
	FROM #PoB_Updates AS PoBd
	JOIN [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Building_Reference] dbr
	ON PoBd.dwelling_id = dbr.dwelling_id AND PoBd.effective_start_date = dbr.effective_start_date AND PoBd.building_id = dbr.building_id
	WHERE (dbr.sys_end_date IS NULL OR dbr.sys_end_date > '4999-01-01')
	AND PoBd.part_of_building_id IS NOT NULL;
END TRY
BEGIN CATCH
	-- Raise an error and stop the whole insert/update section
	THROW;
END CATCH;
GO


-- Update dwelling part of building reference table from part of building changes
BEGIN TRY
	UPDATE dpobr
	SET dpobr.effective_end_date = PoBd.effective_end_date,
		-- Since updates and inserts are handled from the same table, the sys_end_date should be set here
		dpobr.sys_end_date = DATEADD(s, -1, PoBd.sys_create_date),	-- Takes the updated system create date for the new record -1 second and uses it for the system end date.	
		dpobr.sys_deleted_ind = PoBd.sys_deleted_ind
	FROM #PoB_Updates AS PoBd
	JOIN [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Part_of_Building_Reference] dpobr
	ON PoBd.dwelling_id = dpobr.dwelling_id AND PoBd.effective_start_date = dpobr.effective_start_date AND dpobr.part_of_building_id = PoBd.old_pob_id
	WHERE (dpobr.sys_end_date IS NULL OR dpobr.sys_end_date > '4999-01-01')
	AND (PoBd.part_of_building_id IS NULL OR PoBd.part_of_building_id <> dpobr.part_of_building_id);
END TRY
BEGIN CATCH
	-- Raise an error and stop the whole insert/update section
	THROW;
END CATCH;
GO




/* ------------------------------------------------------------------------------------------------------------------------------------------------
	INSERTs
------------------------------------------------------------------------------------------------------------------------------------------------ */

-- Put new records in dwelling register
BEGIN TRY
	INSERT INTO [Sandbox_PlaceIndex].[Final_PlaceIndex].[Dwelling_Register_Skinny] ([dwelling_id]
		  ,[scenario_no]
		  ,[effective_start_date]
		  ,[effective_end_date]
		  ,[sys_create_date]
		  ,[sys_end_date]
		  ,[sys_deleted_ind]
		  )
	SELECT i.[dwelling_id]
		  ,i.[scenario_no]
		  ,i.[effective_start_date]
		  ,i.[effective_end_date]
		  ,i.[sys_create_date]
		  ,i.[sys_end_date]
		  ,i.[sys_deleted_ind]
	FROM #dwell_inserts AS i
	LEFT OUTER JOIN (SELECT * FROM [Sandbox_PlaceIndex].[Final_PlaceIndex].[Dwelling_Register_Skinny]
						WHERE (sys_end_date IS NULL OR sys_end_date > '4999-01-01')) AS drs
	ON i.dwelling_id = drs.dwelling_id AND i.effective_start_date = drs.effective_start_date AND i.scenario_no <> drs.scenario_no;
END TRY
BEGIN CATCH
	-- Raise an error and stop the whole insert/update section
	THROW;
END CATCH;
GO


-- insert new records into dwelling building reference table when the dwelling has no link to part of building
BEGIN TRY
	INSERT INTO [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Building_Reference] ([dwelling_id]
		  ,[building_id]
		  ,[effective_start_date]
		  ,[effective_end_date]
		  ,[sys_create_date]
		  ,[sys_end_date]
		  ,[sys_deleted_ind]
		  )
	SELECT 
		i.dwelling_id,
		i.building_id,
		i.effective_start_date,
		i.effective_end_date,
		i.sys_create_date,
		i.sys_end_date,
		i.sys_deleted_ind
	FROM #dwell_inserts i
	LEFT OUTER JOIN (SELECT * FROM [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Building_Reference]
						WHERE (sys_end_date IS NULL OR sys_end_date > '4999-01-01')) AS dbr
	ON i.dwelling_id = dbr.dwelling_id AND i.effective_start_date = dbr.effective_start_date
	WHERE i.part_of_building_id is null;
END TRY
BEGIN CATCH
	-- Raise an error and stop the whole insert/update section
	THROW;
END CATCH;
GO


-- Put new records into the dwelling to part of building link table when the dwelling has a link to part of building
BEGIN TRY
	INSERT INTO [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Part_of_Building_Reference] ([dwelling_id]
		  ,[part_of_building_id]
		  ,[effective_start_date]
		  ,[effective_end_date]
		  ,[sys_create_date]
		  ,[sys_end_date]
		  ,[sys_deleted_ind]
		  )
	SELECT 
		i.dwelling_id,
		i.part_of_building_id,
		i.effective_start_date,
		i.effective_end_date,
		i.sys_create_date,
		i.sys_end_date,
		i.sys_deleted_ind
	FROM #dwell_inserts i
	LEFT OUTER JOIN (SELECT * FROM [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Part_of_Building_Reference]
						WHERE (sys_end_date IS NULL OR sys_end_date > '4999-01-01')) AS dbr
	ON i.dwelling_id = dbr.dwelling_id AND i.effective_start_date = dbr.effective_start_date
	WHERE i.part_of_building_id IS NOT NULL;
END TRY
BEGIN CATCH
	-- Raise an error and stop the whole insert/update section
	THROW;
END CATCH;
GO




-- insert new records into dwelling building reference table when the dwelling has no link to part of building
BEGIN TRY
	INSERT INTO [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Building_Reference] ([dwelling_id]
		  ,[building_id]
		  ,[effective_start_date]
		  ,[effective_end_date]
		  ,[sys_create_date]
		  ,[sys_end_date]
		  ,[sys_deleted_ind]
		  )
	SELECT 
		PoBi.dwelling_id,
		PoBi.building_id,
		PoBi.effective_start_date,
		PoBi.effective_end_date,
		PoBi.sys_create_date,
		PoBi.sys_end_date,
		PoBi.sys_deleted_ind
	FROM #PoB_Updates PoBi
	LEFT OUTER JOIN (SELECT * FROM [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Building_Reference]
						WHERE (sys_end_date IS NULL OR sys_end_date > '4999-01-01')) AS dbr
	ON PoBi.dwelling_id = dbr.dwelling_id AND PoBi.effective_start_date = dbr.effective_start_date
	WHERE PoBi.part_of_building_id IS NULL;
END TRY
BEGIN CATCH
	-- Raise an error and stop the whole insert/update section
	THROW;
END CATCH;
GO


-- Put new records into the dwelling to part of building link table when the part of building links have changed but the dwelling hasn't
BEGIN TRY
	INSERT INTO [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Part_of_Building_Reference] ([dwelling_id]
		  ,[part_of_building_id]
		  ,[effective_start_date]
		  ,[effective_end_date]
		  ,[sys_create_date]
		  ,[sys_end_date]
		  ,[sys_deleted_ind]
		  )
	SELECT 
		PoBi.dwelling_id,
		PoBi.part_of_building_id,
		PoBi.effective_start_date,
		PoBi.effective_end_date,
		PoBi.sys_create_date,
		PoBi.sys_end_date,
		PoBi.sys_deleted_ind
	FROM #PoB_Updates PoBi
	LEFT OUTER JOIN (SELECT * FROM [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Part_of_Building_Reference]
						WHERE (sys_end_date IS NULL OR sys_end_date > '4999-01-01')) AS dbr
	ON PoBi.dwelling_id = dbr.dwelling_id AND PoBi.effective_start_date = dbr.effective_start_date
	WHERE PoBi.part_of_building_id IS NOT NULL;
END TRY
BEGIN CATCH
	-- Raise an error and stop the whole insert/update section
	THROW;
END CATCH;
GO


/* ----------------------------------------------------------------------------------------------------------------------------------------------
	LOGGING - Log the total number of changes to each table
   --------------------------------------------------------------------------------------------------------------------------------------------*/
DECLARE @change_date DATETIME2(0) = GETDATE();

-- Log changes to dwelling register
BEGIN TRY
	INSERT INTO [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Table_Changelog]
	SELECT @change_date AS [change_date],
	'Dwelling_Register_Skinny' AS [table_name],
	'Dwelling Transfer Step' AS [process],
	(SELECT COUNT(*) FROM #dwell_inserts WHERE change = 'INSERT') AS [records_added],
	(SELECT COUNT(*) FROM #dwell_inserts WHERE change = 'UPDATE') AS [records_modified],
	(SELECT COUNT(*) FROM #dwell_deletes WHERE change = 'DELETE') AS [records_retired],
	(SELECT COUNT(*) FROM [Sandbox_PlaceIndex].[Final_PlaceIndex].[Dwelling_Register_Skinny]
		WHERE sys_end_date IS NULL OR sys_end_date > '4999-01-01') AS [total_records_after],
	NULL AS [source],
	NULL AS [source_from_date],
	NULL AS [source_to_date];
END TRY
BEGIN CATCH
	THROW;
END CATCH;

-- Log changes to dwelling-building reference table
BEGIN TRY
	INSERT INTO [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Table_Changelog]
	SELECT @change_date AS [change_date],
	'Dwelling_Building_Reference' AS [table_name],
	'Dwelling Transfer Step' AS [process],
	(SELECT COUNT(*) FROM #dwell_inserts WHERE change = 'INSERT' AND part_of_building_id IS NULL) AS [records_added],
	(SELECT (SELECT COUNT(*) FROM #dwell_inserts WHERE change = 'UPDATE' AND part_of_building_id IS NULL) + 
		(SELECT COUNT(*) FROM #PoB_Updates WHERE part_of_building_id IS NULL)) AS [records_modified],
	(SELECT COUNT(*) FROM [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Building_Reference]
		WHERE sys_end_date = (select top 1 sys_end_date from #dwell_deletes)) AS [records_retired],
	(SELECT COUNT(*) FROM [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Building_Reference]
		WHERE sys_end_date IS NULL OR sys_end_date > '4999-01-01') AS [total_records_after],
	NULL AS [source],
	NULL AS [source_from_date],
	NULL AS [source_to_date];
END TRY
BEGIN CATCH
	THROW;
END CATCH;

-- Log changes to dwelling-part of building reference table
BEGIN TRY
	INSERT INTO [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Table_Changelog]
	SELECT @change_date AS [change_date],
	'Dwelling_Part_of_Building_Reference' AS [table_name],
	'Dwelling Transfer Step' AS [process],
	(SELECT COUNT(*) FROM #dwell_inserts WHERE change = 'INSERT' AND part_of_building_id IS NOT NULL) AS [records_added],
	(SELECT (SELECT COUNT(*) FROM #dwell_inserts WHERE change = 'UPDATE' AND part_of_building_id IS NOT NULL) + 
		(SELECT COUNT(*) FROM #PoB_Updates WHERE part_of_building_id IS NOT NULL)) AS [records_modified],
	(SELECT COUNT(*) FROM [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Part_of_Building_Reference]
		WHERE sys_end_date = (select top 1 sys_end_date from #dwell_deletes)) AS [records_retired],
	(SELECT COUNT(*) FROM [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Part_of_Building_Reference]
		WHERE sys_end_date IS NULL OR sys_end_date > '4999-01-01') AS [total_records_after],
	NULL AS [source],
	NULL AS [source_from_date],
	NULL AS [source_to_date];
END TRY
BEGIN CATCH
	THROW;
END CATCH;
GO


/* ------------------------------------------------------------------------------------------------------------------------------------------------
	TEST SECTION
------------------------------------------------------------------------------------------------------------------------------------------------ */
-- Get an idea of the total counts of tables after the operations on them happen
--SELECT COUNT(*) AS dwell_reg, sys_end_date
--FROM [Sandbox_PlaceIndex].[Final_PlaceIndex].[Dwelling_Register_Skinny]
--GROUP BY sys_end_date ORDER BY sys_end_date;

--SELECT COUNT(*) AS dwell_build_ref, sys_end_date
--FROM [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Building_Reference]
--GROUP BY sys_end_date ORDER BY sys_end_date;

--SELECT COUNT(*) AS dwell_PoB_ref, sys_end_date
--FROM [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Part_of_Building_Reference]
--GROUP BY sys_end_date ORDER BY sys_end_date


-- Get a count of the total changes
SELECT * FROM [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Table_Changelog]
WHERE change_date > DATEADD(HOUR, -2, GETDATE());
GO

/* Commit the whole set of inserts and updates at once */
-- Complete the transaction
IF @@TRANCOUNT > 0
    COMMIT TRAN;
	
	/* ------------------------------------------------------------------------------------------------------------------------------------------------
		TEST SECTION - comment out the above COMMIT statement and uncomment the below lines
	------------------------------------------------------------------------------------------------------------------------------------------------ */
	--print 'completed'
	--ROLLBACK TRAN; -- This should be in any TRY...CATCH statements
GO




/* ----------------------------------------------------------------------------------------------------------------------------------------------
	CLEAN UP - drop the temporary tables from memory
   --------------------------------------------------------------------------------------------------------------------------------------------*/
IF OBJECT_ID('tempdb..#temp_dwell') IS NOT NULL
    DROP TABLE #temp_dwell

IF OBJECT_ID('tempdb..#dwell_deletes') IS NOT NULL
    DROP TABLE #dwell_deletes

IF OBJECT_ID('tempdb..#dwell_inserts') IS NOT NULL
    DROP TABLE #dwell_inserts

IF OBJECT_ID('tempdb..#PoB_Updates') IS NOT NULL
    DROP TABLE #PoB_Updates





/* ----------------------------------------------------------------------------------------------------------------------------------------------
	TEST SECTION - checks and balances
   --------------------------------------------------------------------------------------------------------------------------------------------*/

/*
SELECT TOP (1000) *
  FROM [Sandbox_PlaceIndex].[Final_PlaceIndex].[Dwelling_Register_Skinny]
  where scenario_no = 0 or scenario_no = 99
  order by dwelling_id, effective_start_date, sys_create_date


SELECT TOP (1000) *
  FROM [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Building_Reference]
  where building_id = 0 or building_id > 9999990
  order by dwelling_id, effective_start_date, sys_create_date
 
 SELECT TOP (1000) *
  FROM [Sandbox_PlaceIndex].[Processing_PlaceIndex].[Dwelling_Part_of_Building_Reference]
  where part_of_building_id like 'tst%'
  order by dwelling_id, effective_start_date, sys_create_date

*/