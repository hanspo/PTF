IF IS_MEMBER(N'db_owner') = 0
   PRINT 'Only members of sysadmin or db_owner role are allowed to uninstall PTF'

ELSE IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'ptf')
   PRINT 'PTF is not installed in database ' + DB_NAME()

ELSE
BEGIN
   DECLARE @ver nvarchar(10)
   SELECT TOP 1 @ver =
          SUBSTRING([description], 13, CHARINDEX(N' installed', [description]) - 13)
     FROM ptf.Log
    WHERE [description] LIKE N'PTF Version%'
    ORDER BY [time] DESC

   PRINT '================================================================'
   PRINT ' Uninstalling PTF version ' + @ver + ' from database ' + DB_NAME()
   PRINT '================================================================'
   PRINT ''

   DECLARE @sql nvarchar(512)
   DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
   SELECT N'DROP ' +
          CASE type WHEN 'P'  THEN N'PROCEDURE '
                    WHEN 'FN' THEN N'FUNCTION '
            		WHEN 'V'  THEN N'VIEW '
	      END +
	      QUOTENAME(SCHEMA_NAME(schema_id)) + N'.' + QUOTENAME(name)
     FROM sys.objects
    WHERE schema_id IN (SCHEMA_ID(N'ptf'), SCHEMA_ID(N'ptf_internal')) AND
          type IN ('P', 'FN', 'V')

   OPEN cur
   FETCH NEXT FROM cur INTO @sql
   WHILE @@FETCH_STATUS = 0
   BEGIN
      EXEC sp_executesql @sql
      FETCH NEXT FROM cur INTO @sql
   END
   CLOSE cur
   DEALLOCATE cur

   DROP TABLE  ptf.Log   
   DROP SCHEMA ptf
   DROP TABLE ptf_internal.ptf_options
   DROP SCHEMA ptf_internal

   PRINT '================================================================'
   PRINT ' PTF has been uninstalled'
   PRINT '================================================================'
END
 