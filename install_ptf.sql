-- When executing the script within SSMS, make sure to activate SQLCMD Mode !!!
:On error exit
DECLARE @version nvarchar(10)
SET @version = N'3.0.19'

IF NOT (SERVERPROPERTY('EngineEdition') = 3 OR
   SERVERPROPERTY('EngineEdition') IN (2, 4) AND @@MICROSOFTVERSION > 0x0D000FA0 OR
   SERVERPROPERTY('EngineEdition') = 5 AND @@MICROSOFTVERSION > 0x0D000000)
BEGIN
   RAISERROR ('PTF is not supported on this Edition of SQL Server', 11, 127)
   RETURN
END

IF @@MICROSOFTVERSION < 0x0A000000
BEGIN
   RAISERROR ('SQL Server 2008 or later is required to run this version of PTF', 11, 127)
   RETURN
END

IF DB_ID() IN (1, 2, 4)
BEGIN
   RAISERROR ('PTF cannot be installed in system databases', 11, 127)
   RETURN
END

IF IS_MEMBER('db_owner') = 0
BEGIN
   RAISERROR('You must be a sysadmin or DB owner to install PTF', 11, 127)
   RETURN
END


SET ANSI_NULLS, ANSI_PADDING, ANSI_WARNINGS ON
SET ARITHABORT, QUOTED_IDENTIFIER, CONCAT_NULL_YIELDS_NULL ON
SET NUMERIC_ROUNDABORT OFF


IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'ptf')
BEGIN
   EXECUTE sp_executesql N'CREATE SCHEMA ptf AUTHORIZATION dbo'
END
ELSE IF OBJECT_ID(N'ptf.Log', N'U') IS NOT NULL
BEGIN
   SET NOCOUNT ON
   DECLARE @ver2 nvarchar(10), @last_ver int
   DECLARE @idx1 int, @idx2 int, @len int

   SELECT TOP 1 @ver2 =
          SUBSTRING([description], 13, CHARINDEX(N' installed', [description]) - 13)
     FROM ptf.Log
    WHERE [description] LIKE N'PTF Version%'
    ORDER BY [time] DESC

   IF @ver2 IS NOT NULL
   BEGIN
      SELECT @idx1 = CHARINDEX(N'.', @ver2),
             @idx2 = CHARINDEX(N'.', REVERSE(@ver2)),
             @len  = LEN(@ver2)

      SET @last_ver = (
          CAST(SUBSTRING(@ver2, 1, @idx1 - 1) AS int) * 100 +
          CAST(SUBSTRING(@ver2, @idx1 + 1, @len - @idx1 - @idx2) AS int)) * 10000 +
          CAST(SUBSTRING(@ver2, @len - @idx2 + 2, @idx2 - 1) AS int)

      SELECT @idx1 = CHARINDEX(N'.', @version),
             @idx2 = CHARINDEX(N'.', REVERSE(@version)),
             @len  = LEN(@version)

      if @last_ver >= (
         CAST(SUBSTRING(@version, 1, @idx1 - 1) AS int) * 100 +
         CAST(SUBSTRING(@version, @idx1 + 1, @len - @idx1 - @idx2) AS int)) * 10000 +
         CAST(SUBSTRING(@version, @len - @idx2 + 2, @idx2 - 1) AS int)
      BEGIN
         RAISERROR(N'PTF is already up-to-date. Current version: %s', 11, 127, @ver2)
         RETURN
      END
   END

   EXEC ptf.ClearLog
   GOTO _log_version
END

CREATE TABLE ptf.Log (
   [id] int NOT NULL IDENTITY,
   [spid] int NOT NULL,
   [time] datetime NOT NULL,
   [description] nvarchar(max) NOT NULL,
   [state] int NOT NULL
) ON [PRIMARY]

_log_version:

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'ptf_internal')
   EXECUTE sp_executesql N'CREATE SCHEMA ptf_internal AUTHORIZATION dbo'
ELSE IF OBJECT_ID(N'ptf_internal.ptf_options', N'U') IS NOT NULL
   EXECUTE sp_executesql N'DROP TABLE ptf_internal.ptf_options'

CREATE TABLE ptf_internal.ptf_options ([id] int NOT NULL, [value] sysname)

GRANT EXECUTE, SELECT ON SCHEMA::ptf TO public

DECLARE @sql nvarchar(1024)
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

PRINT '================================================================'
PRINT ' Installing PTF version ' + @version + ' in database ' + DB_NAME()
PRINT '================================================================'
PRINT ''

SET NOCOUNT ON
INSERT ptf.Log ([spid], [time], [description], [state])
VALUES (0, GETUTCDATE(), N'PTF Version ' + @version + N' installed', 0)
GO
:On error ignore

CREATE PROCEDURE ptf.ClearLog
AS
set nocount on
select 0 as [spid], [time], [description], [state] into #Log
  from ptf.Log
 where [description] like N'PTF Version%' or
       [description] = N'AddLoopbackLink' and [state] = 0
truncate table ptf.Log
insert into ptf.Log select * from #Log order by [time]
drop table #Log
GO


CREATE FUNCTION ptf.GetVersion () RETURNS nvarchar(10) AS
BEGIN
   declare @version nvarchar(10)

   select top 1 @version =
          substring([description], 13, charindex(N' installed', [description]) - 13)
     from ptf.Log
    where [description] like N'PTF Version%'
    order by [time] desc

   return isnull(@version, N'0.0.0')
END
GO


CREATE PROCEDURE ptf_internal.InsLog @spid int, @descr nvarchar(max), @state int
AS
set nocount on
insert into ptf.Log ([spid], [time], [description], [state])
values (@spid, getutcdate(), @descr, @state)
GO
GRANT EXECUTE ON ptf_internal.InsLog TO public
GO

CREATE PROCEDURE ptf_internal.Trace @text nvarchar(max),
   @state int = 0, @debug int = 0, @n int = 0,
   @p1 nvarchar(256) = null, @p2 sql_variant = null,
   @p3 sql_variant = null, @p4 sql_variant = null,
   @p5 sql_variant = null, @p6 sql_variant = null,
   @p7 sql_variant = null, @p8 sql_variant = null,
   @p9 sql_variant = null, @p10 sql_variant = null,
   @p11 sql_variant = null
AS
if @debug > 1 or @text is null return

if @n >=  1 set @text = @text + N' (' + ptf_internal.VariantToString(@p1)
if @n >=  2 set @text = @text + N', ' + ptf_internal.VariantToString(@p2)
if @n >=  3 set @text = @text + N', ' + ptf_internal.VariantToString(@p3)
if @n >=  4 set @text = @text + N', ' + ptf_internal.VariantToString(@p4)
if @n >=  5 set @text = @text + N', ' + ptf_internal.VariantToString(@p5)
if @n >=  6 set @text = @text + N', ' + ptf_internal.VariantToString(@p6)
if @n >=  7 set @text = @text + N', ' + ptf_internal.VariantToString(@p7)
if @n >=  8 set @text = @text + N', ' + ptf_internal.VariantToString(@p8)
if @n >=  9 set @text = @text + N', ' + ptf_internal.VariantToString(@p9)
if @n >= 10 set @text = @text + N', ' + ptf_internal.VariantToString(@p10)
if @n >= 11 set @text = @text + N', ' + ptf_internal.VariantToString(@p11)
if @n >   0 set @text = @text + N')'

if @debug = 1
   print convert(nvarchar(100), getdate(), 121) + N': ' + @text +
         N'; ' + cast(@state as nvarchar)

declare @ls sysname
select @ls = value from ptf_internal.ptf_options where id = 1

if xact_state() <> 0 and @ls is not null
begin
   declare @sql nvarchar(256)
   set @sql = @ls + N'.[' + db_name() + N'].ptf_internal.InsLog @p1, @p2, @p3'
   begin try
      exec sp_executesql @sql, N'@p1 int, @p2 nvarchar(max), @p3 int',
           @@spid, @text, @state
      return
   end try
   begin catch
      if xact_state() = -1 return
   end catch
end

exec ptf_internal.InsLog @@spid, @text, @state
GO


CREATE PROCEDURE ptf_internal.ExecStmt @stmt nvarchar(max), @debug int
AS
declare @err int
if isnull(len(@stmt), 0) > 0
begin
   if isnull(@debug, 0) in (0, 1)
   begin
      exec sp_executesql @stmt
      set @err = @@error
      exec ptf_internal.Trace @stmt, @err, @debug
      return @err
   end
   print @stmt
end
return 0
GO


CREATE PROCEDURE ptf_internal.HandleError
   @debug int = 0,
   @proc_name sysname = NULL,
   @last_stmt nvarchar(max) = NULL
AS
declare @err int, @msg nvarchar(4000), @sev int, @state int

select @err = error_number(), @msg = error_message(),
       @sev = error_severity(), @state = error_state()

if @err < 50000
begin
   exec ptf_internal.Trace @msg, @err, @debug
   set @msg = N'Error in ' + error_procedure() + N': ' +
       cast(@err as nvarchar) + N', ' + @msg
end

if @last_stmt is not null
   exec ptf_internal.Trace @last_stmt, @err, @debug

if @proc_name is not null
   exec ptf_internal.Trace @proc_name, -1, @debug

raiserror (@msg, @sev, @state)
GO


CREATE PROCEDURE ptf.AddLoopbackLink AS
if serverproperty('EngineEdition') = 5
   return 0

declare @ls sysname, @ds nvarchar(512), @sn nvarchar(512), @sql nvarchar(512)
set @ls = N'ptf_loopback'
set @ds = cast(serverproperty('ServerName') as nvarchar(512))
-- Does this also work with VNN's on FCI instances ??

exec sp_executesql
     N'select @sn = data_source collate database_default from sys.servers where name = @ls',
     N'@ls sysname, @sn nvarchar(512) output', @ls, @sn output 

if @sn is not null and @sn = @ds and exists (
   select * from ptf_internal.ptf_options where id = 1 and value = @ls)
   return 0

if is_srvrolemember('sysadmin') = 0 and
   has_perms_by_name(null, null, 'ALTER ANY LINKED SERVER') <> 1
begin
   raiserror('You do not have permission to create a linked server', 16, 1)
   return 1
end

begin try
   set @sql =
       N'if @sn is not null exec sp_dropserver @ls;' +
       N'exec sp_addlinkedserver @ls, @srvproduct = N'''', @provider = ''SQLNCLI'', @datasrc = @ds;' +
       N'exec sp_serveroption @ls, ''remote proc transaction promotion'', ''false'';' +
       N'exec sp_serveroption @ls, ''rpc out'', ''true'';'

   exec sp_executesql @sql, N'@sn nvarchar(512), @ls sysname, @ds nvarchar(512)', @sn, @ls, @ds 
   insert ptf_internal.ptf_options values (1, @ls)
   exec ptf_internal.Trace N'AddLoopbackLink'
   return 0
end try

begin catch
   exec ptf_internal.HandleError 0, N'AddLoopbackLink'
   return 1
end catch
GO


CREATE FUNCTION ptf_internal.VariantToString (@val sql_variant) RETURNS nvarchar(max) AS
BEGIN
   if @val is null return N'NULL'

   declare @t int = (select system_type_id from sys.types
                      where name = cast(sql_variant_property(@val, 'BaseType') as sysname))

   return case when @t in (40, 41, 42, 43, 58, 61)
               then N'''' + convert(nvarchar(max), @val, 21) + N''''
               when @t in (36, 167, 175, 231, 239)
               then N'''' + convert(nvarchar(max), @val) + N''''
               when @t in (165, 173) then convert(nvarchar(max), @val, 1)
               when @t in (60, 122) then convert(nvarchar(max), @val, 2)
               else convert(nvarchar(max), @val)
          end
END
GO


CREATE FUNCTION ptf_internal.GetTypeStr (@fid int, @pid int) RETURNS nvarchar(256) AS
BEGIN
   return (
      select t.name +
             case when p.system_type_id in (165, 167, 173, 175, 231, 239)
                  then N'(' +
                       cast(p.max_length / (p.system_type_id / 100) as nvarchar) +
                       N')'
                  when p.system_type_id in (106, 108)
                  then N'(' + cast(p.precision as nvarchar) +
                       N',' + cast(p.scale as nvarchar) + N')'
                  when p.system_type_id in (41, 42, 43)
                  then N'(' + cast(p.scale as nvarchar) + N')'
                  else N''
             end
        from sys.partition_parameters p join sys.types t
          on t.user_type_id = p.system_type_id 
       where p.function_id = @fid and p.parameter_id = @pid
      )
END
GO


CREATE PROCEDURE ptf_internal.CastVariant @fid int, @pid int, @val sql_variant OUTPUT
AS
declare @stmt nvarchar(512)

select @stmt = N'SELECT @val = CAST(@val AS ' +
       ptf_internal.GetTypeStr(p.function_id, p.parameter_id) +
       case when p.collation_name is null or p.collation_name = t.collation_name
            then N')' else N') COLLATE ' + p.collation_name
       end
  from sys.partition_parameters p join sys.types t
    on t.user_type_id = p.system_type_id 
 where p.function_id = @fid and p.parameter_id = @pid

exec sp_executesql @stmt, N'@val sql_variant output', @val = @val output
GO


CREATE FUNCTION ptf_internal.GetCreateIndexStmt (
   @tab_id int,
   @index_id int,
   @tab_name nvarchar(256),
   @constraint nvarchar(256),
   @opt nvarchar(256),
   @part_col_id int,
   @ind_exists bit
)
RETURNS nvarchar(max)
BEGIN
   if @tab_id is null or @index_id is null or
      isnull(len(@tab_name), 0) = 0 or @opt is null
      return null

   declare @cl1 nvarchar(max) = N'', @cl2 nvarchar(max) = N''

   select @cl1 = @cl1 + N', ' + quotename(col_name(@tab_id, column_id)) +
          case when is_descending_key = 0 then N'' else N' DESC' end
     from sys.index_columns
    where object_id = @tab_id and index_id = @index_id and key_ordinal > 0
    order by key_ordinal

   if @index_id = 1
      select @cl1 = @cl1 + N', ' + quotename(col_name(@tab_id, column_id))
        from sys.index_columns
       where object_id = @tab_id and index_id = 1 and key_ordinal = 0 and partition_ordinal > 0
       order by partition_ordinal
   else
      select @cl2 = @cl2 + N', ' + quotename(col_name(@tab_id, column_id))
        from sys.index_columns
       where object_id = @tab_id and index_id = @index_id and (is_included_column = 1 or
             key_ordinal = 0 and partition_ordinal > 0 and not exists (
             select * from sys.indexes where object_id = @tab_id and type = 1))
       order by index_column_id

   if (select is_unique from sys.indexes where object_id = @tab_id and index_id = @index_id) = 0 and
      isnull(@part_col_id, 0) > 0 and @part_col_id not in (
      select column_id from sys.index_columns where object_id = @tab_id and index_id = @index_id) 
   begin
      if @index_id = 1
         set @cl1 = @cl1 + N', ' + quotename(col_name(@tab_id, @part_col_id))
      else if not exists (select * from sys.indexes where object_id = @tab_id and type = 1)
         set @cl2 = @cl2 + N', ' + quotename(col_name(@tab_id, @part_col_id))
   end

   if @ind_exists = 1
      set @opt = @opt + N', DROP_EXISTING = ON'

   return (
      select case when is_primary_key = 0 and is_unique_constraint = 0 or @ind_exists = 1
                  then N'CREATE' +
                       case when is_unique = 0 then N'' else N' UNIQUE' end +
                       case when type in (1, 5) then N' CLUSTERED' else N'' end +
                       case when type in (5, 6) then N' COLUMNSTORE' else N'' end +
                       N' INDEX ' + quotename(name) + N' ON ' + @tab_name
                  else N'ALTER TABLE ' + @tab_name + N' ADD' + isnull(@constraint, N'') +
                       case when is_primary_key = 1
                            then N' PRIMARY KEY' +
                                 case when type = 2 then N' NONCLUSTERED' else N'' end
                            else N' UNIQUE' +
                                 case when type = 1 then N' CLUSTERED' else N'' end
                       end
             end +
             case when type = 5 then N''
                  else case when len(@cl1) = 0 then N'' else stuff(@cl1, 1, 2, N' (') + N')' end +
                       case when len(@cl2) = 0 then N''
                            else case when len(@cl1) = 0 then N'' else N' INCLUDE' end +
                                 stuff(@cl2, 1, 2, N' (') + N')'
                       end +
                       case when has_filter = 0 then N'' else N' WHERE ' + filter_definition end
             end +
             case when len(@opt) = 0 then N'' else N' WITH' + stuff(@opt, 1, 2, N' (') + N')' end
        from sys.indexes
       where object_id = @tab_id and index_id = @index_id
   )
END
GO


CREATE FUNCTION ptf_internal.GetPartitionCheck (
   @pf_id int,
   @pcol_name sysname,
   @is_nullable bit,
   @part_num int,
   @boundary sql_variant
)
RETURNS nvarchar(512) AS
BEGIN
   declare @num_part int, @is_right bit
   declare @val1 sql_variant, @val2 sql_variant

   if @pf_id is null or isnull(len(@pcol_name), 0) = 0 or @part_num is null
      return null

   select @num_part = fanout, @is_right = boundary_value_on_right
     from sys.partition_functions where function_id = @pf_id

   if @num_part is null or @part_num not between 1 and @num_part
      return null

   select @val1 = value from sys.partition_range_values
    where function_id = @pf_id and parameter_id = 1 and boundary_id = @part_num - 1

   select @val2 = value from sys.partition_range_values
    where function_id = @pf_id and parameter_id = 1 and boundary_id = @part_num

   if @boundary is not null and (@val1 is null or @val1 < @boundary) and
      (@part_num = @num_part or @val2 is not null and @val2 > @boundary)
   begin
      set @num_part = @num_part + 1

      if @is_right = 0 or @part_num > 1
         set @val2 = @boundary
      else
         select @val1 = @boundary, @part_num = 2
   end

   return
      case when @num_part = 1 then null
           when @part_num = 1 and @val2 is null
           then case when @is_right = 1 then null
                     else quotename(@pcol_name) + N' IS NULL'
                end
           when @part_num = 2 and @val1 is null and @num_part = 2
           then case when @is_right = 1 then null
                     else quotename(@pcol_name) + N' IS NOT NULL'
                end
           else case when @is_nullable = 1 and (@part_num = 1 or
                          @part_num = 2 and @is_right = 1 and @val1 is null)
                     then quotename(@pcol_name) + N' IS NULL OR '
                     else case when @part_num = 1 or isnull(@is_nullable, 0) = 0 then N''
                               else quotename(@pcol_name) + ' IS NOT NULL AND '
                          end +
                          case when @part_num = 1 or @val1 is null then N''
                               else quotename(@pcol_name) + N' >' +
                                    case when @is_right = 0 then N' ' else N'= ' end +
                                    ptf_internal.VariantToString(@val1) +
                                    case when @part_num = @num_part
                                         then N'' else N' AND '
                                    end 
                          end
                end +
                case when @part_num = @num_part then N''
                     else quotename(@pcol_name) + N' <' +
                          case when @is_right = 0 then N'= ' else N' ' end +
                          ptf_internal.VariantToString(@val2)
                end
      end
END
GO


CREATE PROCEDURE ptf_internal.GetIndexInfo @tab_id int, @part_num int = 1, @src bit = 0
AS
declare @objid int, @objname nvarchar(256), @idxid int, @idxtype tinyint
declare @idxname nvarchar(128), @unique bit, @is_pk bit
declare @fgid int, @filter nvarchar(max), @compr int
declare @keys varchar(max), @cols varchar(max), @is_clustered int

declare @col_map table (id int primary key, pos int not null);

declare @idxtab table (
   src bit not null, id int identity not null, ref int null, 
   obj_name nvarchar(256) not null, idx_type tinyint not null,
   idx_name nvarchar(128) null, is_unique bit not null, is_pk bit not null,
   fg_id int not null, part_compr int not null, idx_keys varchar(max) not null,
   idx_filter nvarchar(max) not null, view_cols varchar(max) not null
)

declare idxcur cursor local fast_forward for
   select i.object_id,
          quotename(schema_name(o.schema_id)) + N'.' + quotename(o.name),
          i.index_id, i.type, i.name, i.is_unique, i.is_primary_key,
          isnull(dds.data_space_id, i.data_space_id),
          isnull(i.filter_definition, N''), p.data_compression
     from sys.indexes i
     join sys.objects o on o.object_id = i.object_id
     join sys.partitions p on p.partition_number = @part_num and
          p.object_id = i.object_id and p.index_id = i.index_id
     left join sys.destination_data_spaces dds
       on dds.partition_scheme_id = i.data_space_id and
          dds.destination_id = p.partition_number
    where i.type in (0, 1, 2, 5, 6) and
          i.is_disabled = 0 and i.is_hypothetical = 0 and (
          i.object_id = @tab_id or i.object_id in (
           select referencing_id from sys.sql_expression_dependencies
            where referenced_id = @tab_id and
                  referencing_id in (select object_id from sys.views)))
    order by i.object_id, i.index_id

open idxcur
fetch next from idxcur into @objid, @objname, @idxid, @idxtype,
   @idxname, @unique, @is_pk, @fgid, @filter, @compr

while  @@fetch_status = 0
begin
   if @idxid <= 1
   begin
      set @is_clustered = @idxid

      delete from @col_map

      insert @col_map select column_id, row_number() over (order by column_id)
        from sys.columns where object_id = @objid
   end

   select @keys = '', @cols = ''

   if @idxtype not in (0, 5)
   begin
      select @keys = @keys + cast(cm.pos as varchar) +
             case when ic.is_descending_key = 0 then '' else '-' end + ';'
        from sys.index_columns ic join @col_map cm on cm.id = ic.column_id
       where ic.object_id = @objid and ic.index_id = @idxid and ic.key_ordinal > 0
       order by ic.key_ordinal

      if @idxtype = 1
         select @keys = @keys + cast(cm.pos as varchar) + ';'
           from sys.index_columns ic join @col_map cm on cm.id = ic.column_id
          where ic.object_id = @objid and ic.index_id = 1 and
                ic.key_ordinal = 0 and ic.partition_ordinal > 0
          order by ic.partition_ordinal
      else
         select @keys = @keys + ';' + cast(cm.pos as varchar)
           from sys.index_columns ic join @col_map cm on cm.id = ic.column_id
          where ic.object_id = @objid and ic.index_id = @idxid and (ic.is_included_column <> 0 or
                @is_clustered = 0 and ic.key_ordinal = 0 and ic.partition_ordinal > 0)
          order by ic.index_column_id

      if @objid <> @tab_id
      begin
         select @cols = cast(checksum_agg(referenced_minor_id) as varchar)
           from sys.sql_expression_dependencies
          where referencing_id = @objid and referenced_id = @tab_id

         select @cols = @cols + ';' + cast(referenced_id as varchar) +
                ':' + cast(checksum_agg(referenced_minor_id) as varchar)
           from sys.sql_expression_dependencies
          where referencing_id = @objid and referenced_id <> @tab_id
          group by referenced_id
          order by referenced_id
      end
   end

   insert @idxtab (src, obj_name, idx_type, idx_name, is_unique, is_pk,
          fg_id, part_compr, idx_keys, idx_filter, view_cols)
   values (@src, @objname, @idxtype, @idxname, @unique, @is_pk,
          @fgid, @compr, @keys, @filter, @cols)

   fetch next from idxcur into @objid, @objname, @idxid, @idxtype,
      @idxname, @unique, @is_pk, @fgid, @filter, @compr
end

close idxcur
deallocate idxcur

select * from @idxtab
GO


CREATE PROCEDURE ptf_internal.ModifyFileGroups
   @tab_id int, @pf_id int, @pnum int, @cnt int, @ro bit, @debug int
AS
declare @stmt nvarchar(max)

select @stmt = N''
select @stmt = @stmt + N';' + nchar(13) + nchar(10) + N'ALTER DATABASE ' +
       quotename(db_name()) + N' MODIFY FILEGROUP ' + quotename(name) +
       case when is_read_only = 0 then N' READ_ONLY' else N' READ_WRITE' end
  from sys.filegroups
 where is_read_only = @ro and (
       @pf_id is null and data_space_id in (
          select data_space_id from sys.indexes where object_id = @tab_id union all
          select lob_data_space_id from sys.tables where object_id = @tab_id union all
          select filestream_data_space_id from sys.tables
           where object_id = @tab_id and filestream_data_space_id is not null) or
       @pf_id is not null and data_space_id in (
          select data_space_id from sys.destination_data_spaces
           where destination_id >= @pnum and destination_id < @pnum + @cnt and
                 partition_scheme_id in (
                    select data_space_id from sys.partition_schemes where function_id = @pf_id)))

if len(@stmt) > 0
begin
   set @stmt = right(@stmt, len(@stmt) - 3)

   begin try
      exec ptf_internal.ExecStmt @stmt, @debug
   end try

   begin catch
      declare @err int = error_number()
      if @ro = 0 and @err = 5070
         exec ptf_internal.Trace @stmt, @err, @debug
      else
         exec ptf_internal.HandleError @debug, null, @stmt
   end catch
end
GO


CREATE PROCEDURE ptf_internal.GetObjId
   @obj_name   nvarchar(256),
   @is_tab     bit = 0,
   @id         int = NULL OUTPUT,
   @quote_name nvarchar(256) = NULL OUTPUT,
   @name       sysname = NULL OUTPUT,
   @schema     sysname = NULL OUTPUT
AS
declare @sch nvarchar(256), @obj varchar(6), @level int 

set @level = 0
set @obj = case when @is_tab = 1 then N'table' else N'object' end

if isnull(len(@obj_name), 0) = 0
   raiserror('a %s name must be specified', 16, 1, @obj)

while @obj_name is not null and @level < 2
begin
   select @name = parsename(@obj_name, 1), @sch  = parsename(@obj_name, 2)

   if @sch is not null and schema_id(@sch) is null or
      parsename(@obj_name, 3) is not null and parsename(@obj_name, 3) <> db_name() or
      parsename(@obj_name, 4) is not null
      raiserror('''%s'' is not a valid %s name', 16, 1, @obj_name, @obj)

   set @schema = coalesce(
                    @sch,
                    (select default_schema_name from sys.database_principals
                      where name = current_user),
                    (select top 1 default_schema_name from sys.database_principals
                      where (type = 'G' and is_member(suser_sname(sid)) = 1 or
                             type = 'X' and is_member(name) = 1) and
                            default_schema_name is not null
                      order by principal_id),
                    N'dbo')

   select @level = @level + 1, @id = null, @obj_name = null
   select @id = o.object_id, @name = o.name,
          @schema = schema_name(o.schema_id),
          @obj_name = s.base_object_name
     from sys.objects o left join sys.synonyms s on s.object_id = o.object_id
    where o.name = @name and o.schema_id = schema_id(@schema) and (
          @is_tab = 0 or o.type = 'U' or o.type = 'SN')
end

if @obj_name is not null
   raiserror('a synonym must not reference another synonym', 16, 1, @obj)

set @quote_name = case when @id is not null then quotename(@schema) + N'.'
                       else isnull(quotename(@sch) + N'.', N'')
                  end + quotename(@name) 
GO


CREATE PROCEDURE ptf_internal.GetLowPrioWaitOption
   @table_id      int,
   @low_prio_wait int,
   @abort_action  nvarchar(8),
   @wait_option   nvarchar(80) OUTPUT
AS
if @abort_action is not null and
   lower(@abort_action) not in (N'none', N'self', N'blockers')
   raiserror('''%s'' is not a valid abort action', 16, 1, @abort_action)

else if isnull(@low_prio_wait, 0) not between 0 and 24 * 60
   raiserror('''%d'' is not a valid low priority wait duration', 16, 1, @low_prio_wait)

else if @@microsoftversion < 0x0C000000 or @abort_action is null or
   lower(@abort_action) = 'none' and isnull(@low_prio_wait, 0) = 0
   set @wait_option = null

else if lower(@abort_action) = N'blockers' and serverproperty('EngineEdition') <> 5 and
   has_perms_by_name(null, null, N'ALTER ANY CONNECTION') <> 1
   raiserror('you do not have permission to disconnect blockers', 16, 1)

else if exists (
   select * from sys.sql_expression_dependencies
    where referenced_id <> @table_id and referencing_id in (
          select referencing_id from sys.sql_expression_dependencies
           where referenced_id = @table_id and
                 referencing_id in (select object_id from sys.views) and
                 referencing_id in (select object_id from sys.indexes where index_id = 1)))
   raiserror('the table has incompatible indexed views for low priority wait', 16, 1)

else
   set @wait_option =
       N'WAIT_AT_LOW_PRIORITY (MAX_DURATION = ' +
       cast(isnull(@low_prio_wait, 0) as nvarchar) +
       N', ABORT_AFTER_WAIT = ' + upper(@abort_action) + N')'
GO


CREATE PROCEDURE ptf_internal.HandleVersioning
   @tab_id    int,
   @temp_mask tinyint,
   @off_stmt  nvarchar(1024) OUTPUT,
   @on_stmt   nvarchar(1024) OUTPUT,
   @temp_type tinyint = NULL OUTPUT
AS
declare @stmt nvarchar(1024)

set @stmt = N'select @tt = ' + case when @@microsoftversion < 0x0D000000 then N'0' else
            N'       t.temporal_type,' +
            N'       @tn = case when t.temporal_type = 1 then' +
            N'       quotename(schema_name(x.schema_id)) + N''.'' + quotename(x.name) else' +
            N'       quotename(schema_name(t.schema_id)) + N''.'' + quotename(t.name) end,' +
            N'       @hn = case when t.temporal_type = 2 then' +
            N'       quotename(schema_name(h.schema_id)) + N''.'' + quotename(h.name) else' +
            N'       quotename(schema_name(t.schema_id)) + N''.'' + quotename(t.name) end' +
            N'  from sys.tables t' +
            N'  left join sys.tables x on x.history_table_id = t.object_id' +
            N'  left join sys.tables h on h.object_id = t.history_table_id' +
            N' where t.object_id = @tid' end

exec sp_executesql @stmt,
     N'@tid int, @tt tinyint output, @tn nvarchar(1024) output, @hn nvarchar(1024) output',
     @tab_id, @temp_type output, @off_stmt output, @on_stmt output

if (@temp_type & @temp_mask) = 0
   select @off_stmt = N'', @on_stmt = N''
else
begin
   set @off_stmt = N'ALTER TABLE ' + @off_stmt + N' SET (SYSTEM_VERSIONING = '
   set @on_stmt  = case when (@temp_mask & 4) = 4
                        then N';' + nchar(13) + nchar(10) + N'DROP TABLE ' + @on_stmt
                        else @off_stmt + N'ON (HISTORY_TABLE = ' + @on_stmt +
                             N', DATA_CONSISTENCY_CHECK = OFF))'
                   end
   set @off_stmt = @off_stmt + N'OFF);' + nchar(13) + nchar(10)
end
GO


CREATE PROCEDURE ptf_internal.UpdateStatistics
   @table_name nvarchar(256),
   @update_all bit,
   @update_inc bit,
   @out_part   int,
   @in_part    int,
   @debug      int
AS
declare @tab_id int, @num_all int, @num_inc int
declare @stats1 nvarchar(max), @stats2 nvarchar(max), @opt nvarchar(64)
declare @sql nvarchar(512), @stmt nvarchar(max)

declare @all_stats table (name sysname, no_recomp bit, is_inc int)

if @@microsoftversion < 0x0C000000 set @update_all = 1

exec ptf_internal.GetObjId @table_name, 1, @tab_id output

if @tab_id is null or @update_all = 0 and
   @update_inc = 1 and @out_part is null and @in_part is null
   return

set @opt = case when @update_all = 1 or @update_inc = 0 then N''
                else N'RESAMPLE ON PARTITIONS' + stuff(
                     case when @out_part is null then N''
                          else N', ' + cast(@out_part as nvarchar)
                     end +
                     case when @in_part is null then N''
                          else N', ' + cast(@in_part as nvarchar)
                     end, 1, 2, N' (') + N')'
           end

set @sql = N' select s.name, s.no_recompute,' +
           case when @@microsoftversion < 0x0C000000 then N' 0' else
           N'        s.is_incremental'
           end +
           N'   from sys.stats s left join sys.indexes i' +
           N'     on i.object_id = s.object_id and i.index_id = s.stats_id' +
           N'  where s.object_id = @tid and isnull(i.type, 0) not in (5, 6)'

insert @all_stats exec sp_executesql @sql, N'@tid int', @tab_id

if @update_all = 0
begin
   select @num_all = count(*), @num_inc = sum(is_inc) from @all_stats

   if @update_inc = 1 and @num_inc = 0 or @update_inc = 0 and @num_inc = @num_all
      return

   if @num_inc = 0 or @num_inc = @num_all
      set @update_all = 1
end


select @stats1 = N'', @stats2 = N''
select @stats1 = @stats1 +
                 case when no_recomp = 1 then N'' else N', ' + quotename(name) end,
       @stats2 = @stats2 +
                 case when no_recomp = 0 then N'' else N', ' + quotename(name) end
  from @all_stats
 where @update_all = 1 or is_inc = @update_inc


begin try
   if len(@stats1) > 0
   begin
      set @stmt = N'UPDATE STATISTICS ' + @table_name +
          case when @update_all = 1 and len(@stats2) = 0 then N''
               else stuff(@stats1, 1, 2, N' (') + N')'
          end +
          case when len(@opt) = 0 then N'' else N' WITH ' + @opt end

      exec ptf_internal.ExecStmt @stmt, @debug
   end

   if len(@stats2) > 0
   begin
      set @stmt = N'UPDATE STATISTICS ' + @table_name +
          case when @update_all = 1 and len(@stats1) = 0 then N''
               else stuff(@stats2, 1, 2, N' (') + N')'
          end + N' WITH NORECOMPUTE' +
          case when len(@opt) = 0 then N'' else N', ' + @opt end

      exec ptf_internal.ExecStmt @stmt, @debug
   end
end try

begin catch
   exec ptf_internal.HandleError @debug, null, @stmt
end catch

return
GO


CREATE PROCEDURE ptf.CreateTableClone
   @source_tab_id int,                   -- object ID of source table
   @target_table  nvarchar(256),         -- name of clone table to be created
   @fg_name       sysname = NULL OUTPUT, -- filegroup on which table is being created
   @part_num      int = NULL,            -- partition where table will be switched in
   @ps_id         int = NULL,            -- Id of a partition scheme
   @switch_source bit = 0,               -- if 1, the table's purpose is switch source
   @debug         int = 0
AS
set nocount on

declare @stmt nvarchar(max), @stmt2 nvarchar(max), @col_def nvarchar(max)
declare @in_row_limit int, @is_filetable bit
declare @tab_id int, @fg_id int, @tx_id int, @fs_id int, @id int

declare @col_options table (col_id int primary key, opt_type tinyint, opt_str nvarchar(512))

begin try
   if @source_tab_id is null
      raiserror('an object ID for the source table must be specified', 16, 1)

   else if not exists (select * from sys.tables where object_id = @source_tab_id)
      raiserror('''%d'' is not a valid table ID', 16, 1, @source_tab_id)

   set @stmt2 =
       N' select @tirl = text_in_row_limit, @isft = ' +
       case when @@microsoftversion < 0x0B000000 then N'0' else N'is_filetable' end +
       N'   from sys.tables where object_id = @oid'

   exec sp_executesql @stmt2, N'@oid int, @tirl int output, @isft bit output',
        @source_tab_id, @in_row_limit output, @is_filetable output

   if @is_filetable = 1
      raiserror('a FILETABLE source is not supported', 16, 1)

   exec ptf_internal.GetObjId @target_table, 0, @tab_id output, @target_table output

   if @tab_id is not null and isnull(@debug, 0) <> 2
      raiserror('an object with name ''%s'' already exists', 16, 1, @target_table)


   select @tx_id = lob_data_space_id, @fs_id = filestream_data_space_id
     from sys.tables where object_id = @source_tab_id

   if @ps_id is null
   begin
      select @id = data_space_id from sys.indexes
       where object_id = @source_tab_id and index_id in (0, 1)

      if isnull(@id, 0) = 0
         select @id = data_space_id from sys.filegroups where type = 'FG' and is_default = 1
   end
   else
   begin
      select @id = ps1.data_space_id, @fs_id = ps2.data_space_id
        from sys.partition_schemes ps1 left join sys.partition_schemes ps2
          on ps2.function_id = ps1.function_id and ps2.data_space_id in (
             select partition_scheme_id from sys.destination_data_spaces
			  where data_space_id in (select data_space_id from sys.filegroups where type = 'FD'))
       where ps1.data_space_id = @ps_id

      if @id is null or @id = @fs_id
         raiserror('''%d'' is not a valid partition scheme Id', 16, 1, @ps_id)
   end

   if @id not in (select data_space_id from sys.partition_schemes)
      set @fg_id = case when isnull(@part_num, 1) = 1 then @id else null end
   else if @part_num is not null
   begin
      select @fg_id = data_space_id from sys.destination_data_spaces
       where partition_scheme_id = @id and destination_id = @part_num
      select @fs_id = data_space_id from sys.destination_data_spaces
       where partition_scheme_id = @fs_id and destination_id = @part_num
      select @tx_id = case when @tx_id = 0 then 0 else @fg_id end 
   end
   else
      raiserror('a partition number must be specified', 16, 1)

   if @fg_id is null
      raiserror('''%d'' is not a valid partition number', 16, 1, @part_num)

   if @fg_name is not null
   begin
      select @fg_id = data_space_id from sys.filegroups
       where type = 'FG' and (lower(@fg_name) = N'default' and is_default = 1 or name = @fg_name)

      if @fg_id is null
         raiserror('''%s'' is not a valid filegroup name', 16, 1, @fg_name)
   end

   set @fg_name = filegroup_name(@fg_id)

   if isnull(@debug, 0) <> 2 and @fg_id not in (
         select data_space_id from sys.database_files except
         select data_space_id from sys.database_files where state <> 0 except
         select data_space_id from sys.filegroups where is_read_only = 1)
      raiserror('the table cannot be created in filegroup ''%s''', 16, 1, @fg_name)


   set @stmt2 = N'select column_id, 1,' +
                N'       N'' GENERATED ALWAYS AS ROW '' +' +
                N'       case when generated_always_type = 1 then N''START'' else N''END'' end +' +
                N'       N'' HIDDEN''' +
                N'  from sys.columns' +
                N' where object_id = @tid and generated_always_type > 0 and @add_temporal = 1 ' +
                N'union all ' + 
                N'select 0, 2,' +
                N'       N'','' + nchar(13) + nchar(10) + nchar(9) +' +
                N'       N''PERIOD FOR SYSTEM_TIME ('' +' +
                N'       quotename(col_name(object_id, start_column_id)) + N'', '' +' +
                N'       quotename(col_name(object_id, end_column_id)) + N'')''' +
                N'  from sys.periods where object_id = @tid and @add_temporal = 1 ' +
                N'union all ' + 
                N'select c.column_id, 3,' +
                N'       nchar(13) + nchar(10) + nchar(9) +' +
                N'       N''ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = '' +' +
                N'       quotename(cek.name) + N'', ENCRYPTION_TYPE = '' +' +
                N'       c.encryption_type_desc collate database_default +' +
                N'       N'', ALGORITHM = '''''' + c.encryption_algorithm_name + N'''''')''' +
                N'  from sys.columns c join sys.column_encryption_keys cek' +
                N'    on cek.column_encryption_key_id = c.column_encryption_key_id' +
                N' where c.object_id = @tid'

   if @@microsoftversion >= 0x0D000000
      insert into @col_options
         exec sp_executesql @stmt2, N'@tid int, @add_temporal bit', @source_tab_id, @switch_source

   if @source_tab_id in (select object_id from sys.columns where is_filestream = 1)
      insert into @col_options
      select c.column_id, 4,
             case when i.is_primary_key = 1
                  then N' PRIMARY KEY' + case when i.type = 1 then N'' else N' NONCLUSTERED' end
                  else N' UNIQUE' + case when i.type = 1 then N' CLUSTERED' else N'' end
             end +
             case when p.data_compression = 0 then N''
                  else N' WITH (DATA_COMPRESSION = ' +
                       case p.data_compression when 1 then N'ROW' when 2 then N'PAGE' end + N')'
             end + N' ON ' +
             quotename(filegroup_name(isnull(dds.data_space_id, i.data_space_id)))
        from sys.columns c join sys.index_columns ic
          on ic.object_id = c.object_id and ic.column_id = c.column_id and ic.key_ordinal = 1 
        join sys.indexes i on i.object_id = ic.object_id and i.index_id = ic.index_id and
             (i.is_primary_key = 1 or i.is_unique_constraint = 1)
        join sys.partitions p on p.object_id = i.object_id and p.index_id = i.index_id and
             p.partition_number = case when @ps_id is null then isnull(@part_num, 1) else 1 end
        left join sys.destination_data_spaces dds on dds.destination_id = @part_num and
             dds.partition_scheme_id = isnull(@ps_id, i.data_space_id)
       where c.object_id = @source_tab_id and c.is_rowguidcol = 1 and c.is_nullable = 0


   declare col_cur cursor local fast_forward for
   select quotename(c.name) +
          case when c.is_computed <> 0
               then N' AS ' + cc.definition collate database_default +
                    case when cc.is_persisted = 0 then N''
                         else N' PERSISTED' +
                              case when c.is_nullable = 0 then N' NOT NULL' else N'' end
                    end
               when c.user_type_id = 189 and lower(c.name) = t.name then N''
               else N' ' +
                    case when t.schema_id <> schema_id(N'sys')
                         then quotename(schema_name(t.schema_id)) +
                              N'.' + quotename(t.name)
                         else case when c.user_type_id = 189
                                   then N'rowversion' else t.name
                              end +
                              case when c.user_type_id in (165, 167, 173, 175, 231, 239)
                                   then N'(' +
                                        case when c.max_length = -1 then N'max'
                                             else cast(c.max_length / (c.user_type_id / 100)
                                                       as nvarchar)
                                        end + N')'
                                   when c.user_type_id in (106, 108) and
                                        (c.precision <> 18 or c.scale <> 0)
                                   then N'(' + cast(c.precision as nvarchar) +
                                        case when c.scale = 0 then N''
                                             else N', ' + cast(c.scale as nvarchar)
                                        end + N')'
                                   when c.user_type_id in (41, 42, 43)
                                   then case when c.scale = 7 then N''
                                             else N'(' + cast(c.scale as nvarchar) + N')'
                                        end +
                                        case when co.opt_type = 1 then co.opt_str else N'' end
                                   when c.user_type_id = 36 and c.is_rowguidcol <> 0
                                   then N' ROWGUIDCOL'
                                   when c.user_type_id = 241 and x.name is not null
                                   then N'(' +
                                        case when c.is_xml_document = 0
                                             then N'' else N'DOCUMENT '
                                        end +
                                        quotename(schema_name(x.schema_id)) + N'.' +
                                        quotename(x.name) + N')'
                                   when c.user_type_id = 241 and c.is_column_set <> 0
                                   then N' COLUMN_SET FOR ALL_SPARSE_COLUMNS'
                                   else N''
                              end
                    end +
                    case when c.is_filestream = 0 then N'' else N' FILESTREAM' end +
                    case when t.system_type_id not in (35, 99, 167, 175, 231, 239) or
                              c.collation_name is null or
                              c.collation_name = t.collation_name
                         then N'' else N' COLLATE ' + c.collation_name
                    end +
                    case when c.is_column_set <> 0 then N''
                         else case when c.is_nullable = 0 then N' NOT'
                                   when c.is_sparse = 1 then N' SPARSE'
                                   else N''
                              end + N' NULL'
                    end +
                    case when co.opt_type = 3 then co.opt_str else N'' end +
                    case when co.opt_type = 4 then co.opt_str else N'' end +
                    case when dc.definition is null or @switch_source = 0 then N''
                         else N' DEFAULT ' + dc.definition collate database_default
                    end
          end
     from sys.columns c
     join sys.types t on t.user_type_id = c.user_type_id
     left join sys.default_constraints dc
       on dc.parent_object_id = c.object_id and dc.parent_column_id = c.column_id
     left join sys.computed_columns cc
       on cc.object_id = c.object_id and cc.column_id = c.column_id
     left join sys.xml_schema_collections x on x.xml_collection_id = c.xml_collection_id
     left join @col_options co on co.col_id = c.column_id
    where c.object_id = @source_tab_id
    order by c.column_id

   set @stmt = N''
   open col_cur
   fetch next from col_cur into @col_def
   while @@fetch_status = 0
   begin
      set @stmt += N',' + nchar(13) + nchar(10) + nchar(9) + @col_def
      fetch next from col_cur into @col_def
   end
   close col_cur
   deallocate col_cur

   set @stmt = N'CREATE TABLE ' + @target_table + stuff(@stmt, 1, 1, N' (') +
       case when not exists (select * from @col_options where opt_type = 2) then N''
            else (select opt_str from @col_options where opt_type = 2)
       end + nchar(13) + nchar(10) + N') ON ' + quotename(@fg_name) +
       case when @tx_id = 0 or @tx_id = @fg_id then N''
            else N' TEXTIMAGE_ON ' + quotename(filegroup_name(@tx_id))
       end +
       case when @fs_id is null then N''
            else N' FILESTREAM_ON ' + quotename(filegroup_name(@fs_id))
       end

   exec ptf_internal.ExecStmt @stmt, @debug

   if @in_row_limit > 0
   begin
      set @stmt = N'sp_tableoption ''' + @target_table + N''', ''text in row'', ''' +
          case when @in_row_limit = 256 then N'ON' else cast(@in_row_limit as varchar) end + N''''

      exec ptf_internal.ExecStmt @stmt, @debug
   end

   return 0
end try

begin catch
   exec ptf_internal.HandleError @debug, null, @stmt
   return 1
end catch
GO


CREATE PROCEDURE ptf.CreateIndex
   @source_tab_id     int,                  -- object ID of source table or indexed view
   @index_id          int,                  -- ID of index on source table/view
   @target_object     nvarchar(256) = NULL, -- name of object to create index for
   @constraint_name   sysname = NULL,       -- name of key constraint to be created
   @part_num          int = NULL,           -- partition, where index will be switched in
   @ps_id             int = NULL,           -- ID of partition scheme for filegroup mapping
   @part_scheme       sysname = NULL,       -- partition scheme to create index on
   @part_col_id       int = NULL,           -- column ID of partitioning column
   @compression       int = NULL,           -- if not null, apply compression to index/table
   @incremental_stats bit = 0,              -- 1, if index created with incremental stats
   @debug             int = 0
AS
set nocount on

declare @stmt nvarchar(max), @base_type char(2), @index_type tinyint
declare @index_compr int, @index_name sysname, @is_active bit, @fs_idx_type tinyint
declare @tab_id int, @full_name nvarchar(256), @sch_name sysname, @constraint nvarchar(256)
declare @fs_name sysname, @id int, @fg_id int, @fg_name sysname, @tab_fs_idx int
declare @tab_type tinyint, @tab_idx int, @target_type char(2), @tab_fg int, @tab_compr int
declare @compr_opt nvarchar(256), @opt nvarchar(256)

begin try
   if @source_tab_id is null
      raiserror('an ID for the source object must be specified', 16, 1)

   select @base_type = type from sys.objects where object_id = @source_tab_id

   if @base_type is null or @base_type not in ('U', 'V')
      raiserror('''%d'' is not a valid object ID for a table\view', 16, 1, @source_tab_id)

   if @index_id is null
      raiserror('an index ID must be specified', 16, 1)

   select @index_type  = i.type,
          @index_compr = p.data_compression,
          @index_name  =
             case when i.is_primary_key = 0 and i.is_unique_constraint = 0 and i.index_id > 0
                  then i.name else null
             end,
          @is_active   =
             case when i.is_disabled = 0 and i.is_hypothetical = 0 then 1 else 0 end,
          @fs_idx_type =
             case when i.object_id in (
                          select object_id from sys.columns where is_filestream = 1) and
                       (i.is_primary_key = 1 or i.is_unique_constraint = 1) and
                       c.is_rowguidcol = 1
                  then i.type else 0
             end
     from sys.indexes i join sys.partitions p
       on p.object_id = i.object_id and p.index_id = i.index_id and p.partition_number = 1
     left join sys.index_columns ic
       on ic.object_id = i.object_id and ic.index_id = i.index_id and ic.key_ordinal = 1
     left join sys.columns c on c.object_id = ic.object_id and c.column_id = ic.column_id
    where i.object_id = @source_tab_id and i.index_id = @index_id

   if @index_type is null
   begin
      if @index_id = 0 and @base_type = 'U'
         select @index_type = 0, @index_compr = 0, @fs_idx_type = 0
      else
         raiserror('''%d'' is not a valid index ID', 16, 1, @index_id)
   end

   else if @index_type not in (0, 1, 2, 5, 6, 7)
      raiserror('an index of type ''%d'' is not supported', 16, 1, @index_type)


   if @target_object is not null
      exec ptf_internal.GetObjId @target_object, 0, @tab_id output,
           @quote_name = @full_name output, @schema = @sch_name output
   
   else if @part_scheme is null
      raiserror('the name of the target object must be specified', 16, 1)
   else if @index_type in (5, 6)
      raiserror('a columnstore index cannot be partitioned', 16, 1)
   else
      select @tab_id = object_id, @sch_name = schema_name(schema_id),
             @full_name = quotename(schema_name(schema_id)) + N'.' + quotename(name)
        from sys.tables where object_id = @source_tab_id


   if @constraint_name is null
      set @constraint = N''
   else if isnull(@debug, 0) <> 2 and @index_id > 0 and @index_name is null and exists (
      select * from sys.objects where name = @constraint_name and schema_id = schema_id(@sch_name))
      raiserror('a constraint with name ''%s'' cannot be created', 16, 1, @constraint_name)
   else
      set @constraint = N' CONSTRAINT ' + quotename(@constraint_name)


   if not (@compression is null or @compression in (0, 1, 2, 3, 4))
      raiserror('''%d'' is not a valid compression setting', 16, 1, @compression)
   else if @compression in (1, 2) and
      @source_tab_id in (select object_id from sys.columns where is_sparse = 1)
      raiserror('a table that contains sparse columns cannot be compressed', 16, 1)


   if @part_scheme is null
   begin
      if @ps_id is null
      begin
         select @id = data_space_id from sys.indexes
          where object_id = @source_tab_id and index_id = @index_id

         if isnull(@id, 0) = 0
            select @id = data_space_id from sys.filegroups where type = 'FG' and is_default = 1

         set @part_col_id = null
      end
      else if @ps_id in (select data_space_id from sys.partition_schemes)
         set @id = @ps_id
      else
         raiserror('''%d'' is not a valid partition scheme Id', 16, 1, @ps_id)

      if @id not in (select data_space_id from sys.partition_schemes)
         set @fg_id = case when isnull(@part_num, 1) = 1 then @id else null end
      else if @part_num is null
         raiserror('a partition number must be specified', 16, 1)
      else
      begin
         select @fg_id = data_space_id from sys.destination_data_spaces
          where partition_scheme_id = @id and destination_id = @part_num

         select @index_compr = data_compression from sys.partitions
          where object_id = @source_tab_id and index_id = @index_id and
                partition_number = case when @ps_id is null then @part_num else 1 end
      end

      if @fg_id is null
         raiserror('''%d'' is not a valid partition number', 16, 1, @part_num)

      set @fg_name = filegroup_name(@fg_id)

      if isnull(@debug, 0) <> 2 and @fg_id not in (
            select data_space_id from sys.database_files except
            select data_space_id from sys.database_files where state <> 0 except
            select data_space_id from sys.filegroups where is_read_only = 1)
         raiserror('an index cannot be created in filegroup ''%s''', 16, 1, @fg_name)
   end

   else if isnull(@debug, 0) <> 2 and @part_scheme not in (
      select name from sys.partition_schemes where data_space_id not in (
             select partition_scheme_id from sys.destination_data_spaces
              where data_space_id in (select data_space_id from sys.filegroups where type = 'FD')))
      raiserror('''%s'' is not a valid partition scheme name', 16, 1, @part_scheme)

   if @part_scheme is not null or @ps_id is not null
   begin
      if @base_type = 'V'
         raiserror('an indexed view cannot be partitioned', 16, 1)

      if @part_col_id is null
         raiserror('a column ID for the partitioning column must be specified', 16, 1)
      else if @part_col_id not in (
         select column_id from sys.columns where object_id = @source_tab_id)
         raiserror('''%d'' is not a valid column ID', 16, 1, @part_col_id)
      else if @part_col_id in (
         select column_id from sys.columns where object_id = @source_tab_id and is_sparse = 1)
         raiserror('the partitioning column must not be sparse', 16, 1)
      else if @part_col_id in (
         select column_id from sys.computed_columns
          where object_id = @source_tab_id and is_persisted = 0)
         raiserror('a computed partitioning column must be a persisted', 16, 1)
      else if isnull(@debug, 0) <> 2 and not exists (
         select *
           from sys.partition_schemes ps join sys.partition_parameters pp
             on pp.function_id = ps.function_id and pp.parameter_id = 1
           join sys.columns c
             on c.system_type_id = pp.system_type_id and
                c.user_type_id = pp.system_type_id and
                c.max_length = pp.max_length and
                c.precision = pp.precision and c.scale = pp.scale and
                (c.collation_name is null and pp.collation_name is null or
                c.collation_name = pp.collation_name)             
          where (ps.name = @part_scheme or ps.data_space_id = @ps_id) and
                c.object_id = @source_tab_id and c.column_id = @part_col_id)
         raiserror('the column type is incompatible with the partition function', 16, 1)
   end

   if @source_tab_id in (select object_id from sys.columns where is_filestream = 1) and
      @index_type in (0, 1) and @part_scheme is not null
      select top 1 @fs_name = quotename(name) from sys.partition_schemes
       where function_id in (
                select function_id from sys.partition_schemes where name = @part_scheme) and
             data_space_id in (
                select partition_scheme_id from sys.destination_data_spaces
                 where data_space_id in (
                       select data_space_id from sys.filegroups where type = 'FD'))


   if @tab_id is not null
   begin
      select @target_type = o.type,
             @tab_type    = isnull(i.type, 0),
             @tab_fg      = i.data_space_id,
             @tab_compr   = p.data_compression,
             @tab_fs_idx  = fsi.index_id
        from sys.objects o left join sys.indexes i
          on i.object_id = o.object_id and i.index_id in (0, 1)
        left join sys.partitions p
          on p.object_id = i.object_id and p.index_id = i.index_id and p.partition_number = 1
        left join sys.indexes fsi
        join sys.index_columns ic
          on ic.object_id = fsi.object_id and ic.index_id = fsi.index_id and ic.key_ordinal = 1
        join sys.columns c
          on c.object_id = ic.object_id and c.column_id = ic.column_id and c.is_rowguidcol = 1
          on fsi.object_id = o.object_id and fsi.type = @fs_idx_type and
             (fsi.is_primary_key = 1 or fsi.is_unique_constraint = 1)
       where o.object_id = @tab_id

      if @target_type <> @base_type
         raiserror('the type of target object does not match with source object', 16, 1)

      if @target_type = 'U' and isnull(@tab_fg, 0) = 0
         raiserror('the target table must not be memory optimized', 16, 1)
   end
   else if @debug = 2
      select @tab_type = 0, @tab_idx = 0, @tab_fg = @fg_id, @tab_compr = 0, @tab_fs_idx = null
   else if @base_type = 'U'
      raiserror('the table ''%s'' does not exist', 16, 1, @target_object)
   else
      raiserror('the view ''%s'' does not exist', 16, 1, @target_object)

   if @index_id > 1 and @is_active = 0
      return 0

   if @compression is null
      set @compression = @index_compr
   else if @compression = 4 and @@microsoftversion < 0x0C000000
      set @compression = 3

   set @compr_opt =
       case when @compression in (0, 1, 2) and @index_type in (5, 6) or
                 @compression in (3, 4) and @index_type not in (5, 6) or
                 @compression in (0, 3) and (
                    @target_object is null or @index_id = 1 and @tab_type = 0 or 
                    @index_id > 1 and (@tab_fs_idx is null or @part_scheme is not null)) or
                 @compression = @tab_compr and
                    @index_id <= 1 and @part_scheme is null and @fg_id = @tab_fg
            then null
            else N'DATA_COMPRESSION = ' +
                 case @compression
                      when 0 then N'NONE' when 1 then N'ROW' when 2 then N'PAGE'
                      when 3 then N'COLUMNSTORE' when 4 then N'COLUMNSTORE_ARCHIVE'
                 end
       end

   set @stmt =   N'select @opt = ' +
       case when @@microsoftversion < 0x0D000000
            then case when @compr_opt is null then N'N''''' else N'N'', '' + @compr' end
            else N'       case when type not in (5, 6) or isnull(compression_delay, 0) = 0' +
                 N'            then N''''' +
                 N'            else N'', COMPRESSION_DELAY = '' +' +
                 N'                 cast(compression_delay as nvarchar)' +
                 N'       end +' + 
                 N'       case when @compr is null then N'''' else N'', '' + @compr end' +
                 N'  from sys.indexes where object_id = @oid and index_id = @iid'
       end

   exec sp_executesql @stmt,
        N'@oid int, @iid int, @compr nvarchar(256), @opt nvarchar(256) output',
        @source_tab_id, @index_id, @compr_opt, @opt output


   if isnull(@debug, 0) <> 2
   begin
      if @tab_type = 0 and @base_type = 'V' and @index_id > 1
         raiserror('for indexed views the clustered index must be created first', 16, 1)

      if @part_scheme is not null and @index_type not in (0, 1) and
         @tab_fg not in (select data_space_id from sys.partition_schemes)
         raiserror('the table must be partitioned before creating a partitioned index', 16, 1)
   end


   if @index_id > 0 and (@target_object is null or
      @tab_fs_idx is not null and @part_scheme is not null) or
      @tab_fs_idx is null and (@index_id = 1 and @tab_type = 0 or @index_id > 1) 
   begin
      if not (@tab_id is null or @index_name is null or @target_object is null) and
         @index_name in (select name from sys.indexes where object_id = @tab_id)
         raiserror('the index ''%s'' already exists on table/view', 16, 1, @index_name)

      set @stmt = ptf_internal.GetCreateIndexStmt (
                     case when @tab_fs_idx is null then @source_tab_id else @tab_id end,
                     isnull(@tab_fs_idx, @index_id), @full_name, @constraint, @opt,
                     case when @part_scheme is null then @part_col_id else null end,
                     case when @target_object is null or @tab_fs_idx is not null then 1 else 0 end)

      if @part_scheme is null and (@base_type = 'V' and @index_id = 1 or @fg_id <> @tab_fg)
         set @stmt = @stmt + N' ON ' + quotename(@fg_name)

      else if @part_scheme is not null and
              (@index_type = 1 or @target_object is null or @tab_fs_idx is not null)
         set @stmt = @stmt + N' ON ' + quotename(@part_scheme) + N'(' +
             quotename(col_name(@source_tab_id, @part_col_id)) + N')' +
             case when @fs_name is null then N'' else N' FILESTREAM_ON ' + @fs_name end

      if @target_object is null
      begin
         select @index_name = i.name, @opt =
                case when i.is_padded = 0
                     then N'' else N', PAD_INDEX = ON'
                end +
                case when i.ignore_dup_key = 0
                     then N'' else N', IGNORE_DUP_KEY = ON'
                end +
                case when @incremental_stats = 1 and @@microsoftversion >= 0x0C000000
                     then N', STATISTICS_NORECOMPUTE = ON, STATISTICS_INCREMENTAL = ON'
                     when isnull(s.no_recompute, 0) = 0 then N''
                     else N', STATISTICS_NORECOMPUTE = ON'
                end +
                case when i.allow_row_locks = 0
                     then N', ALLOW_ROW_LOCKS = OFF' else N''
                end +
                case when i.allow_page_locks = 0
                     then N', ALLOW_PAGE_LOCKS = OFF' else N''
                end +
                case when i.fill_factor = 0 then N''
                     else N', FILLFACTOR = ' + cast(i.fill_factor as nvarchar)
                end
           from sys.indexes i left join sys.stats s
             on s.object_id = i.object_id and s.stats_id = i.index_id
          where i.object_id = @source_tab_id and i.index_id = @index_id and i.type in (1, 2)

         if isnull(len(@opt), 0) > 0
            set @stmt = @stmt + N';' + nchar(13) + nchar(10) +
                N'ALTER INDEX ' + quotename(@index_name) + N' ON ' + @full_name +
                N' REBUILD WITH' + stuff(@opt, 1, 2, N' (') + N')'
      end
   end 

   else if @part_scheme is null and (@tab_fs_idx is not null or
      @index_type = @tab_type and @fg_id = @tab_fg and @base_type = 'U')
   begin
      if @index_type = 5 or @index_type = 2 and @compr_opt is not null 
         select @stmt = N'ALTER INDEX ' + quotename(name) + N' ON '
           from sys.indexes
          where object_id = @tab_id and index_id = isnull(@tab_fs_idx, 1)
      else if @compr_opt is not null
         set @stmt = N'ALTER TABLE '
      else
         return 0

      set @stmt = @stmt + @full_name +
          case when @compr_opt is not null
               then N' REBUILD WITH (' + @compr_opt + N')'
               when @@microsoftversion < 0x0D000000 then N' REBUILD'
               else N' REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON)'
          end
   end

   else if @index_id = 0 and @tab_type = 0 and (@part_scheme is not null or @fg_id <> @tab_fg)
   begin
      declare @tmp_name nvarchar(64) = quotename(N'IX_' + cast(newid() as nvarchar(36)))

      select top 1 @stmt = N'CREATE CLUSTERED INDEX ' + @tmp_name +
             N' ON ' + @full_name + N'(' + quotename(name) + N')' +
             case when @compr_opt is null then N'' else N' WITH (' + @compr_opt + N')' end +
             N' ON ' +
             case when @part_scheme is null then quotename(@fg_name)
                  else quotename(@part_scheme) + N'(' + quotename(name) + N')' +
                       case when @fs_name is null then N'' else N' FILESTREAM_ON ' + @fs_name end
             end + N';' + nchar(13) + nchar(10) +
             N'DROP INDEX ' + @tmp_name + N' ON ' + @full_name
        from sys.columns
       where object_id = @source_tab_id and is_sparse = 0 and
             (@part_scheme is null or column_id = @part_col_id)
       order by is_nullable,
             case when max_length = -1 or system_type_id in (34, 35, 99)
                  then 8000 else max_length
             end, column_id
   end

   else
      raiserror('the table/view already has a clustered index', 16, 1)

   begin try
      exec ptf_internal.ExecStmt @stmt, @debug
   end try
      
   begin catch
      exec ptf_internal.HandleError @debug, null, @stmt
   end catch

   return 0
end try

begin catch
   exec ptf_internal.HandleError
   return 1
end catch
GO


CREATE PROCEDURE ptf.CreateIndexedViews
   @source_tab_id int,            -- object ID of source table
   @target_table  nvarchar(256),  -- name of table to create views for
   @part_num      int = NULL,     -- partition, where table will be switched in
   @compression   int = NULL,     -- if not 0, apply compression to index/table
   @debug         int = 0
AS
set nocount on

declare @src_name sysname, @src_schema sysname
declare @tab_name sysname, @tab_schema sysname, @id int

begin try
   if @source_tab_id is null
      raiserror('an object ID for the source table must be specified', 16, 1)

   if not exists (
      select * from sys.sql_expression_dependencies
       where referenced_id = @source_tab_id and
             referencing_id in (select object_id from sys.views) and
             referencing_id in (select object_id from sys.indexes where index_id = 1))
      return 0

   select @src_name = name, @src_schema = schema_name(schema_id)
     from sys.tables
    where object_id = @source_tab_id

   if @src_name is null
      raiserror('''%d'' is not a valid table ID', 16, 1, @source_tab_id)

   exec ptf_internal.GetObjId @target_table, 1, @id output,
        @name = @tab_name output, @schema = @tab_schema output

   if @id is null and isnull(@debug, 0) <> 2
      raiserror('the target table ''%s'' does not exist', 16, 1, @target_table)

   if @src_schema <> @tab_schema
      raiserror('both source and target table must belong to the same schema', 16, 1)


   declare @tokens table (
      id int not null, next int not null, alt int not null, val nvarchar(16) null)

   insert @tokens values ( 1,  2,  0, N'CREATE')
   insert @tokens values ( 2,  3,  0, N'VIEW')
   insert @tokens values ( 3,  4,  0, NULL)
   insert @tokens values ( 4,  5,  8, N'(')
   insert @tokens values ( 5,  6,  0, NULL)
   insert @tokens values ( 6,  5,  7, N',')
   insert @tokens values ( 7,  8,  0, N')')
   insert @tokens values ( 8,  9, 13, N'WITH')
   insert @tokens values ( 9, 12, 10, N'ENCRYPTION')
   insert @tokens values (10, 12, 11, N'SCHEMABINDING')
   insert @tokens values (11, 12,  0, N'VIEW_METADATA')
   insert @tokens values (12,  9, 13, N',')
   insert @tokens values (13,  0,  0, N'AS')

   declare @tabpat table (i int, pat nvarchar(256), prefix int, pos int)

   insert @tabpat values (1, N'.' +  @src_name + N' ',      1, 0)
   insert @tabpat values (2, N'.' +  @src_name + N',',      1, 0)
   insert @tabpat values (3, N'.' +  @src_name + N')',      1, 0)
   insert @tabpat values (4, N'.' +  @src_name + nchar(13), 1, 0)
   insert @tabpat values (5, N'.' +  @src_name + nchar(9),  1, 0)
   insert @tabpat values (6, N'.[' + @src_name + N']',      2, 0)
   insert @tabpat values (7, N'."' + @src_name + N'"',      2, 0)


   declare view_cur cursor local fast_forward for
      select v.object_id, v.name, m.definition
        from sys.views v
        join sys.sql_modules m on m.object_id = v.object_id 
        join sys.indexes i on i.object_id = v.object_id and i.index_id = 1
       where v.object_id in (
             select referencing_id from sys.sql_expression_dependencies
              where referenced_id = @source_tab_id)

   open view_cur

   declare @view_id int, @view_name sysname, @view_def nvarchar(max)
   declare @stmt nvarchar(max), @num int, @new_name sysname
   set @num = 0

   while 1 = 1
   begin
      fetch next from view_cur into @view_id, @view_name, @view_def
      if @@fetch_status <> 0 break

      select @num = @num + 1
      select @new_name = quotename(@tab_schema) + N'.' +
         quotename(N'v_' + @tab_name + N'_' + cast(@num as nvarchar))


      declare @p int, @n int, @x int, @an int, @tok nvarchar(16), @tn sysname
      select @p = 1, @n = 1

      while @n != 0
      begin
         while 1 = 1
         begin
            if substring(@view_def, @p, 2) = N'--'
               set @p = charindex(nchar(10), @view_def, @p + 2) + 1
            else if substring(@view_def, @p, 2) = N'/*'
               set @p = charindex(N'*/', @view_def, @p + 2) + 2
            else if unicode(substring(@view_def, @p, 1)) in (9, 10, 13, 32)
               set @p = @p + 1
            else
               break
         end
         while 1 = 1
         begin
            select @tok = val, @x = next, @an = alt from @tokens where id = @n
            if @tok is null
            begin
               while 1 = 1
               begin
                  if substring(@view_def, @p, 1) = N'['
                     set @p = charindex(N']', @view_def, @p + 1) + 1
                  else if substring(@view_def, @p, 1) = N'"'
                     set @p = charindex(N'"', @view_def, @p + 1) + 1
                  else
                     while unicode(substring(@view_def, @p, 1))
                        not in (9, 10, 13, 32, 40, 41, 44, 45, 46, 47)
                        set @p = @p + 1
                  if unicode(substring(@view_def, @p, 1)) <> 46 break
                  set @p = @p + 1
               end
            end
            else if upper(substring(@view_def, @p, len(@tok))) = @tok
               set @p = @p + len(@tok)
            else if @an <> 0
               begin set @n = @an continue end
            break
         end
         set @n = @x
      end

      select @view_def = substring(@view_def, @p, len(@view_def) - @p + 1)
      update @tabpat set pos = charindex(pat, @view_def)

      select @p = 1, @stmt = N'CREATE VIEW ' + @new_name +  N' WITH SCHEMABINDING AS'

      while exists (select * from @tabpat where pos > 0)  
      begin
         select @x = i, @n = pos + prefix,
                @tn = case when prefix = 1 then quotename(@tab_name) else @tab_name end
           from @tabpat
          where pos = (select min(pos) from @tabpat where pos > 0)
         select @stmt = @stmt + substring(@view_def, @p, @n - @p) + @tn
         select @p = @n + len(@src_name)
         update @tabpat set pos = charindex(pat, @view_def, @p + 1) where i = @x
      end

      select @stmt = @stmt + substring(@view_def, @p, len(@view_def) - @p + 1)

      exec ptf_internal.ExecStmt @stmt, @debug
      set @stmt = null


      declare @indid int
      declare icur cursor local fast_forward for
         select index_id from sys.indexes where object_id = @view_id order by index_id

      open icur
      fetch next from icur into @indid
      while @@fetch_status = 0
      begin
         exec ptf.CreateIndex @view_id, @indid, @new_name,
              @part_num = @part_num, @compression = @compression, @debug = @debug

         fetch next from icur into @indid
      end
      close icur
      deallocate icur
   end

   close view_cur 
   deallocate view_cur
   
   return 0
end try

begin catch
   exec ptf_internal.HandleError @debug, null, @stmt
   return 1
end catch
GO


CREATE PROCEDURE ptf.CreateIndexesAndConstraints
   @source_tab_id     int,                  -- object ID of source table
   @target_table      nvarchar(256) = NULL, -- name of the staging table to be prepared
   @part_num          int = NULL,           -- partition, where table will be switched in
   @ps_id             int = NULL,           -- ID of a partition scheme for filegroup mapping
   @part_scheme       sysname = NULL,       -- partition scheme to create index on
   @part_col_id       int = NULL,           -- column ID of partitioning column
   @exclude_check     sysname = NULL,       -- name of check constaint to exclude
   @compression       int = NULL,           -- if not null, apply compression to indexes
   @compress_all      bit = 0,              -- if 0, only compress the clustered index
   @create_indexes    tinyint = 3,          -- indicates the kind of indexes to be created
   @incremental_stats bit = 0,              -- 1, if index created with incremental stats
   @debug             int = 0
AS

declare @stage_tab nvarchar(256), @tab_id int, @ind_id int, @compr int
declare @sql nvarchar(max), @stmt nvarchar(max), @stmt2 nvarchar(max)

begin try
   if @source_tab_id is null
      raiserror('an object ID for the source table must be specified', 16, 1)

   if not exists (select * from sys.tables where object_id = @source_tab_id)
      raiserror('a table with ID ''%d'' does not exist', 16, 1, @source_tab_id)

   if @target_table is not null
      exec ptf_internal.GetObjId @target_table, 1, @tab_id output, @stage_tab output

   else if @part_scheme is null
      raiserror('the name of the target object must be specified', 16, 1)

   else if exists (select * from sys.indexes where object_id = @source_tab_id and type in (5, 6)) 
   begin
      set @sql = N' select @s =' +
          case when @@microsoftversion < 0x0D000000 then N' N'''''
               else N' case when isnull(compression_delay, 0) = 0 then N''''' +
                    N' else N'', COMPRESSION_DELAY = '' + cast(compression_delay as nvarchar) end' +
                    N' from sys.indexes where object_id = @tab_id and type in (5, 6)'
          end

      exec sp_executesql @sql, N'@tab_id int, @s nvarchar(max) output', @source_tab_id, @stmt2 output

      select @stmt  = N'DROP INDEX ' + quotename(i.name) + N' ON ' +
                      quotename(schema_name(t.schema_id)) + N'.' + quotename(t.name),
             @stmt2 = ptf_internal.GetCreateIndexStmt (i.object_id, i.index_id,
                         quotename(schema_name(t.schema_id)) + N'.' + quotename(t.name), null,
                         case when @@microsoftversion < 0x0C000000 then N''
                              else @stmt2 +
                                   case when @compression is null and p.data_compression = 4 or
                                             @compression = 4 and (i.type = 5 or @compress_all = 1)
                                        then N', DATA_COMPRESSION = COLUMNSTORE_ARCHIVE'
                                        else N''
                                   end
                         end, null, 0)
        from sys.indexes i join sys.tables t on t.object_id = i.object_id
        join sys.partitions p
          on p.object_id = i.object_id and p.index_id = i.index_id and p.partition_number = 1
       where i.object_id = @source_tab_id and i.type in (5, 6)
   end


   if @exclude_check is not null and @exclude_check not in (
      select name from sys.check_constraints
       where parent_object_id = @source_tab_id and (
             @part_col_id is null and parent_column_id <> 0 or
             @part_col_id is not null and parent_column_id = @part_col_id))
      raiserror('''%s'' is not a valid check constraint', 16, 1, @exclude_check)

   if @create_indexes <> 3 and (@create_indexes not in (1, 2) or @target_table is null)
      raiserror('''%d'' is not a valid create_indexes value', 16, 1, @create_indexes)

   select top 1 @ind_id = isnull(i.index_id, 0)
     from sys.tables t left join sys.indexes i on i.object_id = t.object_id and (
          i.type = 1 or i.type = 5 and @part_scheme is null or i.type = 7 and i.is_primary_key = 1)
    where t.object_id = @source_tab_id
    order by i.index_id
end try

begin catch
   exec ptf_internal.HandleError
   return 1
end catch

begin try
   exec ptf_internal.ExecStmt @stmt, @debug
   set @stmt = null

   exec ptf.CreateIndex @source_tab_id, @ind_id, @target_table,
        null, @part_num, @ps_id, @part_scheme, @part_col_id,
        @compression, @incremental_stats, @debug

   declare icur cursor local fast_forward for
    select index_id,
           case when index_id = 1 or @compress_all = 1 or @compression is null
                then @compression else 0
           end
      from sys.indexes
     where object_id = @source_tab_id and index_id <> @ind_id and (type = 5 or
           type in (2, 7) and @create_indexes > 1 or type = 6 and @create_indexes > 2)

   open icur
   fetch next from icur into @ind_id, @compr
   while @@fetch_status = 0
   begin
         exec ptf.CreateIndex @source_tab_id, @ind_id, @target_table,
              null, @part_num, @ps_id, @part_scheme, @part_col_id,
              @compr, @incremental_stats, @debug

      fetch next from icur into @ind_id, @compr
   end
   close icur
   deallocate icur

   if @target_table is null
   begin
      set @stmt = @stmt2
      exec ptf_internal.ExecStmt @stmt2, @debug
   end

   else
   begin
      if @part_scheme is null and @create_indexes > 1
      begin
	     set @compr = case when @compress_all = 1 or @compression is null then @compression else 0 end
         exec ptf.CreateIndexedViews @source_tab_id, @target_table, @part_num, @compr, @debug
      end

      declare @fk_obj int, @is_not_trusted bit, @nfr bit, @ref_tab nvarchar(256)

      declare fk_cur cursor local fast_forward for
         select fk.object_id, fk.is_not_trusted, fk.is_not_for_replication,
                quotename(schema_name(t.schema_id)) + N'.' + quotename(t.name)
           from sys.foreign_keys fk join sys.tables t
             on t.object_id = fk.referenced_object_id
          where fk.parent_object_id = @source_tab_id and
                fk.referenced_object_id <> fk.parent_object_id and
                fk.is_disabled = 0

      open fk_cur
      fetch next from fk_cur into @fk_obj, @is_not_trusted, @nfr, @ref_tab

      while @@fetch_status = 0
      begin
         declare @parc nvarchar(2048), @refc nvarchar(2048)

         select @parc = N'', @refc = N''
         select @parc = @parc + N', ' + quotename(pc.name),
                @refc = @refc + N', ' + quotename(rc.name)
           from sys.foreign_key_columns fkc
           join sys.columns pc
             on pc.object_id = fkc.parent_object_id and
                pc.column_id = fkc.parent_column_id
           join sys.columns rc
             on rc.object_id = fkc.referenced_object_id and
                rc.column_id = fkc.referenced_column_id
          where fkc.constraint_object_id = @fk_obj
          order by fkc.constraint_column_id

         select @stmt = N'ALTER TABLE ' + @stage_tab +
                case when @is_not_trusted = 0 then N'' else N' WITH NOCHECK' end +
                N' ADD FOREIGN KEY' + stuff(@parc, 1, 2, N' (') +
                N') REFERENCES ' + @ref_tab + stuff(@refc, 1, 2, N' (') + N')' +
                case when @nfr = 0 then N'' else N' NOT FOR REPLICATION' end

         exec ptf_internal.ExecStmt @stmt, @debug

         fetch next from fk_cur into @fk_obj, @is_not_trusted, @nfr, @ref_tab
      end

      close fk_cur
      deallocate fk_cur


      select @stmt = N''
      select @stmt = @stmt + N'ALTER TABLE ' + @stage_tab +
             case when is_not_trusted = 0 then N'' else N' WITH NOCHECK' end +
             N' ADD CHECK ' +
             case when is_not_for_replication = 0 then N'' else N'NOT FOR REPLICATION ' end +
             definition + N';' + nchar(13) + nchar(10)
        from sys.check_constraints
       where parent_object_id = @source_tab_id and
             name <> isnull(@exclude_check, N'') and is_disabled = 0

      exec ptf_internal.ExecStmt @stmt, @debug
   end

   return 0
end try

begin catch
   exec ptf_internal.HandleError @debug, null, @stmt
   return 1
end catch
GO


CREATE PROCEDURE ptf.SwitchPartition2
   @part_tab_id   int,                  -- object ID of table where switch takes place
   @archive_table nvarchar(256) = NULL, -- name of archive table to be created
   @out_part      int = NULL,           -- partition that is being switched out
   @staging_table nvarchar(256) = NULL, -- name of staging table to be switched in
   @in_part       int = NULL,           -- partition, where table is being switched in
   @split_value   sql_variant = NULL,   -- boundary value for new partition
   @new_filegroup sysname = NULL,       -- filegroup the new partition is mapped to
   @check_name    sysname = NULL,       -- name of check constaint for upper bound
   @upper_bound   sql_variant = NULL,   -- new upper boound for check constraint
   @make_readonly bit = 0,              -- make the input filegroup(s) read only
   @keep_indexes  tinyint = 1,			-- if 0, do not keep any index on archive table
   @no_merge      bit = 0,              -- if 1, do not merge the output partition
   @no_drop       bit = 0,              -- if 1, do not drop staging table after switch
   @wait_option   nvarchar(80) = NULL,  -- low prio wait option
   @temp_mask     tinyint = 2,          -- temporal table type mask
   @archive_mode  tinyint = NULL,       -- what to do with data in switched out partition
   @debug         int = 0
AS
set nocount on
set xact_abort off

declare @part_table nvarchar(256), @idx_id int, @ps_id int, @pf_id int
declare @pf_name sysname, @num_part int, @is_right bit,  @pcol_id int
declare @id int, @archive nvarchar(256), @merge_value sql_variant
declare @stage_tab_id int, @is_temp tinyint, @cnt int, @swap_mode bit
declare @val nvarchar(max), @split_part int, @new_fg_id int
declare @check_exists int, @max_part int, @ub sql_variant, @constr_name sysname
declare @stmt nvarchar(max), @stmt2 nvarchar(max), @svpt nvarchar(32)

declare @split_info table (ps_name sysname, next_fg int, new_fg int)

set @swap_mode = 0

begin try

   if @part_tab_id is null
      raiserror('an object ID for the target table must be specified', 16, 1)

   select @part_table = quotename(schema_name(t.schema_id)) + N'.' + quotename(t.name),
          @idx_id     = i.index_id,
          @ps_id      = i.data_space_id,
          @pf_id      = pf.function_id,
          @pf_name    = pf.name,
          @num_part   = pf.fanout,
          @is_right   = pf.boundary_value_on_right,
          @pcol_id    = ic.column_id
     from sys.tables t
     left join sys.indexes i on i.object_id = t.object_id and i.index_id in (0, 1)
     left join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
     left join sys.partition_functions pf on pf.function_id = ps.function_id
     left join sys.index_columns ic                           
       on ic.object_id = i.object_id and ic.index_id = i.index_id and ic.partition_ordinal = 1
    where t.object_id = @part_tab_id

   if @part_table is null
      raiserror('a table with ID ''%d'' does not exist', 16, 1, @part_tab_id)

   else if isnull(@ps_id, 0) = 0 
      raiserror('the source table must not be memory optimized', 16, 1)


   if @archive_mode is null
      set @archive_mode = case when @archive_table is null then 0 else 1 end
   else if @archive_mode = 0
      set @archive_table = null
   else if @archive_mode = 2
   begin
      if @@microsoftversion < 0x0D000000
         set @is_temp = 0
      else
         exec sp_executesql N'select @tt = temporal_type from sys.tables where object_id = @tid',
              N'@tid int, @tt tinyint output', @part_tab_id, @is_temp output

      if (@pf_id is null or @@microsoftversion >= 0x0D000000) and @is_temp = 0 and
         @part_tab_id not in (select referenced_object_id from sys.foreign_keys) and
         @part_tab_id not in (select referenced_id from sys.sql_expression_dependencies
                               where referencing_id in (select object_id from sys.views) and
                                     referencing_id in (select object_id from sys.indexes))
         select @archive_table = null

      else if @archive_table is null
         select @archive_table = quotename(schema_name(schema_id)) + N'.' +
                quotename(left(name, 91) + N'_' + cast(newid() as nvarchar(36)))
           from sys.tables where object_id = @part_tab_id

      select @keep_indexes = 1, @temp_mask = 2
   end
   else if @archive_mode <> 1
      raiserror('%d is not a valid value for the archive mode parameter', 16, 1, @archive_mode)
   else if @archive_table is null
      raiserror('an archive table name must be specified', 16, 1)

   if @archive_mode = 1
   begin
      if @keep_indexes not between 0 and 3 or @keep_indexes = 0 and
         @part_tab_id in (select object_id from sys.columns where is_filestream = 1)
         raiserror('%d is not a valid value for the keep indexes parameter', 16, 1, @keep_indexes)

      if isnull(@temp_mask & ~3, 1) <> 0 
         raiserror ('%d is not a valid value for the temp mask parameter', 16, 1, @temp_mask)
   end

   if @archive_mode = 0
      set @out_part = null
   else if @pf_id is null
      select @out_part = 1, @no_merge = 1
   else if @out_part is null
      raiserror('an output partition number must be specified', 16,1)
   else if @out_part < 1 or @out_part > @num_part
      raiserror('''%d'' is not a valid output partition number', 16, 1, @out_part)

   if @archive_table is not null
   begin
      exec ptf_internal.GetObjId @archive_table, 0, @id output, @archive output

      if @id is not null and isnull(@debug, 0) <> 2
         raiserror('the archive table ''%s'' already exists', 16, 1, @archive)

      if isnull(@debug, 0) <> 2 and @part_tab_id in (
         select referenced_object_id from sys.foreign_keys where is_disabled = 0)
         raiserror('the table must not be referenced by a foreign key constraint', 16, 1)
   end

   if @archive_mode <> 0 and isnull(@no_merge, 0) = 0
   begin
      if isnull(@debug, 0) <> 2 and exists (
         select *
           from sys.partitions p join sys.indexes i
             on i.object_id = p.object_id and i.index_id = p.index_id and i.data_space_id in (
                select data_space_id from sys.partition_schemes where function_id = @pf_id)
          where p.rows <> 0 and p.partition_number = @out_part and p.object_id in (
                select object_id from sys.tables where object_id <> @part_tab_id))
         raiserror('partition %d cannot be merged due to not being empty', 16, 1, @out_part)

      if @out_part - @is_right not between 1 and @num_part - 1
         raiserror('the first/last partition cannot be merged', 16, 1)

      select @merge_value = value
        from sys.partition_range_values
       where function_id = @pf_id and parameter_id = 1 and boundary_id = @out_part - @is_right
   end


   if @archive_mode <> 0 or @staging_table is not null
   begin
      if @pf_id is not null and exists (
         select * from sys.indexes i
          where is_disabled = 0 and is_hypothetical = 0 and (
                object_id = @part_tab_id and not exists (
                   select * from sys.index_columns
                    where object_id = i.object_id and index_id = i.index_id and
                          partition_ordinal = 1 and column_id = @pcol_id) or (
                object_id = @part_tab_id or object_id in (
                   select referencing_id from sys.sql_expression_dependencies
                    where referenced_id = @part_tab_id and
                          referencing_id in (select object_id from sys.views))) and
                data_space_id not in (
                   select data_space_id from sys.partition_schemes where function_id = @pf_id)))
         raiserror('the table has indexes that are not partition aligned', 16, 1)

      if @part_tab_id in (select object_id from sys.fulltext_indexes)
         raiserror('the table must not have a fulltext index', 16, 1)
      else if @part_tab_id in (select object_id from sys.change_tracking_tables)
         raiserror('change tracking must not be enabled for the table', 16, 1)
   end

   if SERVERPROPERTY('EngineEdition') = 5
      set @make_readonly = 0

   if @staging_table is null and isnull(@make_readonly, 0) = 0
      set @in_part = null
   else if @pf_id is null
      set @in_part = 1
   else if @in_part is null
      raiserror('an input partition number must be specified', 16, 1)
   else if @in_part < 1 or @in_part > @num_part
      raiserror('''%d'' is not a valid input partition number', 16, 1, @in_part)

   if @staging_table is not null
   begin
      exec ptf_internal.GetObjId @staging_table, 1, @stage_tab_id output, @staging_table output

      if @stage_tab_id is null
	  begin
         if isnull(@debug, 0) <> 2
            raiserror('the staging table ''%s'' does not exist', 16, 1, @staging_table)
         else if @staging_table = @archive
            raiserror('the archive and staging table names must be different', 16, 1)
      end
      else
      begin
         if @stage_tab_id in (select object_id from sys.indexes where data_space_id = 0)
            raiserror('the staging table must not be memory optimized', 16, 1)

         if @stage_tab_id in (select object_id from sys.indexes where data_space_id in (
                                     select data_space_id from sys.partition_schemes))
            raiserror('the staging table must not be partitioned', 16, 1)

         if @@microsoftversion >= 0x0D000000
         begin
            exec sp_executesql
                 N'select @tt = temporal_type from sys.tables where object_id = @oid',
                 N'@oid int, @tt tinyint output', @stage_tab_id, @is_temp output

            if @is_temp <> 0
               raiserror('the staging table must not be a system versioned temporal table', 16, 1)
         end

         set @stmt2 =
             N'select @num = count(*)' +
             N'  from (select row_number() over (order by c.column_id) as column_id,' +
             N'               c.name, c.system_type_id, c.max_length, c.precision, c.scale,' +
             N'               c.collation_name, c.is_nullable, c.is_ansi_padded, c.is_rowguidcol,' +
             N'               c.is_filestream, c.is_computed, cc.definition, cc.is_persisted' +
         case when @@microsoftversion < 0x0D000000 then N'' else N', c.generated_always_type' end +
             N'          from sys.columns c left join sys.computed_columns cc' +
             N'            on cc.object_id = c.object_id and cc.column_id = c.column_id' +
             N'         where c.object_id = @t1) as c1' +
             N'  full outer join' +
             N'       (select row_number() over (order by c.column_id) as column_id,' +
             N'               c.name, c.system_type_id, c.max_length, c.precision, c.scale,' +
             N'               c.collation_name, c.is_nullable, c.is_ansi_padded, c.is_rowguidcol,' +
             N'               c.is_filestream, c.is_computed, cc.definition, cc.is_persisted' +
         case when @@microsoftversion < 0x0D000000 then N'' else N', c.generated_always_type' end +
             N'          from sys.columns c left join sys.computed_columns cc' +
             N'            on cc.object_id = c.object_id and cc.column_id = c.column_id' +
             N'         where c.object_id = @t2) as c2' +
             N'    on c1.column_id = c2.column_id and c1.name = c2.name and' +
             N'       c1.system_type_id = c2.system_type_id and' +
             N'       c1.max_length = c2.max_length and' +
             N'       c1.precision = c2.precision and c1.scale = c2.scale and' +
             N'       (c1.collation_name is null and c2.collation_name is null or' +
             N'       c1.collation_name = c2.collation_name) and' +
             N'       c1.is_nullable = c2.is_nullable and' +
             N'       c1.is_ansi_padded = c2.is_ansi_padded and' +
	         N'       c1.is_rowguidcol = c2.is_rowguidcol and' +
	         N'       c1.is_filestream = c2.is_filestream and' +
             N'       c1.is_computed = c2.is_computed and (c1.is_computed = 0 or' +
             N'       c1.definition = c2.definition and c1.is_persisted = c2.is_persisted)' +
         case when @@microsoftversion < 0x0D000000 or @debug = 2 then N''
              else N' and (c1.generated_always_type = 0 or' +
                   N'      c1.generated_always_type = c2.generated_always_type)'
         end +
             N' where c1.column_id is null or c2.column_id is null'

         exec sp_executesql @stmt2, N'@t1 int, @t2 int, @num int output',
              @part_tab_id, @stage_tab_id, @cnt output

         if @cnt <> 0
            raiserror('the staging table structurally differs from the target table', 16, 1)

         if not exists (
            select * from sys.tables
             where object_id = @stage_tab_id and
                   (lob_data_space_id = 0 or lob_data_space_id = (
                      select isnull(dds.data_space_id, t.lob_data_space_id)
                        from sys.tables t left join sys.indexes i
                          on i.object_id = t.object_id and i.index_id in (0, 1)
                        left join sys.destination_data_spaces dds
                          on dds.partition_scheme_id = i.data_space_id and
                             dds.destination_id = @in_part
                       where t.object_id = @part_tab_id)) and
                   (filestream_data_space_id is null or filestream_data_space_id = ( 
                      select isnull(dds.data_space_id, t.filestream_data_space_id)
                        from sys.tables t left join sys.destination_data_spaces dds
                          on dds.partition_scheme_id = t.filestream_data_space_id and
                             dds.destination_id = @in_part
                       where t.object_id = @part_tab_id)))
            raiserror('LOB or filestream data of staging table are not in correct filegroup', 16, 1)
      end

      if @archive_mode in (1, 2) and @out_part = @in_part
      begin
         if isnull(@no_merge, 0) = 0
            raiserror('a partition cannot be merged while swapping this partition', 16, 1)

         set @swap_mode = 1
      end

      else if isnull(@debug, 0) <> 2 and exists (
         select * from sys.partitions
          where object_id = @part_tab_id and partition_number = @in_part and rows <> 0)
         raiserror('the input partition must be empty', 16, 1)

      if @pf_id is null and exists (
         select * from sys.indexes where object_id = @part_tab_id and data_space_id in (
                select data_space_id from sys.filegroups where is_read_only = 1)) or
         @pf_id is not null and exists (
         select * from sys.destination_data_spaces
          where destination_id = @in_part and partition_scheme_id in (
                select data_space_id from sys.partition_schemes where function_id = @pf_id) and
                data_space_id in (select data_space_id from sys.filegroups where is_read_only = 1))
         raiserror('the input partition is mapped to a read only filegroup', 16, 1)

      if @pf_id is not null and @in_part = 1 and @is_right = 1 and exists (
         select * from sys.partition_range_values
          where function_id = @pf_id and parameter_id = 1 and boundary_id = 1 and value is null)
         raiserror('no data can be switched into the first partition', 16, 1)

      if isnull(@debug, 0) <> 2 and @part_tab_id in (
         select referenced_object_id from sys.foreign_keys
          where referenced_object_id = parent_object_id and is_disabled = 0)
         raiserror('the table has a self-referencing foreign key constraint', 16, 1)
   end


   if @split_value is not null
   begin
      if @pf_id is null
         raiserror('a non-partitioned table cannot be split', 16, 1)

      if @swap_mode = 1
         raiserror('a new partition cannot be created while swapping a partition', 16, 1)
         
      exec ptf_internal.CastVariant @pf_id, 1, @split_value output
      set @val = ptf_internal.VariantToString(@split_value)

      if exists (select * from sys.partition_range_values
         where function_id = @pf_id and parameter_id = 1 and value = @split_value)
         raiserror('the boundary value %s already exists in the partition function', 16, 1, @val)

      select @split_part = isnull(min(boundary_id), @num_part)
        from sys.partition_range_values
       where function_id = @pf_id and parameter_id = 1 and value > @split_value
         
      if @staging_table is not null and @is_right = 0 and @split_part = @in_part
         raiserror('the input partition cannot be split', 16, 1)

      if isnull(@debug, 0) <> 2 and exists (
         select *
           from sys.partition_schemes ps join sys.indexes i
             on i.data_space_id = ps.data_space_id and i.index_id in (0, 1) and
                (i.object_id <> @part_tab_id or @archive_mode = 0 or @split_part <> @out_part)
           join sys.tables t on t.object_id = i.object_id
           join sys.partitions p on p.partition_number = @split_part and
                p.object_id = i.object_id and p.index_id = i.index_id
          where ps.function_id = @pf_id and p.rows <> 0)
         raiserror('partition %d cannot be split because it is not empty', 16, 1, @split_part)

      if @new_filegroup is null
         set @new_fg_id = null
      else if lower(@new_filegroup) = N'default'
         select @new_fg_id = data_space_id from sys.filegroups
          where is_default = 1 and type = 'FG'
      else
      begin
         select @new_fg_id = data_space_id from sys.filegroups
          where name = @new_filegroup and type = 'FG'

         if @new_fg_id is null
            raiserror('''%s'' is not a valid filegroup name', 16, 1, @new_filegroup)
      end

      insert into @split_info (ps_name, next_fg, new_fg)
         select ps.name, dds_next.data_space_id,
                case when @new_fg_id is null
                     then case when dds_out.data_space_id is null
                               then dds_cur.data_space_id else dds_out.data_space_id
                          end
                     else case when ps.data_space_id = @ps_id
                               then @new_fg_id else dds_next.data_space_id
                          end
                end
           from sys.partition_schemes ps
           left join sys.destination_data_spaces dds_next
             on dds_next.partition_scheme_id = ps.data_space_id and
                dds_next.destination_id = @num_part + 1
           left join sys.destination_data_spaces dds_out
             on dds_out.partition_scheme_id = ps.data_space_id and
                dds_out.destination_id = @out_part and isnull(@no_merge, 0) = 0
           left join sys.destination_data_spaces dds_cur
             on dds_cur.partition_scheme_id = ps.data_space_id and dds_cur.destination_id =
                @split_part - case when @is_right = 0 and @archive_mode = 0 then 1 else 0 end
          where ps.function_id = @pf_id

      if exists (select * from @split_info where new_fg is null) 
      begin
         declare @ps_name sysname
         select top 1 @ps_name = ps_name from @split_info where new_fg is null
         raiserror('a NEXT USED designation is missing for partition scheme ''%s''', 16, 1, @ps_name)
      end
   end


   if @check_name is not null
   begin
      if @pf_id is null
         raiserror('for a non-partitioned table a check constraint cannot be specified', 16, 1)

      select @check_exists = parent_column_id
        from sys.check_constraints
       where name = @check_name and parent_object_id = @part_tab_id

      if @check_exists is not null and @check_exists <> @pcol_id or
         @check_exists is null and (len(@check_name) = 0 or exists (
         select * from sys.objects where name = @check_name and schema_id =
            (select schema_id from sys.tables where object_id = @part_tab_id)))
         raiserror('%s is not a valid check constraint name', 16, 1, @check_name)

      if @upper_bound is null
      begin
         select @max_part = boundary_id from sys.partition_range_values
          where function_id = @pf_id and parameter_id = 1 and value is null

         if @is_right = 1 or @max_part is null
            raiserror('a value for the upper bound must be specified', 16, 1)
      end
      else
      begin
         exec ptf_internal.CastVariant @pf_id, 1, @upper_bound output
         set @val = ptf_internal.VariantToString(@upper_bound)

         select @max_part = isnull(min(boundary_id), @num_part)
           from sys.partition_range_values
          where function_id = @pf_id and parameter_id = 1 and value >= @upper_bound
      end

      if @staging_table is not null and @in_part > @max_part
         raiserror('the upper bound %s conflicts with the input partition boundaries', 16, 1, @val)
   end


   if @staging_table is not null
   begin
      create table #idxtab (
         src bit, id int, ref int, obj_name nvarchar(256), idx_type tinyint,
         idx_name nvarchar(128), is_unique bit, is_pk bit, fg_id int, part_compr int,
         idx_keys varchar(256), idx_filter nvarchar(max), view_cols varchar(256)
      )

      begin try

         insert into #idxtab
            exec ptf_internal.GetIndexInfo @part_tab_id, @in_part, 1

         if exists (select * from #idxtab
                     group by idx_type, is_unique, is_pk, idx_keys, idx_filter, view_cols
                    having count(*) > 1)
            raiserror('the source table has indexes with an identical signature', 16, 1)

         if @stage_tab_id is not null
         begin
            insert into #idxtab
               exec ptf_internal.GetIndexInfo @stage_tab_id

            update #idxtab set ref = i2.id
              from #idxtab inner join #idxtab i2
                on #idxtab.idx_type = i2.idx_type and #idxtab.is_unique = i2.is_unique
               and #idxtab.is_pk = i2.is_pk and #idxtab.idx_keys = i2.idx_keys
               and #idxtab.idx_filter = i2.idx_filter and #idxtab.view_cols = i2.view_cols
               and i2.src = 0
             where #idxtab.src = 1

            if isnull(@debug, 0) <> 2 and exists (
               select * from #idxtab where src = 1 and ref is null)
               raiserror('some indexes are missing on the staging table', 16, 1)

            if exists (
               select * from #idxtab i1 join #idxtab i2
                   on i2.src = 0 and i2.id = i1.ref and i2.fg_id <> i1.fg_id
                where i1.src = 1)
               raiserror('the staging table or one of its indexes is in a wrong filegroup', 16, 1)
         end

         if @split_value is not null and @split_part = @in_part
            set @ub = @split_value

         if @check_name is not null and @max_part = @in_part and
            (@ub is null or @upper_bound is null or @upper_bound < @ub)
            set @ub = @upper_bound

         if @pf_id is not null
         begin
            select @stmt = N'ALTER TABLE ' + @staging_table + N' ADD CHECK (' +
                           ptf_internal.GetPartitionCheck (
                               @pf_id, c2.name, c2.is_nullable, @in_part, @ub) + N')'
              from (select column_id, row_number() over(order by column_id) as pos
                      from sys.columns where object_id = @part_tab_id) as c1
              join (select name, is_nullable, row_number() over(order by column_id) as pos
                      from sys.columns where object_id = @stage_tab_id) as c2 on c2.pos = c1.pos
             where c1.column_id = @pcol_id

            begin try
               exec ptf_internal.ExecStmt @stmt, @debug
            end try
            begin catch
               exec ptf_internal.HandleError @debug, null, @stmt
            end catch
         end
      end try

      begin catch
         drop table #idxtab
         exec ptf_internal.HandleError
      end catch
   end

end try

begin catch
   exec ptf_internal.HandleError
   return 1
end catch


begin try

   if @archive_mode <> 0
      exec ptf_internal.ModifyFileGroups @part_tab_id, @pf_id, @out_part, 1, 1, @debug

   begin try

      if @@trancount = 0
         begin transaction
      else
      begin
         set @svpt = N'SwitchPartition_svpt'
         save transaction @svpt
      end


      if @archive_table is not null
      begin
         exec ptf.CreateTableClone @part_tab_id, @archive_table,
              @part_num = @out_part, @switch_source = @no_merge, @debug = @debug

         if @keep_indexes >= 2
            exec ptf.CreateIndexesAndConstraints @part_tab_id, @archive_table,
                 @part_num = @out_part, @create_indexes = @keep_indexes, @debug = @debug
         else
         begin
            if @keep_indexes = 0 and @idx_id = 1
               set @constr_name = N'PK_' + cast(newid() as nvarchar(36))

            exec ptf.CreateIndex @part_tab_id, @idx_id, @archive_table,
                 @constraint_name = @constr_name, @part_num = @out_part, @debug = @debug
         end

         exec ptf_internal.HandleVersioning
              @part_tab_id, @temp_mask, @stmt output, @stmt2 output

         set @stmt = @stmt + N'ALTER TABLE ' + @part_table + N' SWITCH' +
                     case when @pf_id is null then N''
                          else N' PARTITION ' + cast(@out_part as nvarchar)
                     end + N' TO ' + @archive +
                     case when isnull(len(@wait_option), 0) = 0 then N''
                          else N' WITH (' + @wait_option + N')'
                     end + N';' + nchar(13) + nchar(10) + @stmt2

         exec ptf_internal.ExecStmt @stmt, @debug

         if @keep_indexes = 0 and @idx_id = 1
         begin
            select @stmt =
                   case when is_primary_key = 0 and is_unique_constraint = 0
                        then N'DROP INDEX ' + quotename(name) + N' ON ' + @archive
                        else N'ALTER TABLE ' + @archive +
                             N' DROP CONSTRAINT ' + quotename(@constr_name)
                   end
              from sys.indexes
             where object_id = @part_tab_id and index_id = 1

            exec ptf_internal.ExecStmt @stmt, @debug
         end 
      end

      if @archive_mode = 2
      begin
         set @stmt = case when @archive_table is not null
                          then N'DROP TABLE ' + @archive
                          else N'TRUNCATE TABLE ' + @part_table +
                               case when @pf_id is null then N''
                                    else N' WITH (PARTITIONS (' +
                                         cast(@out_part as nvarchar) + N'))'
                               end
                     end

         exec ptf_internal.ExecStmt @stmt, @debug
      end


      if @split_value is not null
      begin
         select @stmt = N''
         select @stmt = @stmt + N';' + nchar(13) + nchar(10) + N'ALTER PARTITION SCHEME ' +
                quotename(ps_name) + N' NEXT USED ' + quotename(filegroup_name(new_fg))
           from @split_info
          where next_fg is null or next_fg <> new_fg

         if len(@stmt) > 0 set @stmt = right(@stmt, len(@stmt) - 3)
         exec ptf_internal.ExecStmt @stmt, @debug

         set @stmt = N'ALTER PARTITION FUNCTION ' +
                     quotename(@pf_name) + N'() SPLIT RANGE (' +
                     ptf_internal.VariantToString(@split_value) + N')'

         exec ptf_internal.ExecStmt @stmt, @debug

         if @out_part is not null and @split_part <= @out_part - @is_right
            set @out_part = @out_part + 1

         if @in_part is not null and @split_part < @in_part
            set @in_part = @in_part + 1
      end


      if @check_name is not null
      begin
         if @check_exists is not null
         begin
            set @constr_name = left(@check_name, 91) + N'_' + cast(newid() as nvarchar(36))

            select @stmt = N'EXEC sp_rename ''' + schema_name(schema_id) + N'.' +
                   @check_name + N''', ''' + @constr_name + N''', ''OBJECT'''
              from sys.tables where object_id = @part_tab_id

            exec ptf_internal.ExecStmt @stmt, @debug
         end

         set @stmt = N'ALTER TABLE ' + @part_table +
                     N' ADD CONSTRAINT ' + quotename(@check_name) +
                     N' CHECK (' + quotename(col_name(@part_tab_id, @pcol_id)) +
                     case when @upper_bound is null then N' IS NULL'
                          else case when @is_right = 0 then N'<=' else N'<' end +
                               ptf_internal.VariantToString(@upper_bound)
                     end + N')'

         exec ptf_internal.ExecStmt @stmt, @debug

         if @check_exists is not null
         begin
            set @stmt = N'ALTER TABLE ' + @part_table +
                        N' DROP CONSTRAINT ' + quotename(@constr_name)

            exec ptf_internal.ExecStmt @stmt, @debug
         end
      end


      if @staging_table is not null
      begin
         declare rebuild_cur cursor local fast_forward for
            select N'ALTER ' +
                   case when i1.idx_type = 0 then N'TABLE '
                        else N'INDEX ' + quotename(i1.idx_name) + N' ON '
                   end + i1.obj_name + N' REBUILD' +
                   case when @pf_id is null then N''
                        else N' PARTITION = ' + cast(@in_part as nvarchar)
                   end + N' WITH (DATA_COMPRESSION = ' +
                   case i2.part_compr
                        when 0 then N'NONE'
                        when 1 then N'ROW'
                        when 2 then N'PAGE'
                        when 3 then N'COLUMNSTORE'
                        when 4 then N'COLUMNSTORE_ARCHIVE'
                   end + N');'
              from #idxtab i1 join #idxtab i2
                on i2.src = 0 and i2.id = i1.ref and i2.part_compr <> i1.part_compr
             where i1.src = 1
             order by i1.idx_type

         open rebuild_cur
         fetch next from rebuild_cur into @stmt
         while @@fetch_status = 0
         begin
            exec ptf_internal.ExecStmt @stmt, @debug
            fetch next from rebuild_cur into @stmt
         end
         close rebuild_cur
         deallocate rebuild_cur

         set @stmt = N'ALTER TABLE ' + @staging_table +
                     N' SWITCH TO ' + @part_table +
                     case when @pf_id is null then N''
                          else N' PARTITION ' + cast(@in_part as nvarchar)
                     end +
                     case when isnull(len(@wait_option), 0) = 0 then N''
                          else N' WITH (' + @wait_option + N')'
                     end

         exec ptf_internal.ExecStmt @stmt, @debug


         if isnull(@no_drop, 0) = 0
         begin
            if @stage_tab_id is not null
            begin
               select @stmt = N''  
               select @stmt = @stmt + 'DROP VIEW ' + 
                      quotename(schema_name(schema_id)) + N'.' +
                      quotename(name) + N';' + nchar(13) + nchar(10)
                 from sys.views
                where object_id in (
                      select referencing_id from sys.sql_expression_dependencies
                       where is_schema_bound_reference = 1 and referenced_id = @stage_tab_id)
                group by schema_id, name

               exec ptf_internal.ExecStmt @stmt, @debug
            end

            set @stmt = N'DROP TABLE ' + @staging_table
            exec ptf_internal.ExecStmt @stmt, @debug
         end
      end


      if @no_merge = 1
         set @out_part = null
      else if @archive_mode <> 0
      begin
         set @stmt = N'ALTER PARTITION FUNCTION ' + quotename(@pf_name) +
                     N'() MERGE RANGE (' + ptf_internal.VariantToString(@merge_value) + N')'

         exec ptf_internal.ExecStmt @stmt, @debug

         if @in_part is not null and @in_part > @out_part
            set @in_part = @in_part - 1

         set @out_part = @out_part - @is_right
      end

      if @svpt is null
         commit transaction
   end try

   begin catch
      if @svpt is null
         rollback transaction
      else if xact_state() = 1
         rollback transaction @svpt

      exec ptf_internal.HandleError @debug, null, @stmt
   end catch

   if @make_readonly = 1
   begin
      exec ptf_internal.ModifyFileGroups @part_tab_id, @pf_id, @in_part, 1, 0, @debug

      if @staging_table is null
         set @in_part = null
   end

   exec ptf_internal.UpdateStatistics @part_table, 0, 1, @out_part, @in_part, @debug

   if @staging_table is not null
      drop table #idxtab

   return 0
end try

begin catch 
   if @staging_table is not null
      drop table #idxtab
   exec ptf_internal.HandleError
   return 1
end catch
GO


CREATE PROCEDURE ptf.CreateStagingTable
   @source_table  nvarchar(256),         -- name of source table
   @staging_table nvarchar(256),         -- name of table to be created
   @fg_name       sysname = NULL OUTPUT, -- filegroup in which table will be created
   @is_clustered  bit = NULL,            -- if 1, additionally create a clustered index
   @debug         int = 0
AS

declare @source_tab_id int, @ind_id int, @ind_type int, @num_part int, @in_part int

begin try
   exec ptf_internal.Trace N'CreateStagingTable', 0, @debug, 4,
      @source_table, @staging_table, @fg_name, @is_clustered

   exec ptf_internal.GetObjId @source_table, 1, @source_tab_id output

   if @source_tab_id is null
      raiserror('the source table ''%s'' does not exist', 16, 1, @source_table)

   if has_perms_by_name(null, 'database', 'create table') = 0
      raiserror('you do not have permission to create a table in the database', 16, 1)

   select top 1
          @ind_id   = i.index_id,
          @ind_type = isnull(i.type, 0),
          @num_part = pf.fanout,
          @in_part  = pf.fanout + pf.boundary_value_on_right - 1
     from sys.tables t
     left join sys.indexes i on i.object_id = t.object_id and
          (i.type in (0, 1, 5) or i.type = 7 and i.is_primary_key = 1)
     left join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
     left join sys.partition_functions pf on pf.function_id = ps.function_id
    where t.object_id = @source_tab_id
    order by i.index_id

   if @num_part = 1
      raiserror('the source table does not have a next input partition', 16, 1)
   else if @fg_name is not null and @is_clustered = 1
      raiserror('a filegroup name cannot be specified if table is going to be clustered', 16, 1)
   else if @is_clustered = 0 or @ind_type <> 5 or @fg_name is not null
      set @ind_id = null

   exec ptf.CreateTableClone
        @source_tab_id, @staging_table, @fg_name output, @in_part, null, 1, @debug = @debug

   begin try
      if isnull(@ind_id, 0) > 0
         exec ptf.CreateIndex @source_tab_id, @ind_id, @staging_table, null, @in_part, @debug = @debug
   end try

   begin catch
      declare @stmt nvarchar(512)
      exec ptf_internal.GetObjId @staging_table, 1, @quote_name = @staging_table output
      set @stmt = N'DROP TABLE ' + @staging_table
      exec sp_executesql @stmt
      exec ptf_internal.HandleError
   end catch

   exec ptf_internal.Trace N'CreateStagingTable', 1, @debug
   return 0
end try

begin catch
   exec ptf_internal.HandleError @debug, N'CreateStagingTable'
   return 1
end catch
GO


CREATE PROCEDURE ptf.PrepareStagingTable
   @part_table    nvarchar(256),       -- name of source table
   @staging_table nvarchar(256),       -- name of staging table
   @check_name    sysname = NULL,      -- name of check constaint to exclude
   @compression   nvarchar(20) = NULL, -- compression mode specifier
   @compress_all  bit = 0,             -- if 1, also compress nonclustered indexes
   @debug         int = 0
AS
declare @part_table_id int, @in_part int, @compr int, @svpt nvarchar(32)

begin try
   exec ptf_internal.Trace N'PrepareStagingTable', 0, @debug, 5,
      @part_table, @staging_table, @check_name, @compression, @compress_all

   exec ptf_internal.GetObjId @part_table, 1, @part_table_id output

   if @part_table_id is null
      raiserror('the source table ''%s'' does not exist', 16, 1, @part_table)

   select @in_part = case when pf.function_id is null then null
                          when pf.fanout = 1 then 0
                          else pf.fanout + pf.boundary_value_on_right - 1
                     end
     from sys.indexes i
     left join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
     left join sys.partition_functions pf on pf.function_id = ps.function_id
    where i.object_id = @part_table_id and i.index_id in (0, 1)

   if @in_part = 0
      raiserror('the source table does not have a next input partition', 16, 1)

   set @compr =
       case when @compression is null then 0
	        else case lower(@compression)
                      when N'none' then 0 when N'row' then 1 when N'page' then 2
                      when N'columnstore' then 3 when N'columnstore_archive' then 4
                      else -1
                 end
       end

   if @compr = -1
      raiserror('''%s'' is not a valid compression value', 16, 1, @compression)

   else if @compr in (1, 2) and exists (
      select * from sys.columns where object_id = @part_table_id and is_sparse = 1)
      raiserror('compression is not possible due to sparse columns', 16, 1)

   if @@trancount = 0
      begin transaction
   else
   begin
      set @svpt = N'PrepStaging_svpt'
      save transaction @svpt
   end

   begin try
      exec ptf.CreateIndexesAndConstraints @part_table_id, @staging_table,
           @part_num = @in_part, @exclude_check = @check_name,
           @compression = @compr, @compress_all = @compress_all, @debug = @debug

      if @svpt is null
         commit transaction
   end try

   begin catch
      if @svpt is null
         rollback transaction
      else if xact_state() = 1
         rollback transaction @svpt

      exec ptf_internal.HandleError
   end catch

   exec ptf_internal.Trace N'PrepareStagingTable', 1, @debug
   return 0
end try

begin catch
   exec ptf_internal.HandleError @debug, N'PrepareStagingTable'
   return 1
end catch
GO


CREATE PROCEDURE ptf.SwitchPartition
   @part_table    nvarchar(256),         -- name of partitioned table
   @archive_table nvarchar(256) = NULL,  -- name of archive table to be created
   @staging_table nvarchar(256) = NULL,  -- name of staging table to be switched in
   @split_value   sql_variant = NULL,    -- boundary value for new partition
   @new_filegroup sysname = NULL,        -- filegroup the new partition is mapped to 
   @check_name    sysname = NULL,        -- name of check constaint for upper bound
   @upper_bound   sql_variant = NULL,    -- new upper bound for check constraint
   @make_readonly bit = 0,               -- make the input filegroup(s) read only
   @low_prio_wait int = 0,               -- minutes to wait in low priority queue
   @abort_action  nvarchar(8) = NULL,    -- whom to abort after wait period elapses
   @archive_mode  nvarchar(10) = NULL,   -- archive mode
   @fg_name       sysname = NULL OUTPUT, -- filegroup where table has been switched in
   @debug         int = 0
AS

declare @part_table_id int, @pf_id int, @num_part int, @is_right bit
declare @out_part int, @in_part int, @wait_opt nvarchar(80)
declare @stmt nvarchar(128), @temp_type tinyint, @rda_enabled bit, @archive int

begin try
   exec ptf_internal.Trace N'SwitchPartition', 0, @debug, 11,
      @part_table, @archive_table, @staging_table, @split_value,
      @new_filegroup, @check_name, @upper_bound, @make_readonly,
      @low_prio_wait, @abort_action, @archive_mode

   exec ptf_internal.GetObjId @part_table, 1, @part_table_id output

   if @part_table_id is null
      raiserror('the table ''%s'' does not exist', 16, 1, @part_table)

   if @archive_mode is null
      set @archive = case when @archive_table is null then 0 else 1 end
   else
   begin  
      set @archive = case lower(@archive_mode)
	                      when N'none' then 0 when N'save' then 1 when N'drop' then 2 else -1
                     end

      if @archive = -1
         raiserror('''%s'' is not a valid archive mode', 16, 1, @archive_mode)
      else if @archive = 1 and @archive_table is null
         raiserror('an archive table must be specified', 16, 1)
   end

   select @pf_id    = pf.function_id,
          @num_part = pf.fanout,
          @is_right = pf.boundary_value_on_right,
          @out_part = 1 + pf.boundary_value_on_right,
          @in_part  = dds.destination_id,
          @fg_name  = filegroup_name(dds.data_space_id)
     from sys.indexes i
     left join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
     left join sys.partition_functions pf on pf.function_id = ps.function_id
     left join sys.destination_data_spaces dds
       on dds.partition_scheme_id = ps.data_space_id and
          dds.destination_id = pf.fanout + pf.boundary_value_on_right - 1
    where i.object_id = @part_table_id and i.index_id in (0, 1)

   if @pf_id is null
      raiserror('the table ''%s'' is not partitioned', 16, 1, @part_table)
   else if @num_part < 2 and not (@archive = 0 and @staging_table is null)
      raiserror('no partition can be switched in or out', 16, 1)
   else if @out_part = @in_part and @archive <> 0 and @staging_table is not null
      raiserror('at least three partitions are required', 16, 1)

   if @@microsoftversion >= 0x0D000000
   begin
      set @stmt = N'select @tt = temporal_type, @rda = is_remote_data_archive_enabled' +
                  N'  from sys.tables where object_id = @tid'

      exec sp_executesql @stmt, N'@tid int, @tt tinyint output, @rda bit output',
           @part_table_id, @temp_type output, @rda_enabled output

      if @archive <> 0 and @temp_type = 2
         raiserror('partitions cannot be switched out from temporal tables', 16, 1)
      else if @staging_table is not null and @temp_type = 1
         raiserror('no partition can be switched to a temporal history table', 16, 1)

      if @rda_enabled = 1
         raiserror('stretch tables are not supported', 16, 1)
   end

   if @split_value is not null
   begin
      exec ptf_internal.CastVariant @pf_id, 1, @split_value output

      if exists (
         select * from sys.partition_range_values
          where function_id = @pf_id and parameter_id = 1 and value >= @split_value)
         raiserror('only the last partition can be split', 16, 1)
   end
   else if @staging_table is not null and @is_right = 1
      raiserror('a split value must be specified', 16, 1)

   if @check_name is not null and @upper_bound is null
   begin
      if @staging_table is not null and @is_right = 0 
         select @upper_bound = max(value)
           from sys.partition_range_values
          where function_id = @pf_id and parameter_id = 1
      else if @split_value is not null
         set @upper_bound = @split_value
      else
         raiserror('an upper bound for the check constraint must be specified', 16, 1)
   end

   if @make_readonly = 1 and SERVERPROPERTY('EngineEdition') = 5
      raiserror('on Azure SQL Database a filegroup cannot be set to read only', 16, 1)

   exec ptf_internal.GetLowPrioWaitOption @part_table_id,
        @low_prio_wait, @abort_action, @wait_opt output


   exec ptf.SwitchPartition2 @part_table_id,
        @archive_table, @out_part, @staging_table, @in_part,
        @split_value, @new_filegroup, @check_name, @upper_bound,
        @make_readonly, @wait_option = @wait_opt,
        @archive_mode = @archive, @debug = @debug

   exec ptf_internal.Trace N'SwitchPartition', 1, @debug
   return 0
end try

begin catch
   exec ptf_internal.HandleError @debug, N'SwitchPartition'
   return 1
end catch
GO


CREATE PROCEDURE ptf.SwapPartition
   @part_table    nvarchar(256),         -- name of partitioned table
   @staging_table nvarchar(256),         -- name of staging table
   @is_last       bit = 0,               -- if 1, it's the last swap operation
   @make_readonly bit = 0,               -- make the swapped filegroup read only
   @compression   nvarchar(20) = NULL,	 -- compression mode specifier
   @compress_all  bit = 0,               -- if 1, also compress nonclustered indexes
   @low_prio_wait int = 0,               -- minutes to wait in low priority queue
   @abort_action  nvarchar(8) = NULL,    -- whom to abort after wait period elapses
   @fg_name       sysname = NULL OUTPUT, -- filegroup where partition was swapped
   @debug         int = 0
AS

declare @part_table_id int, @tab_name nvarchar(256), @tab_schema sysname
declare @ki tinyint, @pf_id int, @swap_part int, @ptf_trg nvarchar(32)
declare @part_func nvarchar(256), @swap_table nvarchar(256)
declare @is_hist tinyint, @compr int, @wait_opt nvarchar(80), @am tinyint
declare @stmt nvarchar(1024), @svpt nvarchar(32)

begin try
   exec ptf_internal.Trace N'SwapPartition', 0, @debug, 8,
      @part_table, @staging_table, @is_last, @make_readonly,
      @compression, @compress_all, @low_prio_wait, @abort_action

   exec ptf_internal.GetObjId @part_table, 1, @part_table_id output,
        @quote_name = @tab_name output, @schema = @tab_schema output

   if @part_table_id is null
      raiserror('the table ''%s'' does not exist', 16, 1, @part_table)

   select @ki        = case when i.type = 5 then 1 else 0 end,
          @pf_id     = pf.function_id,
          @swap_part = case when pf.function_id is null then null
                            when pf.fanout <= 1 + pf.boundary_value_on_right then 0
							else dds.destination_id
                       end,
          @fg_name   = filegroup_name(isnull(dds.data_space_id, i.data_space_id)),
          @ptf_trg   = N'tr_ptf_' + cast(i.object_id as nvarchar) +
		               N'_' + cast(isnull(dds.destination_id, 1) as nvarchar),
          @part_func = N'$partition.' + quotename(pf.name) + N'(' +
                       quotename(col_name(ic.object_id, ic.column_id)) + N')'
     from sys.indexes i
     left join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
     left join sys.partition_functions pf on pf.function_id = ps.function_id
     left join sys.destination_data_spaces dds
       on dds.partition_scheme_id = ps.data_space_id and dds.destination_id = pf.fanout - 1
	 left join sys.index_columns ic
       on ic.object_id = i.object_id and ic.index_id = i.index_id and ic.partition_ordinal = 1
    where i.object_id = @part_table_id and i.index_id in (0, 1)

   if @swap_part = 0
      raiserror('not enough partitions exist for partition swapping', 16, 1)

   if @staging_table is null
      raiserror('a staging table name must be specified', 16, 1)

   set @swap_table =
       case when parsename(@staging_table, 2) is null then N''
            else quotename(parsename(@staging_table, 2)) + N'.'
       end + quotename(left(parsename(@staging_table, 1), 91) +
                       N'_' + cast(newid() as nvarchar(36)))

   if @@microsoftversion < 0x0D000000
      set @is_hist = 0
   else
      exec sp_executesql N'select @tt = temporal_type from sys.tables where object_id = @tid',
           N'@tid int, @tt tinyint output', @part_table_id, @is_hist output

   if @is_hist = 1
      raiserror('a partition swap is not allowed on a temporal history table', 16, 1)

   if @make_readonly = 1
   begin
      if SERVERPROPERTY('EngineEdition') = 5
         raiserror('on Azure SQL Database a filegroup cannot be set to read only', 16, 1)
      else if isnull(@is_last, 0) = 0
         raiserror('readonly can only be set for the last swap operation', 16, 1) 
   end

   set @compr =
       case when @compression is null then 0
	        else case lower(@compression)
                      when N'none' then 0 when N'row' then 1 when N'page' then 2
                      when N'columnstore' then 3 when N'columnstore_archive' then 4
                      else -1
                 end
       end

   if @compr = -1
      raiserror('''%s'' is not a valid compression value', 16, 1, @compression)

   else if isnull(@compr, 0) <> 0 and exists (
      select * from sys.columns where object_id = @part_table_id and is_sparse = 1)
      raiserror('compression is not possible due to sparse columns', 16, 1)

   exec ptf_internal.GetLowPrioWaitOption @part_table_id,
        @low_prio_wait, @abort_action, @wait_opt output

   set @am = case when @is_last = 1 then 2 else 1 end

   begin try
      if @@trancount = 0
         begin transaction
      else
      begin
         set @svpt = N'SwapPartition_svpt'
         save transaction @svpt
      end

      if not exists (
         select * from sys.triggers
          where parent_class = 1 and type = 'TR' and
                parent_id = @part_table_id and name = @ptf_trg)
      begin
         set @stmt = N'CREATE TRIGGER ' + @ptf_trg + N' ON ' + @tab_name +
             N' FOR INSERT, UPDATE, DELETE AS declare @p int' +
             case when @swap_part is null then N''
                  else N' = ' + cast(@swap_part as nvarchar)
             end + N'; if exists (select * from inserted' +
			 case when @swap_part is null then N''
                  else N' where ' + @part_func + N' = @p'
             end + N') or exists (select * from deleted' +
			 case when @swap_part is null then N''
                  else N' where ' + @part_func + N' = @p'
             end + N') begin raiserror(''' +
             case when @swap_part is null
                  then N'no DML operations are allowed on table'
                  else N'on partition %d no DML operations are allowed'
             end + N' while a PTF switch is active'', 16, 1, @p); rollback transaction end'

         exec ptf_internal.ExecStmt @stmt, @debug
         set @stmt = null
      end

      exec ptf.CreateIndexesAndConstraints @part_table_id, @staging_table,
           @part_num = @swap_part, @compression = @compr,
           @compress_all = @compress_all, @debug = @debug

      exec ptf.SwitchPartition2 @part_table_id,
           @archive_table = @swap_table, @out_part = @swap_part,
           @archive_mode = @am, @keep_indexes = @ki, @no_merge = 1,
           @staging_table = @staging_table, @in_part = @swap_part,
           @wait_option = @wait_opt, @debug = @debug

      set @stmt =
          case when @is_last = 1
               then N'DROP TRIGGER ' + quotename(@tab_schema) + N'.' + @ptf_trg
               else N'EXEC sp_rename ''' + @swap_table + N''', ''' +
                    parsename(@staging_table, 1) + N''', ''OBJECT'''
          end

      exec ptf_internal.ExecStmt @stmt, @debug

      if @svpt is null
         commit transaction
   end try

   begin catch
      if @svpt is null
         rollback transaction
      else if xact_state() = 1
         rollback transaction @svpt

      exec ptf_internal.HandleError @debug, null, @stmt
   end catch

   if @make_readonly = 1
      exec ptf_internal.ModifyFileGroups @part_table_id, @pf_id, @swap_part, 1, 0, @debug

   exec ptf_internal.Trace N'SwapPartition', 1, @debug
   return 0
end try

begin catch
   exec ptf_internal.HandleError @debug, N'SwapPartition'
   return 1
end catch
GO


CREATE PROCEDURE ptf.SwitchOut
   @part_table    nvarchar(256),       -- name of partitioned table
   @switch_table  nvarchar(256),       -- name of switch table to be created
   @part_value    sql_variant = NULL,  -- value that determines the partition to switch
   @part_num      int = NULL OUTPUT,   -- partition that will be switched out
   @keep_indexes  nvarchar(12) = NULL, -- the kind of indexes to keep on the table
   @low_prio_wait int = 0,             -- minutes to wait in low priority queue
   @abort_action  nvarchar(8) = NULL,  -- whom to abort after wait period elapses
   @debug         int = 0
AS

declare @part_table_id int, @pf_id int, @ki int
declare @wait_opt nvarchar(80), @stmt nvarchar(max)

begin try
   exec ptf_internal.Trace N'SwitchOut', 0, @debug, 7,
      @part_table, @switch_table, @part_value, @part_num,
      @keep_indexes, @low_prio_wait, @abort_action

   exec ptf_internal.GetObjId @part_table, 1, @part_table_id output

   if @part_table_id is null
      raiserror('the table ''%s'' does not exist', 16, 1, @part_table)

   if @switch_table is null
      raiserror('a switch table name must be specified', 16, 1)

   select @pf_id = ps.function_id
     from sys.indexes i
     left join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
    where i.object_id = @part_table_id and i.index_id in (0, 1)

   if @pf_id is null
      set @part_num = 1

   else if @part_num is null
   begin
      select @stmt = N'select @pid = $partition.' + quotename(name) +
             N'(cast(@val as ' + ptf_internal.GetTypeStr(function_id, 1) + N'))'
        from sys.partition_functions
       where function_id = @pf_id

      exec sp_executesql @stmt, N'@val sql_variant, @pid int output',
           @part_value, @part_num output

      if @part_num is null
         raiserror('the partition number could not be determined', 16, 1) 
   end

   exec ptf_internal.GetLowPrioWaitOption @part_table_id,
        @low_prio_wait, @abort_action, @wait_opt output

   set @ki = case when @keep_indexes is null then 2
                  else case lower(@keep_indexes)
                            when N'none' then 0 when N'clustered' then 1
                            when N'nonclustered' then 2 when N'all' then 3
                            else -1
                       end
             end

   if @ki = -1
      raiserror('''%s'' is not a valid value for keep indexes', 16, 1, @keep_indexes)


   exec ptf.SwitchPartition2 @part_table_id, @switch_table, @part_num,
        @keep_indexes = @ki, @no_merge = 1, @wait_option = @wait_opt,
		@archive_mode = 1, @debug = @debug


   exec ptf_internal.Trace N'SwitchOut', 1, @debug
   return 0
end try

begin catch
   exec ptf_internal.HandleError @debug, N'SwitchOut'
   return 1
end catch
GO


CREATE PROCEDURE ptf.SwitchIn
   @part_table     nvarchar(256),		  -- name of partitioned table
   @switch_table   nvarchar(256),		  -- name of the table to be switched in
   @part_value     sql_variant = NULL,    -- determines the partition to be switched
   @part_num       int = NULL,		      -- partition where table will be switched in
   @create_indexes nvarchar(12) = NULL,   -- create 'none' or 'all' indexes on table
   @make_readonly  bit = 0,               -- make the input filegroup(s) read only
   @low_prio_wait  int = 0,               -- minutes to wait in low priority queue
   @abort_action   nvarchar(8) = NULL,    -- whom to abort after wait period elapses
   @fg_name        sysname = NULL OUTPUT, -- filegroup where table has been switched in
   @debug          int = 0
AS

declare @part_table_id int, @pf_id int, @switch_tab_id int
declare @idx_id int, @ci int, @wait_opt nvarchar(80)
declare @stmt nvarchar(max), @svpt nvarchar(32), @is_hist tinyint

begin try
   exec ptf_internal.Trace N'SwitchIn', 0, @debug, 8,
      @part_table, @switch_table, @part_value, @part_num,
      @create_indexes, @make_readonly, @low_prio_wait, @abort_action

   exec ptf_internal.GetObjId @part_table, 1, @part_table_id output

   if @part_table_id is null
      raiserror('the table ''%s'' does not exist', 16, 1, @part_table)

   if @switch_table is null
      raiserror('a switch table name must be specified', 16, 1)

   exec ptf_internal.GetObjId @switch_table, 1, @switch_tab_id output, @stmt output

   if isnull(@debug, 0) <> 2 and @switch_tab_id is null
      raiserror('the switch table ''%s'' does not exist', 16, 1, @switch_table)

   select @pf_id = ps.function_id
     from sys.indexes i
     left join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
    where i.object_id = @part_table_id and i.index_id in (0, 1)


   if @part_num is null and @pf_id is not null
   begin
      if @part_value is null and @switch_tab_id is not null
      begin
         if (select sum(rows) from sys.partitions where object_id = @switch_tab_id) = 0
            raiserror('either a partition number or partition value must be specified', 16, 1)

         select @stmt = N'select top 1 @val = ' + quotename(c.name) + N' from ' +
                        @stmt + N' where ' + quotename(c.name) + N' is not null'
           from sys.index_columns ic join sys.columns c
             on c.object_id = ic.object_id and c.column_id = ic.column_id
          where ic.object_id = @part_table_id and ic.index_id in (0, 1) and
                ic.partition_ordinal = 1

         exec sp_executesql @stmt, N'@val sql_variant output', @part_value output
      end

      if @part_value is not null
      begin
         select @stmt = N'select @pid = $partition.' + quotename(name) +
                N'(cast(@val as ' + ptf_internal.GetTypeStr(function_id, 1) + N'))'
           from sys.partition_functions
          where function_id = @pf_id

         exec sp_executesql @stmt, N'@val sql_variant, @pid int output',
              @part_value, @part_num output
      end

      if @part_num is null
         raiserror('the partition number could not be determined', 16, 1) 
   end

   if @@microsoftversion < 0x0D000000
      set @is_hist = 0
   else
      exec sp_executesql N'select @tt = temporal_type from sys.tables where object_id = @tid',
	       N'@tid int, @tt tinyint output', @part_table_id, @is_hist output

   if @is_hist = 1
      raiserror('no partition can be switched into a temporal history table', 16, 1)

   if @create_indexes is not null and
      lower(@create_indexes) not in (N'none', N'all', N'columnstore')
      raiserror('''%s'' is not a valid create_indexes value', 16, 1, @create_indexes)

   if @make_readonly = 1 and SERVERPROPERTY('EngineEdition') = 5
      raiserror('on Azure SQL Database a filegroup cannot be set to read only', 16, 1)

   exec ptf_internal.GetLowPrioWaitOption @part_table_id,
        @low_prio_wait, @abort_action, @wait_opt output

   begin try
      if @@trancount = 0
         begin transaction
      else
      begin
         set @svpt = N'SwitchIn_svpt'
         save transaction @svpt
      end

      if isnull(lower(@create_indexes), N'columnstore') = N'columnstore'
      begin
         select @idx_id = index_id from sys.indexes
          where object_id = @part_table_id and type = 6

         if @idx_id is not null
            exec ptf.CreateIndex @part_table_id, @idx_id, @switch_table,
                 @part_num = @part_num, @debug = @debug
      end
      else if lower(@create_indexes) = N'all'
         exec ptf.CreateIndexesAndConstraints @part_table_id, @switch_table,
		      @part_num = @part_num, @debug = @debug

      exec ptf.SwitchPartition2 @part_table_id,
           @staging_table = @switch_table, @in_part = @part_num,
           @wait_option = @wait_opt, @debug = @debug

      if @svpt is null
         commit transaction
   end try

   begin catch
      if @svpt is null
         rollback transaction
      else if xact_state() = 1
         rollback transaction @svpt

      exec ptf_internal.HandleError
   end catch

   if @make_readonly = 1
      exec ptf_internal.ModifyFileGroups @part_table_id, @pf_id, @part_num, 1, 0, @debug

   select @fg_name = filegroup_name(isnull(dds.data_space_id, i.data_space_id))
     from sys.indexes i join sys.destination_data_spaces dds
       on dds.partition_scheme_id = i.data_space_id and dds.destination_id = @part_num
    where i.object_id = @part_table_id and i.index_id in (0, 1)

   exec ptf_internal.Trace N'SwitchIn', 1, @debug
   return 0
end try

begin catch
   exec ptf_internal.HandleError @debug, N'SwitchIn'
   return 1
end catch
GO


CREATE PROCEDURE ptf.DropPartitions
   @part_table         nvarchar(256),         -- name of partitioned table
   @archive_table      nvarchar(256) = NULL,  -- name of archive table to be created
   @partition_function sysname = NULL,        -- name of partition function to be created
   @partition_scheme   sysname = NULL,        -- name of partition scheme to be created
   @start_partition    int = NULL,            -- first partition to be switched out
   @num_partitions     int = NULL,            -- number of partitions to be switched out
   @stop_at            sql_variant = NULL,    -- boundary value to stop at
   @low_prio_wait      int = 0,               -- minutes to wait in low priority queue
   @abort_action       nvarchar(8) = NULL,    -- whom to abort after wait period elapses
   @fg_name            sysname = NULL OUTPUT, -- filegroup that has been emptied
   @debug              int = 0
AS
set nocount on

declare @part_tab_id int, @part_tab_name nvarchar(256)
declare @idx_id int, @ps_id int, @pf_id int, @num_part int
declare @is_right bit, @first_part int, @pcol_id int
declare @temp_type bit, @id int, @target_table nvarchar(256), @sa nvarchar(max)
declare @row_compr_list nvarchar(max), @page_compr_list nvarchar(max)
declare @wait_opt nvarchar(80), @fgn sysname, @fgid int, @n int
declare @stmt nvarchar(max), @svpt nvarchar(32)

begin try
   exec ptf_internal.Trace N'DropPartitions', 0, @debug, 9,
      @part_table, @archive_table, @partition_function, @partition_scheme,
	  @start_partition, @num_partitions, @stop_at, @low_prio_wait, @abort_action

   exec ptf_internal.GetObjId @part_table, 1, @part_tab_id output, @part_tab_name output

   if @part_tab_id is null
      raiserror('the table ''%s'' does not exist', 16, 1, @part_table)

   select @idx_id     = i.index_id,
          @ps_id      = i.data_space_id,
          @pf_id      = pf.function_id,
          @num_part   = pf.fanout,
          @is_right   = pf.boundary_value_on_right,
          @first_part = 1 + pf.boundary_value_on_right,
		  @pcol_id    = ic.column_id
     from sys.indexes i
     left join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
     left join sys.partition_functions pf on pf.function_id = ps.function_id
     left join sys.index_columns ic on ic.partition_ordinal = 1 and
          ic.object_id = i.object_id and ic.index_id = i.index_id
    where i.object_id = @part_tab_id and i.index_id in (0, 1)

   if @num_part is null
      raiserror('the table ''%s'' is not partitioned', 16, 1, @part_table)
   else if @num_part < 2
      raiserror('no partition can be dropped', 16, 1)

   if exists (
      select * from sys.indexes i
       where is_disabled = 0 and is_hypothetical = 0 and (
             object_id = @part_tab_id and not exists (
             select * from sys.index_columns
              where object_id = i.object_id and index_id = i.index_id and
                    partition_ordinal = 1 and column_id = @pcol_id) or (
             object_id = @part_tab_id or object_id in (
                select referencing_id from sys.sql_expression_dependencies
                 where referenced_id = @part_tab_id and
                       referencing_id in (select object_id from sys.views))) and
             data_space_id not in (
             select data_space_id from sys.partition_schemes where function_id = @pf_id)))
      raiserror('the table has indexes that are not partition aligned', 16, 1)

   if isnull(@debug, 0) <> 2 and @part_tab_id in (
         select referenced_object_id from sys.foreign_keys where is_disabled = 0)
      raiserror('the table must not be referenced by a foreign key constraint', 16, 1)
   else if @part_tab_id in (select object_id from sys.change_tracking_tables)
      raiserror('change tracking must not be enabled for the table', 16, 1)

   if @@microsoftversion < 0x0D000000
      set @temp_type = 0
   else
      exec sp_executesql N'select @tt = temporal_type from sys.tables where object_id = @tid',
	       N'@tid int, @tt tinyint output', @part_tab_id, @temp_type output

   if @temp_type = 2
      raiserror('no partitions can be dropped from temporal tables', 16, 1)

   if @archive_table is not null
   begin
      exec ptf_internal.GetObjId @archive_table, 0, @id output, @target_table output

      if @id is not null and isnull(@debug, 0) <> 2
         raiserror('the archive table ''%s'' already exists', 16, 1, @target_table)
   end
   else if @@microsoftversion < 0x0D000000 or @temp_type <> 0 or
      @part_tab_id in (select referenced_object_id from sys.foreign_keys) or
      @part_tab_id in (select referenced_id from sys.sql_expression_dependencies
                        where referencing_id in (select object_id from sys.views) and
                              referencing_id in (select object_id from sys.indexes))
      select @target_table = quotename(schema_name(schema_id)) + N'.' +
	         quotename(left(name, 91) + N'_' + cast(newid() as nvarchar(36)))
	    from sys.tables where object_id = @part_tab_id


   if @start_partition is null
      set @start_partition = @first_part
   else if @start_partition < @first_part or @start_partition >= @num_part + @is_right
      raiserror('''%d'' is not a valid start partition', 16, 1, @start_partition)

   if @num_partitions is null
   begin
      if @stop_at is null
         set @num_partitions = 1
	  else
	  begin
         exec ptf_internal.CastVariant @pf_id, 1, @stop_at output
         set @sa = ptf_internal.VariantToString(@stop_at)

	     select @num_partitions = boundary_id - @start_partition + 1
		   from sys.partition_range_values
		  where function_id = @pf_id and parameter_id = 1 and value = @stop_at

		 if @num_partitions is null or @num_partitions < 1
		    raiserror('%s is not a valid stop at boundary value', 16, 1, @sa)
	  end
   end
   else if @num_partitions = -1
      set @num_partitions = @num_part - @start_partition + @is_right
   else if @num_partitions < 1 or
      @num_partitions > @num_part - @start_partition + @is_right
      raiserror('''%d'' is not a valid partition count', 16, 1, @num_partitions)


   if @target_table is not null
   begin
      if @num_partitions > 1
      begin
         if @partition_function is null
            select @partition_function =
                   left(name, 91) + N'_' + cast(newid() as nvarchar(36))
              from sys.partition_functions where function_id = @pf_id
         else if isnull(@debug, 0) <> 2 and exists (
            select * from sys.partition_functions where name = @partition_function)
            raiserror('partition function ''%s'' already exists', 16, 1, @partition_function)

         if @partition_scheme is null
            select @partition_scheme =
                   left(name, 91) + N'_' + cast(newid() as nvarchar(36))
              from sys.partition_schemes where data_space_id = @ps_id
         else if isnull(@debug, 0) <> 2 and exists (
            select * from sys.partition_schemes where name = @partition_scheme)
            raiserror('partition scheme ''%s'' already exists', 16, 1, @partition_scheme)
      end

      exec ptf_internal.GetLowPrioWaitOption @part_tab_id,
           @low_prio_wait, @abort_action, @wait_opt output
   end


   if isnull(@debug, 0) <> 2 and exists (
      select *
        from sys.partitions p join sys.indexes i
          on i.object_id = p.object_id and i.index_id = p.index_id and i.data_space_id in (
             select data_space_id from sys.partition_schemes where function_id = @pf_id)
       where p.rows <> 0 and p.partition_number >= @start_partition and
             p.partition_number < @start_partition + @num_partitions and
             p.object_id in (select object_id from sys.tables where object_id <> @part_tab_id))
      raiserror('the partitions cannot be merged due to non-empty partitions', 16, 1)


   exec ptf_internal.ModifyFileGroups
        @part_tab_id, @pf_id, @start_partition, @num_partitions, 1, @debug

   if @@trancount = 0
      begin transaction
   else
   begin
      set @svpt = N'DropPartitions_svpt'
      save transaction @svpt
   end

   begin try
      if @target_table is null
      begin
         set @stmt = N'TRUNCATE TABLE ' + @part_tab_name +
             N' WITH (PARTITIONS (' + cast(@start_partition as nvarchar) +
             case when @num_partitions = 1 then N''
                  else N' TO ' + cast(@start_partition + @num_partitions - 1 as nvarchar)
             end + N'))'

         exec ptf_internal.ExecStmt @stmt, @debug
      end

      else if @num_partitions = 1
      begin
         exec ptf.CreateTableClone @part_tab_id, @target_table,
              @part_num = @start_partition, @debug = @debug

         exec ptf.CreateIndex @part_tab_id, @idx_id, @target_table,
              @part_num = @start_partition, @debug = @debug

         set @stmt = N'ALTER TABLE ' + @part_tab_name +
                     N' SWITCH PARTITION ' + cast(@start_partition as nvarchar) +
                     N' TO ' + @target_table +
                     case when isnull(len(@wait_opt), 0) = 0 then N''
                          else N' WITH (' + @wait_opt + N')'
                     end

         exec ptf_internal.ExecStmt @stmt, @debug
      end
      
      else
      begin
         select @stmt = N''
         select @stmt = @stmt + N', ' + ptf_internal.VariantToString(value)
           from sys.partition_range_values
          where function_id = @pf_id and parameter_id = 1 and
                boundary_id >= @start_partition - @is_right and
                boundary_id < @start_partition + @num_partitions - @is_right
          order by boundary_id

         select @stmt = N'CREATE PARTITION FUNCTION ' +
                quotename(@partition_function) + N'(' +
                ptf_internal.GetTypeStr(@pf_id, 1) + N') AS RANGE ' +
                case when @is_right = 1 then N'RIGHT' else 'LEFT' end +
                N' FOR VALUES' + stuff(@stmt, 1, 2, N' (') + N')'

         exec ptf_internal.ExecStmt @stmt, @debug

         select @stmt = N''
         select @stmt = @stmt + N', ' + quotename(fg.name)
           from sys.destination_data_spaces dds
           join sys.filegroups fg on fg.data_space_id = dds.data_space_id
          where dds.partition_scheme_id = @ps_id and
                dds.destination_id >= @start_partition and
                dds.destination_id < @start_partition + @num_partitions
          order by dds.destination_id

         select @stmt = N'CREATE PARTITION SCHEME ' + quotename(@partition_scheme) +
                N' AS PARTITION ' + quotename(@partition_function) + N' TO' +
                case when @is_right = 0
                     then stuff(@stmt, 1, 2, N' (') + N', ' + quotename(fg.name)
                     else N' (' + quotename(fg.name) + @stmt
                end + N')'
           from sys.destination_data_spaces dds
           join sys.filegroups fg on fg.data_space_id = dds.data_space_id
          where dds.partition_scheme_id = @ps_id and
                dds.destination_id = case when @is_right = 0 then @num_part else 1 end

         exec ptf_internal.ExecStmt @stmt, @debug
	     set @stmt = null

         exec ptf.CreateTableClone @part_tab_id, @target_table,
              @part_num = @start_partition, @debug = @debug

         exec ptf.CreateIndex @part_tab_id, @idx_id, @target_table,
              @part_scheme = @partition_scheme, @part_col_id = @pcol_id, @debug = @debug


         select @row_compr_list = N'', @page_compr_list = N''
         select @row_compr_list = @row_compr_list +
                case when data_compression <> 1 then N''
                     else N', ' +
                          cast(partition_number - @start_partition + @is_right + 1 as nvarchar)
                end,
                @page_compr_list = @page_compr_list +
                case when data_compression <> 2 then N''
		             else N', ' +
                          cast(partition_number - @start_partition + @is_right + 1 as nvarchar)
                end
           from sys.partitions
          where object_id = @part_tab_id and index_id = @idx_id and
                partition_number >= @start_partition and
		        partition_number < @start_partition + @num_partitions

         if len(@row_compr_list) + len(@page_compr_list) > 0
         begin
            set @stmt = case when len(@row_compr_list) = 0 then N''
                             else N', DATA_COMPRESSION = ROW ON PARTITIONS' +
                                  stuff(@row_compr_list, 1, 2, N' (') + N')'
                        end +
                        case when len(@page_compr_list) = 0 then N''
                             else N', DATA_COMPRESSION = PAGE ON PARTITIONS' +
                                  stuff(@page_compr_list, 1, 2, N' (') + N')'
                        end

            set @stmt = N'ALTER TABLE ' + @target_table +
                        N' REBUILD PARTITION = ALL WITH' +
                        stuff(@stmt, 1, 2, N' (') + N')'

            exec ptf_internal.ExecStmt @stmt, @debug
         end

         set @n = 0
         while @n < @num_partitions
         begin
            set @stmt = N'ALTER TABLE ' + @part_tab_name + N' SWITCH PARTITION ' +
                cast(@start_partition + @n as nvarchar) + N' TO ' + @target_table +
                N' PARTITION ' + cast(1 + @n + @is_right as nvarchar) +
                case when isnull(len(@wait_opt), 0) = 0 then N''
                     else N' WITH (' + @wait_opt + N')'
                end

            exec ptf_internal.ExecStmt @stmt, @debug
            set @wait_opt = N''
            set @n = @n + 1
         end
      end


      select top 1 @fgid = data_space_id, @n = count(*)
        from sys.destination_data_spaces
       where partition_scheme_id = @ps_id and
             destination_id >= @start_partition and
             destination_id < @start_partition + @num_partitions
       group by data_space_id

      set @fg_name = case when @n < @num_partitions then null else filegroup_name(@fgid) end


      select @n = 0, @start_partition = @start_partition - @is_right
      while @n < @num_partitions
      begin
         select @stmt = N'ALTER PARTITION FUNCTION ' + quotename(pf.name) + 
				N'() MERGE RANGE (' + ptf_internal.VariantToString(prv.value) + N')'
           from sys.partition_functions pf join sys.partition_range_values prv
             on prv.function_id = pf.function_id and prv.parameter_id = 1 and
                prv.boundary_id = @start_partition + case when @debug = 2 then @n else 0 end
          where pf.function_id = @pf_id

         exec ptf_internal.ExecStmt @stmt, @debug

         select @stmt = null, @n = @n + 1

         exec ptf_internal.UpdateStatistics @part_tab_name, 0, 1, @start_partition, null, @debug
      end


      if @archive_table is null and @target_table is not null
      begin
         set @stmt = N'DROP TABLE ' + @target_table

         if @num_partitions > 1
         begin
            set @stmt = @stmt + N'; DROP PARTITION SCHEME ' + quotename(@partition_scheme)
            set @stmt = @stmt + N'; DROP PARTITION FUNCTION ' + quotename(@partition_function)
         end

         exec ptf_internal.ExecStmt @stmt, @debug
      end

      if @svpt is null
         commit transaction
   end try

   begin catch
      if @svpt is null
         rollback transaction
      else if xact_state() = 1
         rollback transaction @svpt

      exec ptf_internal.HandleError @debug, null, @stmt
   end catch

   exec ptf_internal.Trace N'DropPartitions', 1, @debug
   return 0
end try

begin catch
   exec ptf_internal.HandleError @debug, N'DropPartitions'
   return 1
end catch
GO


CREATE PROCEDURE ptf.DropTable
   @table_name nvarchar(256), -- name of table to be dropped
   @debug      int = 0
AS
set nocount on

declare @tab_id int, @ps_id int, @pf_id int
declare @temp_type tinyint, @post_stmt nvarchar(1024)
declare @stmt nvarchar(max), @svpt nvarchar(32)

begin try
   exec ptf_internal.Trace N'DropTable', 0, @debug, 1, @table_name

   exec ptf_internal.GetObjId @table_name, 1, @tab_id output, @table_name output

   if @tab_id is not null
   begin
      if @tab_id in (select referenced_object_id from sys.foreign_keys)
         raiserror('the table must not be referenced by a foreign key constraint', 16, 1)

      if exists (
         select * from sys.sql_expression_dependencies
          where is_schema_bound_reference = 1 and referenced_id = @tab_id and
                referencing_id not in (select object_id from sys.check_constraints))
         raiserror('the table must not be referenced by a schema bound object', 16, 1)

      if isnull(@debug, 0) <> 2 and exists (
         select * from sys.filegroups where is_read_only = 1 and (
                data_space_id in (select data_space_id
                                    from sys.indexes where object_id = @tab_id) or
                data_space_id in (select lob_data_space_id
                                    from sys.tables where object_id = @tab_id) or
                data_space_id in (select filestream_data_space_id
                                    from sys.tables where object_id = @tab_id) or
                data_space_id in (select data_space_id from sys.destination_data_spaces
                                   where partition_scheme_id in (
                                         select data_space_id from sys.indexes
                                          where object_id = @tab_id)) or
                data_space_id in (select data_space_id from sys.destination_data_spaces
                                   where partition_scheme_id in (
                                         select filestream_data_space_id from sys.tables
                                          where object_id = @tab_id and
                                                filestream_data_space_id is not null))))
         raiserror('cannot drop the table due to read-only filegroups', 16, 1)

      exec ptf_internal.HandleVersioning @tab_id, 6,
           @stmt output, @post_stmt output, @temp_type output

      if @temp_type = 1
         raiserror('cannot drop system-versioned temporal history tables', 16, 1)
   end
   else if isnull(@debug, 0) <> 2
      raiserror('the table ''%s'' does not exist', 16, 1, @table_name)
   else
      select @stmt = N'', @post_stmt = N''

   if @@trancount = 0
      begin transaction
   else
   begin
      set @svpt = N'DropTable_svpt'
      save transaction @svpt
   end

   begin try
      set @stmt = @stmt + N'DROP TABLE ' + @table_name + @post_stmt
      exec ptf_internal.ExecStmt @stmt, @debug

      select top 1 @ps_id = i.data_space_id, @pf_id = ps.function_id
        from sys.indexes i left join sys.partition_schemes ps
          on ps.data_space_id = i.data_space_id
       where i.object_id = @tab_id
       order by i.index_id

      if @ps_id is not null and not exists (
         select * from sys.indexes where object_id <> @tab_id and data_space_id = @ps_id)
      begin
         select @stmt = N'DROP PARTITION SCHEME ' + quotename(name)
           from sys.partition_schemes where data_space_id = @ps_id

         exec ptf_internal.ExecStmt @stmt, @debug

         if @pf_id is not null and not exists (
            select * from sys.partition_schemes
             where data_space_id <> @ps_id and function_id = @pf_id)
         begin
            select @stmt = N'DROP PARTITION FUNCTION ' + quotename(name)
              from sys.partition_functions where function_id = @pf_id

            exec ptf_internal.ExecStmt @stmt, @debug
         end
      end

      if @svpt is null
         commit transaction
   end try

   begin catch
      if @svpt is null
         rollback transaction
      else if xact_state() = 1
         rollback transaction @svpt

      exec ptf_internal.HandleError @debug, null, @stmt
   end catch

   exec ptf_internal.Trace N'DropTable', 1, @debug
   return 0
end try

begin catch
   exec ptf_internal.HandleError @debug, N'DropTable'
   return 1
end catch
GO


CREATE PROCEDURE ptf.CopyPartition
   @tab_id       int,            -- ID of original (unpartitioned) table
   @src_tab      nvarchar(256),  -- name of temp table that has the data
   @part_tab     nvarchar(256),  -- name of partitioned target table
   @part_num     int,            -- number of target partition
   @ps_id        int,            -- ID of partition scheme
   @part_col_id  int,            -- ID of the partitioning column
   @fg_name      sysname = NULL, -- filegroup to create temp table in
   @compression  int = NULL,     -- compression to apply to target partition
   @compress_all bit = 0,        -- if 1, also compress nonclustered indexes
   @debug        int = 0
AS
set nocount on

declare @idx_id int, @pcol sysname, @nullable bit, @pf_id int, @check nvarchar(512)
declare @temp_table nvarchar(256), @drop_cmd nvarchar(512), @stmt nvarchar(max)

if @tab_id is null or @tab_id not in (select object_id from sys.tables) or
   isnull(len(@src_tab), 0) = 0 or isnull(len(@part_tab), 0) = 0 or
   @part_col_id not in (select column_id from sys.columns where object_id = @tab_id) or
   @ps_id not in (select data_space_id from sys.partition_schemes) or
   @part_num not in (
   select destination_id from sys.destination_data_spaces where partition_scheme_id = @ps_id) or
   isnull(@compression, 0) not between 0 and 4
   return

begin try
   exec ptf_internal.Trace N'CopyPartition', 0, @debug, 9,
        @tab_id, @src_tab, @part_tab, @part_num, @ps_id,
        @part_col_id, @fg_name, @compression, @compress_all

   select @pf_id = function_id from sys.partition_schemes where data_space_id = @ps_id

   select @pcol = name, @nullable = is_nullable from sys.columns
    where object_id = @tab_id and column_id = @part_col_id

   set @check = ptf_internal.GetPartitionCheck (@pf_id, @pcol, @nullable, @part_num, null)

   select @idx_id = index_id from sys.indexes where object_id = @tab_id and index_id in (0, 1)

   if @idx_id = 0 set @fg_name = null

   select @temp_table = quotename(schema_name(schema_id)) + N'.' + quotename(
          left(name, 119) + N'_##_' + right(N'0000' + cast(@part_num as nvarchar), 5))
     from sys.tables where object_id = @tab_id

   exec ptf.CreateTableClone @tab_id, @temp_table, @fg_name, @part_num, @ps_id, @debug = @debug
   set @drop_cmd = N'DROP TABLE ' + @temp_table

   begin try
      if @fg_name is null
         exec ptf.CreateIndex @tab_id, @idx_id, @temp_table, null, @part_num, @ps_id,
              @part_col_id = @part_col_id, @compression = @compression, @debug = @debug

      select @stmt = N''
      select @stmt = @stmt + N', ' + quotename(name)
        from sys.columns where object_id = @tab_id and user_type_id <> 189 order by column_id

      select @stmt = N'INSERT INTO ' + @temp_table +
             N' WITH (TABLOCK)' + stuff(@stmt, 1, 2, N' (') + N') SELECT' +
             right(@stmt, len(@stmt) - 1) + N' FROM ' + @src_tab +
             case when @check is null then N'' else N' WHERE ' + @check end

      exec ptf_internal.ExecStmt @stmt, @debug
      set @stmt = null

      exec ptf.CreateIndexesAndConstraints @tab_id, @temp_table, @part_num, @ps_id,
           @part_col_id = @part_col_id, @compression = @compression,
           @compress_all = @compress_all, @debug = @debug

      if @check is not null
      begin
         set @stmt = N'ALTER TABLE ' + @temp_table + N' ADD CHECK (' + @check + N')'
         exec ptf_internal.ExecStmt @stmt, @debug
      end

      set @stmt = N'ALTER TABLE ' + @temp_table + N' SWITCH TO ' + @part_tab +
                  N' PARTITION ' + cast(@part_num as nvarchar)

      exec ptf_internal.ExecStmt @stmt, @debug
   end try

   begin catch
      if isnull(@debug, 0) <> 2 exec sp_executesql @drop_cmd
      exec ptf_internal.HandleError @debug, null, @stmt
   end catch

   exec ptf_internal.ExecStmt @drop_cmd, @debug
   exec ptf_internal.Trace N'CopyPartition', 1, @debug
end try

begin catch
   exec ptf_internal.HandleError @debug, N'CopyPartition'
end catch
GO


CREATE PROCEDURE ptf.PartitionTable
   @source_table      nvarchar(256),       -- name of table to be partitioned
   @partition_scheme  sysname,             -- scheme used to partition the table
   @partition_column  sysname,             -- name of partitioning column
   @fg_name           sysname = NULL,      -- filegroup where to create staging table
   @compression       nvarchar(20) = NULL, -- 'none', 'row' or 'page' compression
   @compress_all      bit = 0,             -- if 1, also compress nonclustered indexes
   @incremental_stats bit = 1,             -- 1, indexes created with incremental stats
   @fast_mode         bit = null,
   @debug             int = 0
AS
set nocount on

declare @stmt nvarchar(max), @stmt2 nvarchar(1024), @svpt nvarchar(32)
declare @source_tab_id int, @full_name nvarchar(256)
declare @tab_name nvarchar(256), @tab_schema sysname
declare @ps_id int, @pf_id int, @num_part int, @pstart int
declare @in_part int, @in_fg int, @in_fs int, @fs_id int
declare @pcol_id int, @nullable bit, @stat tinyint
declare @compr int, @fast_mode_not_ok int, @count_rows bigint, @pnum int
declare @temp_tab nvarchar(256), @drop_temp_stmt nvarchar(512)
declare @is_temporal tinyint, @pre_stmt nvarchar(1024), @post_stmt nvarchar(1024)
declare @check nvarchar(512), @chk_name sysname, @drop_chk nvarchar(512)
declare @rev_switch nvarchar(550), @part_tab nvarchar(256)

begin try
   exec ptf_internal.Trace N'ParitionTable', 0, @debug, 8,
      @source_table, @partition_scheme, @partition_column, @fg_name,
      @compression, @compress_all, @incremental_stats, @fast_mode

   if @source_table is null
      raiserror('the name of the table to be partitioned must be specified', 16, 1)

   exec ptf_internal.GetObjId @source_table, 1, @source_tab_id output,
        @full_name output, @tab_name output, @tab_schema output

   if @source_tab_id is null
      raiserror('the table ''%s'' does not exist', 16, 1, @source_table)

   if @source_tab_id in (select object_id from sys.indexes where type not in (0, 1, 2, 5, 6))
      raiserror('the table has an index of a type that is not supported', 16, 1)
   else if @source_tab_id in (select object_id from sys.fulltext_indexes)
      raiserror('the table must not have a fulltext index', 16, 1)
   else if @source_tab_id in (
      select referenced_object_id from sys.foreign_keys where is_disabled = 0)
      raiserror('the table must not be referenced by a foreign key constraint', 16, 1)
   else if @source_tab_id in (select object_id from sys.change_tracking_tables)
      raiserror('change tracking must not be enabled for the table', 16, 1)
   else if @source_tab_id in (
      select referenced_id from sys.sql_expression_dependencies
       where referencing_id in (select object_id from sys.views) and
             referencing_id in (select object_id from sys.indexes where index_id = 1))
      raiserror('the table must not be referenced by an indexed view', 16, 1)

   select @stat = case when t.is_ms_shipped = 0 then 0 else 1 end +
                  case when t.is_published = 0 and t.is_schema_published = 0 and
                            t.is_replicated = 0 and t.is_merge_published = 0 and
                            t.is_sync_tran_subscribed = 0
                       then 0 else 2
                  end +
                  case when t.is_tracked_by_cdc = 0 then 0 else 4 end +
                  case when ds.type is null then 8 when ds.type = 'PS' then 16 else 0 end
     from sys.tables t left join sys.indexes i
       on i.object_id = t.object_id and i.index_id in (0, 1)
     left join sys.data_spaces ds on ds.data_space_id = i.data_space_id
    where t.object_id = @source_tab_id

   if (@stat & 1) = 1
      raiserror('cannot partition MS-shipped tables', 16, 1)
   else if (@stat & 2) = 2
      raiserror('cannot partition tables that are replicated', 16, 1)
   else if (@stat & 4) = 4
      raiserror('cannot partition tables that are CDC enabled', 16, 1)
   else if (@stat & 8) = 8
      raiserror('cannot partition memory-optimized tables', 16, 1)
   else if (@stat & 16) = 16
      raiserror('the table ''%s'' is already partitioned', 16, 1, @source_table)


   if @partition_scheme is null
      raiserror('a partition scheme name must be specified', 16, 1)

   select @ps_id    = ps.data_space_id,
          @pf_id    = ps.function_id,
          @num_part = pf.fanout,
          @in_part  = dds.destination_id,
          @in_fg    = case when fg.type = 'FG' then fg.data_space_id else null end,
          @pstart   = case when pf.boundary_value_on_right = 1 and
                                pf.fanout > 1 and prv.value is null
                           then 1 else 0
                      end
     from sys.partition_schemes ps
     join sys.partition_functions pf on pf.function_id = ps.function_id
     join sys.destination_data_spaces dds
       on dds.partition_scheme_id = ps.data_space_id and dds.destination_id =
          case when pf.boundary_value_on_right = 0 or pf.fanout = 1 then 1 else 2 end
     join sys.filegroups fg on fg.data_space_id = dds.data_space_id
     left join sys.partition_range_values prv
       on prv.function_id = pf.function_id and prv.parameter_id = 1 and prv.boundary_id = 1
    where ps.name = @partition_scheme

   if @ps_id is null
      raiserror('the partition scheme ''%s'' does not exist', 16, 1, @partition_scheme)
   else if @in_fg is null
      raiserror('the partition scheme must not map to filestream filegroups', 16, 1)

   if @source_tab_id in (select object_id from sys.columns where is_filestream = 1)
   begin
      select top 1 @fs_id = partition_scheme_id, @in_fs = data_space_id
        from sys.destination_data_spaces
       where destination_id = @in_part and partition_scheme_id in (
             select data_space_id from sys.partition_schemes where function_id = @pf_id) and
             data_space_id in (select data_space_id from sys.filegroups where type = 'FD')

      if @fs_id is null
         raiserror('no FILESTREAM partition scheme exists for the partition function', 16, 1)
   end


   if @partition_column is null
      raiserror('a partitioning column must be specified', 16, 1)

   select @pcol_id  = c.column_id,
          @nullable = c.is_nullable,
          @pf_id    = pp.function_id,
          @stat     = case when c.is_computed = 1 and cc.is_persisted = 0
                           then 1 when c.is_sparse = 1 then 2 else 0
                      end
     from sys.columns c left join sys.computed_columns cc
       on cc.object_id = c.object_id and cc.column_id = c.column_id
     left join sys.partition_parameters pp
       on pp.function_id = @pf_id and pp.parameter_id = 1 and
          pp.system_type_id = c.system_type_id and
          pp.system_type_id = c.user_type_id and
          pp.max_length = c.max_length and
          pp.precision = c.precision and pp.scale = c.scale and
          (pp.collation_name is null and c.collation_name is null or
          pp.collation_name = c.collation_name)             
    where c.object_id = @source_tab_id and c.name = @partition_column

   if @pcol_id is null
      raiserror('a column with name ''%s'' does not exist', 16, 1, @partition_column)
   else if @pf_id is null
      raiserror('the column type is incompatible with the partition function', 16, 1)
   else if @stat = 1
      raiserror('the partitioning column must be a persisted computed column', 16, 1)
   else if @stat = 2
      raiserror('the partitioning column must not be a sparse column', 16, 1)
   else if exists (
      select * from sys.indexes i
       where object_id = @source_tab_id and is_unique = 1 and @pcol_id not in (
             select column_id from sys.index_columns
              where object_id = i.object_id and index_id = i.index_id and key_ordinal > 0))
      raiserror('the partitioning column must be a key column of all unique indexes', 16, 1)


   if @fg_name is not null 
   begin
      if not exists (select * from sys.filegroups where name = @fg_name and type = 'FG')
         raiserror('''%s'' is not a valid filegroup name', 16, 1, @fg_name)

      if isnull(@debug, 0) <> 2 and @fg_name not in (
         select name from sys.filegroups
          where is_read_only = 0 and data_space_id in (
                   select data_space_id from sys.database_files except
                   select data_space_id from sys.database_files where state <> 0))
         raiserror('the temp table cannot be created in filegroup ''%s''', 16, 1, @fg_name)
   end


   if @compression is null
      set @compr = null
   else
   begin
      set @compr = case lower(ltrim(rtrim(@compression)))
                        when N'none'                then 0
                        when N'row'                 then 1
                        when N'page'                then 2
                        when N'columnstore'         then 3
                        when N'columnstore_archive' then 4
                        else -1
                   end

      if @compr = -1 or @compr = 4 and @@microsoftversion < 0x0C000000 
         raiserror('''%s'' is not a valid compression value', 16, 1, @compression)
      else if @compr in (1, 2) and exists (
         select * from sys.columns where object_id = @source_tab_id and is_sparse = 1)
         raiserror('the table cannot be compressed due to sparse columns', 16, 1)
   end


   if isnull(@fast_mode, 1) = 1
   begin
      set @fast_mode_not_ok =
          case when @compression is not null then 1
               when @source_tab_id in (
                       select object_id from sys.indexes where data_space_id <> @in_fg)
                    then 2
               when @source_tab_id not in (
                       select object_id from sys.tables where lob_data_space_id in (0, @in_fg))
                    then 3
               when @source_tab_id in (select object_id from sys.columns where is_filestream = 1) and
                    @source_tab_id not in (
                       select object_id from sys.tables where filestream_data_space_id = @in_fs)
                    then 4
               when @source_tab_id in (select object_id from sys.indexes where type = 1)
                    then case when @pcol_id in (select column_id from sys.index_columns
                                                 where object_id = @source_tab_id and index_id = 1)
                              then 0 else 5
                         end
               when exists (select * from sys.indexes i
                             where object_id = @source_tab_id and type in (2, 6) and @pcol_id not in (
                                   select column_id from sys.index_columns
                                    where object_id = i.object_id and index_id = i.index_id))
                    then 6
               else 0
          end

      if @fast_mode_not_ok = 0
      begin
         set @check = ptf_internal.GetPartitionCheck (
                         @pf_id, @partition_column, @nullable, @in_part, null)

         if @check is not null and isnull(@debug, 0) <> 2
         begin
            set @stmt2 = N'select @cnt = count_big(*) from ' + @full_name +
                         N' where not (' + @check + N')'

            exec sp_executesql @stmt2, N'@cnt bigint output', @count_rows output
            if @count_rows <> 0 set @fast_mode_not_ok = 8
         end
      end

      if @fast_mode_not_ok = 0
         set @fast_mode = 1
      else if @fast_mode is null
         set @fast_mode = 0
      else
         raiserror('the requirements for fast mode are not met', 16, @fast_mode_not_ok)
   end

   if isnull(@debug, 0) <> 2 and exists (
      select * from sys.destination_data_spaces
       where partition_scheme_id in (@ps_id, @fs_id) and data_space_id not in ( 
                select data_space_id from sys.database_files except
                select data_space_id from sys.database_files where state <> 0 except
                select data_space_id from sys.filegroups where is_read_only = 1) and (
             @fast_mode = 1 and destination_id = @in_part or 
             @fast_mode = 0 and destination_id > @pstart and destination_id <= @num_part))
      raiserror('the partition scheme is mapped to a read-only filegroup', 16, 1)


   set @temp_tab = quotename(@tab_schema) + N'.' +
       quotename(left(@tab_name, 91) + N'_' + cast(newid() as nvarchar(36)))

   exec ptf.CreateTableClone @source_tab_id, @temp_tab, @switch_source = @fast_mode, @debug = @debug
   set @drop_temp_stmt = N'DROP TABLE ' + @temp_tab

   begin try
      exec ptf_internal.HandleVersioning @source_tab_id, 3,
           @pre_stmt output, @post_stmt output, @is_temporal output

      if @fast_mode = 1 and @check is not null and @is_temporal <> 1
      begin
         set @chk_name = quotename(left(@tab_name, 91) + N'_' + cast(newid() as nvarchar(36)))
         set @drop_chk = N'ALTER TABLE ' + @full_name
         set @stmt = @drop_chk + N' ADD CONSTRAINT ' + @chk_name + N' CHECK (' + @check + N')'

         exec ptf_internal.ExecStmt @stmt, @debug
         set @stmt = null
         set @drop_chk = @drop_chk + N' DROP CONSTRAINT ' + @chk_name
      end

      begin try
         exec ptf.CreateIndexesAndConstraints @source_tab_id, @temp_tab, @debug = @debug

         set @stmt = @pre_stmt + N'ALTER TABLE ' + @full_name + N' SWITCH TO ' + @temp_tab
         set @rev_switch =       N'ALTER TABLE ' + @temp_tab +  N' SWITCH TO ' + @full_name

         if @fast_mode = 0
         begin
            exec ptf_internal.ExecStmt @stmt, @debug

            set @stmt     = null
            set @part_tab = quotename(@tab_schema) + N'.' +
                            quotename(left(@tab_name, 91) + N'_' + cast(newid() as nvarchar(36)))
            begin try
               exec ptf.CreateTableClone @source_tab_id, @part_tab, @debug = @debug

               begin try
                  exec ptf.CreateIndexesAndConstraints @source_tab_id, @part_tab,
                       @part_scheme = @partition_scheme, @part_col_id = @pcol_id,
                       @compression = @compr, @compress_all = @compress_all, @debug = @debug

                  set @pnum = @pstart
                  while @pnum < @num_part
                  begin
                     set @pnum = @pnum + 1
                     exec ptf.CopyPartition @source_tab_id, @temp_tab, @part_tab, @pnum,
                          @ps_id, @pcol_id, @fg_name, @compr, @compress_all, @debug
                  end

                  if @is_temporal = 2
                  begin
                     set @stmt2 = 
                         N'select @sql = N''ALTER TABLE '' + @name + ' +
                         N'       N'' ADD PERIOD FOR SYSTEM_TIME ('' +' +
                         N'       quotename(col_name(object_id, start_column_id)) + N'', '' +' +
                         N'       quotename(col_name(object_id, end_column_id)) + N'')''' +
                         N'  from sys.periods where object_id = @tid'

                     exec sp_executesql @stmt2,
                          N'@tid int, @name nvarchar(256), @sql nvarchar(max) output',
                          @source_tab_id, @part_tab, @stmt output

                     exec ptf_internal.ExecStmt @stmt, @debug
                  end
               end try

               begin catch
                  set @stmt2 = N'DROP TABLE ' + @part_tab
                  if isnull(@debug, 0) <> 2 exec sp_executesql @stmt2
                  exec ptf_internal.HandleError
               end catch
            end try

            begin catch
               set @stmt2 = @rev_switch + N';' + @post_stmt
               if isnull(@debug, 0) <> 2 exec sp_executesql @stmt2
               exec ptf_internal.HandleError
            end catch
         end

         if @@trancount = 0
            begin transaction
         else
         begin
            set @svpt = N'PartitionTable_svpt'
            save transaction @svpt
         end

         begin try
            if @fast_mode = 1
               exec ptf_internal.ExecStmt @stmt, @debug

            set @stmt = null
            exec ptf.CreateIndexesAndConstraints @source_tab_id,
                 @part_scheme = @partition_scheme, @part_col_id = @pcol_id,
                 @compression = @compr, @compress_all = @compress_all,
                 @incremental_stats = @incremental_stats, @debug = @debug

            if @fast_mode = 0
            begin
               set @pnum = @pstart
               while @pnum < @num_part
               begin
                  set @pnum = @pnum + 1
                  set @stmt = N'ALTER TABLE ' + @part_tab +
                              N' SWITCH PARTITION ' + cast(@pnum as nvarchar) + N' TO ' +
                              @full_name + N' PARTITION ' + cast(@pnum as nvarchar)

                  exec ptf_internal.ExecStmt @stmt, @debug
               end
            end
            else
            begin
               if @check is not null and @is_temporal = 1
               begin
                  set @stmt = N'ALTER TABLE ' + @temp_tab + N' ADD CHECK (' + @check + N')'
                  exec ptf_internal.ExecStmt @stmt, @debug
               end

               set @stmt = @rev_switch + N' PARTITION ' + cast(@in_part as nvarchar)
               exec ptf_internal.ExecStmt @stmt, @debug
            end

            if @svpt is null
               commit transaction
         end try

         begin catch
            if @svpt is null
               rollback transaction
            else if xact_state() = 1
               rollback transaction @svpt

            if @fast_mode = 0 and isnull(@debug, 0) <> 2
            begin
               set @stmt2 = N'DROP TABLE ' + @part_tab
               exec sp_executesql @stmt2
               exec sp_executesql @rev_switch
               exec sp_executesql @post_stmt
            end

            exec ptf_internal.HandleError
         end catch

         if @fast_mode = 0
         begin
            set @stmt = N'DROP TABLE ' + @part_tab
            exec ptf_internal.ExecStmt @stmt, @debug
         end

         set @stmt = @post_stmt
         exec ptf_internal.ExecStmt @post_stmt, @debug
      end try

      begin catch
         if @drop_chk is not null and isnull(@debug, 0) <> 2
            exec sp_executesql @drop_chk
         exec ptf_internal.HandleError
      end catch

      set @stmt = @drop_chk
      exec ptf_internal.ExecStmt @drop_chk, @debug

      set @stmt = N'ALTER TABLE ' + @full_name + N' SET (LOCK_ESCALATION = AUTO)'
      exec ptf_internal.ExecStmt @stmt, @debug
   end try

   begin catch
      if isnull(@debug, 0) <> 2 exec sp_executesql @drop_temp_stmt
      exec ptf_internal.HandleError @debug, NULL, @stmt
   end catch

   exec ptf_internal.ExecStmt @drop_temp_stmt, @debug

   exec ptf_internal.Trace N'PartitionTable', 1, @debug
   return 0
end try

begin catch
   exec ptf_internal.HandleError @debug, N'PartitionTable'
   return 1
end catch
GO


CREATE PROCEDURE ptf.AddEmptyPartition 
   @part_table   nvarchar(256), -- name of the partitioned table
   @split_value  sql_variant,   -- the new boundary value for empty the partition
   @debug        int = 0
AS
set nocount on

declare @tab_id int, @src_tab nvarchar(256), @tmp_tab nvarchar(256)
declare @idx_id int, @pf_id int, @num_part int, @is_right bit
declare @part_num int, @num_rows bigint, @boundary sql_variant
declare @sv nvarchar(max), @svpt nvarchar(32)
declare @stmt nvarchar(1024), @stmt2 nvarchar(1024)

begin try
   exec ptf_internal.Trace N'AddEmptyPartition', 0, @debug, 2, @part_table, @split_value

   exec ptf_internal.GetObjId @part_table, 1, @tab_id output, @src_tab output

   if @tab_id is null
      raiserror('the table ''%s'' does not exist', 16, 1, @part_table)

   select @idx_id   = i.index_id,
          @pf_id    = ps.function_id,
          @num_part = pf.fanout,
          @is_right = pf.boundary_value_on_right,
          @part_num = p.partition_number,
          @num_rows = p.rows,
          @boundary = prv.value
     from sys.indexes i
     left join sys.partition_schemes ps on ps.data_space_id = i.data_space_id
     left join sys.partition_functions pf on pf.function_id = ps.function_id
     left join sys.partitions p
       on p.object_id = i.object_id and p.index_id = i.index_id and p.partition_number =
          case when pf.boundary_value_on_right = 0 then pf.fanout else 1 end
     left join sys.partition_range_values prv
       on prv.function_id = pf.function_id and prv.parameter_id = 1 and
          prv.boundary_id = p.partition_number + pf.boundary_value_on_right - 1
    where i.object_id = @tab_id and i.index_id in (0, 1)

   if @pf_id is null
      raiserror('the table ''%s'' is not partitioned', 16, 1, @part_table)
   else if @num_rows = 0
      raiserror('partition %d of table ''%s'' is already empty', 16, 1, @part_num, @part_table)

   if @split_value is null
      raiserror('a value for the new partition boundary must be specified', 16, 1)

   exec ptf_internal.CastVariant @pf_id, 1, @split_value output
   set @sv = ptf_internal.VariantToString(@split_value)

   if @num_part > 1 and (
      @is_right = 1 and (@boundary is null or @split_value >= @boundary) or
      @is_right = 0 and @boundary is not null and @split_value <= @boundary)
      raiserror('%s is not a valid split value', 16, 1, @sv)

   select @stmt2 = N'select @cnt = count_big(*) from ' + @src_tab +
                   N' with (tablock, repeatableread) where ' + quotename(c.name) +
                   case when @is_right = 0 then N' > '
                        else case when c.is_nullable = 0 then N''
                                  else N' is null or ' + quotename(c.name)
                             end + N' < '
                   end + @sv
     from sys.index_columns ic join sys.columns c
       on c.object_id = ic.object_id and c.column_id = ic.column_id
    where ic.object_id = @tab_id and ic.index_id = @idx_id and ic.partition_ordinal = 1


   if @@trancount = 0
      begin transaction
   else
   begin
      set @svpt = N'AddEmptyPartition_svpt'
      save transaction @svpt
   end

   begin try
      exec sp_executesql @stmt2, N'@cnt bigint output', @num_rows output

      if @num_rows <> 0
         raiserror('some rows of the table are outside of the new partition boundary', 16, 1)

      exec ptf_internal.HandleVersioning @tab_id, 3, @stmt output, @stmt2 output
      exec ptf_internal.ExecStmt @stmt, @debug

      select @stmt    = null,
             @tmp_tab = quotename(schema_name(schema_id)) + N'.' +
                        left(name, 91) + N'_' + cast(newid() as nvarchar(36))
        from sys.tables where object_id = @tab_id

      exec ptf.SwitchPartition2 @tab_id,
           @archive_table = @tmp_tab, @out_part = @part_num,
           @keep_indexes = 3, @no_merge = 1, @split_value = @split_value,
           @archive_mode = 1, @temp_mask = 0, @debug = @debug

      set @part_num = @part_num + @is_right

      exec ptf.SwitchPartition2 @tab_id,
           @staging_table = @tmp_tab, @in_part = @part_num, @debug = @debug

      set @stmt = @stmt2
      exec ptf_internal.ExecStmt @stmt, @debug

      if @svpt is null
         commit transaction
   end try

   begin catch
      if @svpt is null
         rollback transaction
      else if xact_state() = 1
         rollback transaction @svpt

      exec ptf_internal.HandleError @debug, null, @stmt
   end catch

   exec ptf_internal.Trace N'AddEmptyPartition', 1, @debug
   return 0
end try

begin catch
   exec ptf_internal.HandleError @debug, N'AddEmptyPartition'
   return 1
end catch
GO


CREATE VIEW ptf.PartitionDetails AS
select pf.name as partition_function, ps.name as partition_scheme,
       schema_name(o.schema_id) as schema_name, o.name as object_name,
       c.name as partitioning_column, i.name as index_name, i.index_id,
       prv.boundary_id as partition_number, p.rows as num_rows,
       p.data_compression_desc as compression,
       case when prv.value is not null
            then case when pf.boundary_value_on_right = 0
                      then case when prv.boundary_id < pf.fanout then '<=' else '>' end
                      else case when prv.boundary_id < pf.fanout then '<' else '>=' end
                 end
            when pf.fanout = 1 or pf.boundary_value_on_right <> 0 then ''
            when prv.boundary_id = 1 then '=' else '<>'
       end as rel,
       case when prv.value is not null then prv.value
            when pf.fanout = 1 or pf.boundary_value_on_right <> 0 then ''
            else 'NULL'
       end as boundary_value,
       fg.name as filegroup_name, fg.is_read_only,
       f.num_files, f.sum_size_MB, f.sum_used_MB
  from sys.partition_functions pf
  join (select function_id, boundary_id, value
          from sys.partition_range_values
         where parameter_id = 1
        union all
        select f.function_id, f.fanout, v.value
         from sys.partition_functions as f
         left join sys.partition_range_values v
           on v.function_id = f.function_id and v.parameter_id = 1 and
              v.boundary_id = f.fanout - 1
       ) as prv on prv.function_id = pf.function_id
  left join sys.partition_schemes ps on ps.function_id = pf.function_id
  left join sys.destination_data_spaces dds
    on dds.partition_scheme_id = ps.data_space_id and
       dds.destination_id = prv.boundary_id
  left join sys.filegroups fg on fg.data_space_id = dds.data_space_id
  left join (
       select data_space_id, count(*) as num_files,
              (sum(size) + 127) / 128 as sum_size_MB,
              (sum(fileproperty(name, 'SpaceUsed')) + 127) / 128 as sum_used_MB
         from sys.database_files
        group by data_space_id
       ) as f on f.data_space_id = fg.data_space_id
  left join sys.indexes i on i.data_space_id = ps.data_space_id
  left join sys.objects o on o.object_id = i.object_id
  left join sys.index_columns ic
    on ic.object_id = i.object_id and ic.index_id = i.index_id and
       ic.partition_ordinal = 1
  left join sys.columns c
    on c.object_id = ic.object_id and c.column_id = ic.column_id
  left join sys.partitions p
    on p.object_id = i.object_id and p.index_id = i.index_id and
       p.partition_number = dds.destination_id
GO

GRANT SELECT, REFERENCES ON ptf.PartitionDetails TO PUBLIC
GO

EXEC ptf.AddLoopbackLink

PRINT '================================================================'
PRINT ' PTF has been installed'
PRINT '================================================================'
