**Project Description**
The Partitioned Table Framework (PTF) consists of a set of T-SQL Procedures that ease the maintenance work associated with partitioned tables in Microsoft SQL Server.

This project previously had been published at https://ptf.codeplex.com/

PTF supports the following Microsoft SQL Server versions:
* SQL Server 2008 (R2)
* SQL Server 2012
* SQL Server 2014
* SQL Server 2016
* SQL Server 2017
* Azure SQL Database V12


The PTF project consists of the following files:
* install_ptf.sql - PTF Installation script
* uninstall_ptf.sql - script for uninstallation of all PTF database objects
* PTF.docx - PTF documentation and reference

To install PTF run the installation script in each SQL Server database that contains partitioned tables you would like to be managed by PTF, e.g.
**sqlcmd.exe** /S <SQL Server instance> /E /d <database name> /i install_ptf.sql
