USE DATABASE UTIL;
USE SCHEMA PUBLIC;

CREATE OR REPLACE SECRET tableau_creds
    TYPE = password
    USERNAME = '<replace-me>'
    PASSWORD = '<replace-me>';
