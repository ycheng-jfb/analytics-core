CREATE OR REPLACE FUNCTION REPORTING_PROD.SXF.UDF_REMOVE_REPEATED(S VARCHAR)
    returns VARCHAR
    language PYTHON
    runtime_version = '3.11'
    handler = 'remove_repeated'
as
$$
def remove_repeated (s):
    l = s.split('|')
    x = set(a.strip() for a in l)
    return ' | '.join(x)
$$;

CREATE OR REPLACE FUNCTION REPORTING_PROD.SXF.UDF_REMOVE_REPEATED(S VARCHAR, D VARCHAR)
    returns VARCHAR
    language PYTHON
    runtime_version = '3.11'
    handler = 'remove_repeated'
as
$$
def remove_repeated (s,d):
     l = s.split(''+d+'')
     x = set(a.strip() for a in l)
     if len(l) == len(x):
         return s
     else:
        return ' | '.join(x)
$$;
