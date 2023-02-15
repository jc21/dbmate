-- Create some roles that we can test the connection string with
CREATE ROLE dbadmin WITH SUPERUSER;
CREATE ROLE dbrole WITH LOGIN PASSWORD 'dbrole';
GRANT dbadmin TO dbrole;
