# migrationReport
This repository contains the tools required for someone to verify the correctness of a database migration for docker images in postgresql.

# Usage
1. `cd src` - Change into the source directory.
2. Create a csv of (at least) 2 databases whos rows correspond with:


`<user>,<database>,<password>,<docker_image>`


Each line must be valid and contain all of the above content.
3. `python3 MigrationReport.py` - Run the main database verification.
4. Reports will be written into the top level `reports` directory.


# Notes
General Outline / Brainstorming:


https://docs.google.com/document/d/1CyDupJ1XRwUHPTVWDCyCdqBmpvQlgkAmDezFPfhoG3A/edit?usp=sharing


Plan for scripting:


https://www.postgresqltutorial.com/postgresql-python/connect/


We save on 1 level of sending if we construct the report in the host. 
Although, the system wont be completely OS agnostic (which is part of the point of using docker containers)it can be adapted if need be.

# PSQL Documentation:
`\dt` - Displays tables


`\du` - Displays users


`psql -h <Host> -d <Database Name> -U <User> -W` - W indicates the login requires a password.


`https://www.postgresql.org/docs/8.1/queries-limit.html` - Details the select, limit, and order by for postgres.


Mac Users: Local PSQL


`brew services start postgresql`


`brew services stop postgresql`


# Netcat Commands:
These are really helpful if you have trouble setting up your DB due to port-in-use errors:

Using netcat:
`nc -zv <IP Address> <Port>` - Checks if there is something listening on this port and with what protocol.


Same thing but using netstat:
`netstat -vanp tcp | grep 3000`

# Docker Commands:
`docker ps` - show all running docker containers.


`docker ps -a` - shows all containers, regardless of running status.


`docker containers prune` - delete all stopped containers.


Docker run documentation:
https://docs.docker.com/engine/reference/commandline/run/o


Docker SDK for Python:
https://docker-py.readthedocs.io/en/stable/

# Psycopg2 Links:
Connect:
https://www.postgresqltutorial.com/postgresql-python/connect/


Cursor DB Management:
www.psycopg.org/docs/cursor.html
