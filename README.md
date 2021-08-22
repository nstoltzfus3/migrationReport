# migrationReport
This repository contains the tools required for someone to verify the correctness of a database migration for docker images in postgresql.

General Outline / Brainstorming:
https://docs.google.com/document/d/1CyDupJ1XRwUHPTVWDCyCdqBmpvQlgkAmDezFPfhoG3A/edit?usp=sharing

Plan for scripting:
https://www.postgresqltutorial.com/postgresql-python/connect/

# Notes:
We save on 1 level of sending if we construct the report in the host. 
Although, the system wont be completely OS agnostic (which is part of the point of using docker containers)it can be adapted if need be.

# PSQL Documentation:
`\dt` - Displays tables
`\du` - Displays users

`psql -U <user> -W <password> -`

# Netcat Commands
These are really helpful if you have trouble setting up your DB due to port-in-use errors.

`nc -zv <IP Address> <Port>` - Checks if there is something listening on this port and with what protocol.

