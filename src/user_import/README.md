# User import

This script imports users from on-premise active directory into a ODM2 postgres database.

The reason for importing these users, is that the 3 letter niva usernames are not accessible in azure active directory (AD). Azure AD uses UserPrincipalName which is the user's email address.

The script inserts into the following tables:

- people
- affiliations
- personexternalidentifiers

## Running

### Active directory dump

1. Run this on a NIVA windows machine on the NIVA network:

```
Get-ADUser -Filter * -SearchBase "OU=OSLO,OU=NIVA,DC=niva,DC=corp" |
Select UserPrincipalName, GivenName, Surname, SamAccountName |
Export-CSV -Delimiter ";" -Encoding UTF8 -Path
"c:\niva\port\utils\data\emails.csv"
```

The list of users should be in a csv file with the following format:

```
#TYPE Selected.Microsoft.ActiveDirectory.Management.ADUser
"UserPrincipalName";"GivenName";"Surname";"SamAccountName"
"Firstname.Lastname@niva.no";"Firstname Middlename";"Lastname";"FML"
```

2. send the file to my linux machine 
3. on linux  I ran the following to fix encodings:

```
dos2unix data/emails.csv
```

### Setup environment

The script assumes access to a postgres instance. It is configured with the following environment variables (default in parentheses):

- TIMESCALE_ODM2_SERVICE_HOST (localhost)
- TIMESCALE_ODM2_SERVICE_PORT (8700)
- ODM2_DB_USER (postgres)
- ODM2_DB_PASSWORD (postgres)
- ODM2_DB (niva_odm2)

The data could either be imported to a local development environment or a port-forwarded postgres instance running in test/prod.

### Import users

assuming a working python installation with dependencies installed. If not:

```
# assuming that you are in this directory
pip install -r ../../requirements.txt
```

Run script:

```
python import_users.py
```
