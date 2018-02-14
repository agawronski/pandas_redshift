## v 1.1.2

This release contains the following:
agawronski - A fix for the issue raised by drakemccabe. aws_session_token is optional and not required.

## v 1.1.1

This release contains the following:
pealco - The abilty to use a .pgpass file rather than an explicit password.
joshpeng - The ability to specify region (now necessary when copying from s3 to Redshift in different regions).
agawronski - The ability to pass an aws_session_token to boto3 when connecting to s3.
agawronski - A fix for the issue raised by alexisrosuel. Making the script properly stop running on error.


## v 1.1.0

This release contains the following:
dfernan - The ability to append to an existing table rather only overwrite.
