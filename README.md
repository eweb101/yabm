### Overview
Back up all databases on a mysql server using mysqldump, gzip the backup file, then copy that file to s3.

### Environment variables

AWS_ACCESS_KEY_ID<br>
AWS_SECRET_ACCESS_KEY<br>
AWS_DEFAULT_REGION<br>
AWS_URL<br>
MYSQL_PWD<br>
MYSQL_USER<br>
MYSQL_HOST<br>
SLEEP_DURATION //How much time between backups in seconds. Default is 86400<br>
BACKUPS_TO_KEEP //How many backup files to keep on s3.<br>
SLACK_URL //Optional slack webhook url.<br>
