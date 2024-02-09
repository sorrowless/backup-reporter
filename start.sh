#!/bin/bash
set -e
WAITING_COUNT=0
STATUS="health: starting"
while [[ ${STATUS} = "health: starting" ]] && [[ "100" -gt "WAITING_COUNT" ]] 
do
  echo "Still waiting"
  let WAITING_COUNT=WAITING_COUNT+1
  sleep 10
  STATUS=`docker ps --format "{{.Status}}"  | awk -F '[()]' '{print $2}'`
done
if [[ ${STATUS} = "healthy" ]]
then
  docker ps
  if [[ -z `docker exec -i postgres psql -d postgres -Atqc "\list ${PGDATABASE}"` ]]
  then
    echo "Database ${PGDATABASE} is absent"
    docker-compose down
    exit 1
  else
    echo "Database ${PGDATABASE} is present"
  fi
  if [[ `docker exec -i postgres psql -Aqtc 'SELECT pg_database_size(current_database());'` -gt 10000000 ]]
  then
    echo "Size is bigger than 10 MB"
  else
    echo "Size is less than 10 MB"
    docker-compose down
    exit 1
  fi
  if [[ `docker exec -i postgres psql -Aqtc "SELECT count (*) table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema','pg_catalog');"` -gt 3 ]]
  then
    echo "The number of tables is more than 3"
  else
    echo "The number of tables is less than 3"
    docker-compose down
    exit 1
  fi
else
  docker ps -a
  docker logs postgres
  docker inspect postgres
  docker-compose down
  df -h
  du -sh ${PGDATA}
  exit 1
fi
DATE_LAST_BACKUP=$(docker exec -i postgres wal-g backup-list | tail -n 1 | awk '{print $2}' | awk -F "T" '{print $1}')
DIFF=$(( ($(date -d $(date +%Y%m%d) +%s) - $(date -d $DATE_LAST_BACKUP +%s) )/(60*60*24) ))
if [[ "$DIFF" -gt "2" ]]
then
  echo "Backup is older than $DIFF !!! Please, check production of backups!"
  exit 1
fi
echo $(docker exec -i postgres wal-g backup-list | tail -n 1 | awk '{print $1}') > VALIDATED_BACKUP
docker-compose down
