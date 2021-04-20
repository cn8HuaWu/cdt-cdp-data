#!/bin/bash
PGHOST=${1:?must run this script with gp host parameter}
PGPORT=${2:?must run this script with gp port parameter}
PGUSER=${3:?must run this script with gp user parameter}
PGPASSWORD=${4:?must run this script with gp password parameter}
PGDB=${5:?must run this script with gp db name parameter}

export PGHOST=${PGHOST}
export PGPORT=${PGPORT}
export PGUSER=${PGUSER}
export PGPASSWORD=${PGPASSWORD}

#tablelist=`psql -d ${PGDB} -c "copy ( select schemaname||'.'||relname from pg_catalog.pg_stat_all_tables where schemaname in ('dm','edw')  ) to stdout"`
tablelist=`psql -h ${PGHOST} -p ${PGPORT} -U etl_user -d ${PGDB} -c "copy ( select schemaname||'.'||relname from pg_catalog.pg_stat_all_tables where schemaname in ('dm','edw')  ) to stdout"`
for tab in $tablelist ; do
    echo processing $tab
    psql -d $PGDB -e -a -c "VACUUM ANALYZE ${tab};"
done
