## Quick Start

This section describes how to run YCSB on SQL-database using JDBC with JSON support.

### 1. Start SQL database

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl com.yahoo.ycsb:core,com.yahoo.ycsb:jdbc-json-binding clean package

### 4. Provide JDBC Connection Parameters
    
Set the DB driver, url, user and password in the
workload you plan to run.

- `db.driver`
- `db.url`
- `db.user`
- `db.passwd`

Or, you can set configs with the shell command, EG:

    ./bin/ycsb load jdbc-json -s -P workloads/workloada -p "db.driver=org.postgresql.Driver" -p "db.url=jdbc:postgresql://SERVER_NAME:5432/DB_NAME" -p "db.user=myuser" -p "db.passwd=12345" > outputLoad.txt

### 5. Load data and run tests

Load the data:

    ./bin/ycsb load jdbc-json -s -P workloads/workloada > outputLoad.txt

Run the workload test:

    ./bin/ycsb run jdbc-json -s -P workloads/workloada > outputRun.txt
    
