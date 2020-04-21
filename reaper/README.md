# esi2ddl
Generates Postgres tables from ESI swagger specification
## Build
` gradle clean build`
## Usage
`java -jar ./build/libs/esi2ddl-1.0-SNAPSHOT.jar -h`
```shell
usage: [-d] [-v] [-h] -c <connection-url> [-e <esi-swagger>] [-s <schema>] [-u <username>] [-p <password>]
Generates Postgres tables from ESI swagger specification
 -c,--connection-url <arg>   Postgres database url string
 -d,--dry-run                Parses swagger but skips db step3UploadDdl
 -e,--esi-swagger <arg>      ESI swagger specification URL (default
                             https://esi.evetech.net/latest/swagger.json?datasource=tranquility)
 -h,--help                   Print command line argument help and usage
 -p,--password <arg>         Postgres database password
 -s,--schema <arg>           Postgres database schema (default public)
 -u,--username <arg>         Postgres database username (default postgres)
 -r,--roles <arg>            Print role grants. Arg is 2 coma separated role names: auth,public 
 -v,--verbose                Produces additional output lines
```

## Command Line Example
`java -jar ./build/libs/esi2ddl-1.0-SNAPSHOT.jar -c jdbc:postgresql://localhost/ESI -s public`

