export-couchbase-mysql
======================

These commands allows you to extract couchbase documents from a view and import them into a mysql table.

The table and all necessary columns will be created automatically (just TEXT columns at the moment).

To allow for the export of large amounts of documents the batch-size option is set to 1000 documents by default.

Usage:

```
# example/console.php <command> <options>

The export-couchbase-mysql command retrieves data using the native couchbase API and supports the following options:

 --config-file     file containing configuration in JSON
 --cb-host         The couchbase host to connect to
 --cb-bucket       The couchbase bucket, defaults to 'default'
 --cb-user         The couchbase user
 --cb-pass         The couchbase password
 --cb-design       The couchbase design to use
 --cb-view         The couchbase view to retieve data from
 --mysql-host      The mysql host host to connect to
 --mysql-db        The mysql database to use
 --mysql-user      The mysql user to use
 --mysql-password  The mysql password to use
 --mysql-table     The mysql table, defaults to the name of the couchbase view
 --truncate        [Optional] truncate all records from the mysql table automatically without asking
 --batch-size      [Optional] Amount of documents to retrieve from couchbase in one batch

 * All options are required unless noted otherwise.

The export-couchbase-mysql-custom command retrieves couchbase data using our custom memcache/json driver and supports the following options:

 --config-file        file containing configuration in JSON
 --cb-host            The couchbase host
 --cb-bucket          The couchbase bucket, defaults to 'default'
 --cb-user            The couchbase user
 --cb-pass            The couchbase password
 --cb-design          The couchbase design
 --cb-view            The couchbase view to retieve data from
 --mysql-host         The mysql host
 --mysql-db           The mysql database
 --mysql-user         The mysql user
 --mysql-password     The mysql password
 --mysql-table        The mysql table, defaults to the name of the couchbase view
 --truncate           [Optional] truncate all records from the mysql table automatically without asking
 --batch-size         [Optional] Amount of documents to retrieve from couchbase in one batch
 --cb-bucket-port     The couchbase bucket port for connecting the memcache driver
 --cb-ignore-cluster  Connect to given node only, ignore the rest of the cluster (useful when other nodes are firewalled)

 * All options are required unless noted otherwise.
 ```