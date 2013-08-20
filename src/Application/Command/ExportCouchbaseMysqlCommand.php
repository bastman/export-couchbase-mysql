<?php
namespace Application\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class ExportCouchbaseMysqlCommand extends Command
{
    protected function configure()
    {
        $this
            ->setName('export-couchbase-mysql')
            ->setDescription('Export data from a couchbase view to a mysql table')
            ->addOption(
                'cb-server',
                null,
                InputOption::VALUE_REQUIRED,
                'The couchbase server to connect to'
            )
            ->addOption(
                'cb-doc',
                null,
                InputOption::VALUE_REQUIRED,
                'The couchbase document to use'
            )
            ->addOption(
                'cb-view',
                null,
                InputOption::VALUE_REQUIRED,
                'The couchbase view to retieve data from'
            )
            ->addOption(
                'mysql-host',
                null,
                InputOption::VALUE_REQUIRED,
                'The mysql server host to connect to'
            )
            ->addOption(
                'mysql-db',
                null,
                InputOption::VALUE_REQUIRED,
                'The mysql database name to connect to'
            )
            ->addOption(
                'mysql-user',
                null,
                InputOption::VALUE_REQUIRED,
                'The mysql user'
            )
            ->addOption(
                'mysql-password',
                null,
                InputOption::VALUE_REQUIRED,
                'The mysql password'
            )
            ->addOption(
                'mysql-table',
                null,
                InputOption::VALUE_REQUIRED,
                'The mysql password'
            )
            ->addOption(
                'truncate',
                null,
                InputOption::VALUE_NONE,
                'Truncate table '
            );
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $cbConfig = array(
            'server' => $input->getOption('cb-server'),
            'document' => $input->getOption('cb-doc'),
            'view' => $input->getOption('cb-view')
        );

        // connect to couchbase
        $cb = new \Couchbase($cbConfig['server'], "", "", "default");

        // throw exception if view does not exists
        $view = $cb->view($cbConfig['document'], $cbConfig['view']);

        $mysqlConfig = array(
            'host' => $input->getOption('mysql-host') ? $input->getOption('mysql-host') : 'localhost',
            'db' => $input->getOption('mysql-db'),
            'user' => $input->getOption('mysql-user'),
            'pass' => $input->getOption('mysql-password'),
            'table' => $input->getOption('mysql-table') ? $input->getOption('mysql-table') : $cbConfig['view'],
        );

        // connect to mysql
        $db = new \PDO('mysql:host='.$mysqlConfig['host'].';dbname='.$mysqlConfig['db'],$mysqlConfig['user'],$mysqlConfig['pass']);
        $db->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        // check mysql table & structure
        try {
            $table = $db->query('describe '.$mysqlConfig['table'])->fetchAll();
            $dbColumns = array_map(function($row){
                    return $row['Field'];
                }, $table);

            // truncate data
            $dialog = $this->getHelperSet()->get('dialog');
            if ($input->getOption('truncate') || $dialog->askConfirmation($output, '<question>Table already exists, truncate?</question>',false)) {
                $db->query('truncate '.$mysqlConfig['table']);
            }
        } catch (\Exception $ex) {
            $output->writeln('Creating mysql table '.$mysqlConfig['table']);
            $db->query('create table '.$mysqlConfig['table']. '(id varchar(255))');
            $dbColumns = array('id');
        }

        // retrieve keys from couchbase view
        $keys = array_map(function($row) {
                return $row['id'];
            }, $view['rows']);

        $output->writeln("Retrieved ".count($keys).' keys from couchbase view');

        // retrieve couchbase results, convert json to php array
        $results = array_map(function($json) {
                return json_decode($json, true);
            }, $cb->getMulti($keys));

        // retrieve a list of unique columns from the data
        $cbColumns = array_reduce($results, function($v, $w) {
                if (is_array($w))
                    $v = array_unique(array_merge($v, array_keys($w)));
                return $v;
            }, array());

        // create new columns if needed
        foreach (array_diff($cbColumns, $dbColumns) as $column) {
            $output->writeln("Adding mysql table column ".$column.' TEXT');
            $db->exec('alter table '.$mysqlConfig['table'].' add column '.$column.' TEXT');
        }

        // insert data
        foreach ($results as $id => $row) {
            $binds = array_map(function($key){return ":$key";}, array_keys($row));
            $stmt = $db->prepare('insert into '.$mysqlConfig['table'].'('.implode(',',array_keys($row)).') VALUES ('.implode(',', $binds).')');
            foreach ($row as $key => $value)
                $stmt->bindValue(":$key", $value);
            $stmt->execute();
        }

        $output->writeln('Inserted '.count($results).' rows into mysql table '.$mysqlConfig['table']);
    }
}