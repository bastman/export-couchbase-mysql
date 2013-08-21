<?php
namespace Application\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Application\Lib;

class ExportCouchbaseMysqlCommand extends Command
{
    /** @var array couchbase config  */
    protected $cbConfig;

    /** @var array database config  */
    protected $dbConfig;

    /** @var instance of native couchbase */
    private $nativeCb;

    /**
     * configure this command
     */
    protected function configure()
    {
        $this
            ->setName('export-couchbase-mysql')
            ->setDescription('Export data from a couchbase view to a mysql table')
            ->addOption(
                'cb-host',
                null,
                InputOption::VALUE_REQUIRED,
                'The couchbase host to connect to'
            )
            ->addOption(
                'cb-bucket',
                null,
                InputOption::VALUE_REQUIRED,
                'The couchbase bucket'
            )
            ->addOption(
                'cb-user',
                null,
                InputOption::VALUE_REQUIRED,
                'The couchbase user'
            )
            ->addOption(
                'cb-pass',
                null,
                InputOption::VALUE_REQUIRED,
                'The couchbase password'
            )
            ->addOption(
                'cb-design',
                null,
                InputOption::VALUE_REQUIRED,
                'The couchbase design to use'
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
                'The mysql host host to connect to'
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
            )
            ->addOption(
                'pageSize',
                null,
                InputOption::VALUE_REQUIRED,
                'Amount of documents to retrieve from couchbase in one batch'
            );
    }

    /**
     * return a native couchbase instance
     *
     * @return instance|\Couchbase
     */
    protected function getNativeCb()
    {
        if (!$this->nativeCb)
        {
            $cbConfig = $this->cbConfig;

            // connect to couchbase
            $this->nativeCb = new \Couchbase($cbConfig['host'], $cbConfig['user'], $cbConfig['pass'], $cbConfig['bucket']);
        }

        return $this->nativeCb;
    }

    /***
     * retrieve list of couchbase keys we want to export from couchbase into mysql
     * @return array of keys
     */
    protected function getKeys()
    {
        $cbConfig = $this->cbConfig;

        // throws exception if view does not exists
        $view = $this->getNativeCb()->view($cbConfig['design'], $cbConfig['view']);

        return array_map(function($row) {
                return $row['id'];
            }, $view['rows']);
    }

    /**
     * @param $keys of the documents we want to get
     * @return array of documents
     */
    protected function getMulti($keys)
    {
        $data = $this->getNativeCb()->getMulti($keys);

        return array_map(function($json) {
                return json_decode($json, true);
            }, $data);
    }

    /**
     * parse input parameters into couchbase & database configuration
     * @param InputInterface $input
     */
    protected function parseInput($input)
    {
        $this->cbConfig = array(
            'host' => $input->getOption('cb-host'),
            'bucket' => $input->getOption('cb-bucket') ? $input->getOption('cb-bucket') : 'default',
            'user' => $input->getOption('cb-user'),
            'pass' => $input->getOption('cb-pass'),
            'design' => $input->getOption('cb-design'),
            'view' => $input->getOption('cb-view'),
        );

        $this->mysqlConfig = array(
            'host' => $input->getOption('mysql-host') ? $input->getOption('mysql-host') : 'localhost',
            'db' => $input->getOption('mysql-db'),
            'user' => $input->getOption('mysql-user'),
            'pass' => $input->getOption('mysql-password'),
            'table' => $input->getOption('mysql-table') ? $input->getOption('mysql-table') : $this->cbConfig['view'],
        );
    }

    /**
     * Execute this command
     *
     * @param InputInterface $input
     * @param OutputInterface $output
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $this->parseInput($input);

        $mysqlConfig = $this->mysqlConfig;

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

        // retrieve all keys from couchbase
        $allKeys = $this->getKeys();

        $output->writeln('Found a total of '.count($allKeys).' documents');

        // splice results using pagesize
        $offset = 0;
        $pageSize = $input->getOption('pageSize') ? $input->getOption('pageSize') : 1000;
        while ($offset < count($allKeys)) {
            $keys = array_slice($allKeys, $offset, $pageSize);
            //var_dump($keys);die();

            // retrieve couchbase results, convert json to php array
                $results = $this->getMulti($keys);


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
                $dbColumns[] = $column;
            }
            $dbColumns = array_unique($dbColumns);

            // insert data
            foreach ($results as $id => $row) {
                $binds = array_map(function($key){return ":$key";}, array_keys($row));
                $stmt = $db->prepare('insert into '.$mysqlConfig['table'].'('.implode(',',array_keys($row)).') VALUES ('.implode(',', $binds).')');
                foreach ($row as $key => $value)
                    $stmt->bindValue(":$key", $value);
                $stmt->execute();
            }

            $output->writeln('Inserted '.count($results).' rows into mysql table '.$mysqlConfig['table']);

            $offset += $pageSize;
        }
    }
}