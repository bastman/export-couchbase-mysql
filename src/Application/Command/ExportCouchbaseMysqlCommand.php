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

    /** @var array mysql config  */
    protected $mysqlConfig;

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
                'config-file',
                null,
                InputOption::VALUE_REQUIRED,
                'file containing configuration in JSON'
            )
            ->addOption(
                'cb-host',
                null,
                InputOption::VALUE_REQUIRED,
                'The couchbase host'
            )
            ->addOption(
                'cb-bucket',
                null,
                InputOption::VALUE_REQUIRED,
                "The couchbase bucket, defaults to 'default'"
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
                'The couchbase design'
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
                "The mysql host"
            )
            ->addOption(
                'mysql-db',
                null,
                InputOption::VALUE_REQUIRED,
                'The mysql database'
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
                'The mysql table, defaults to the name of the couchbase view'
            )
            ->addOption(
                'truncate',
                null,
                InputOption::VALUE_NONE,
                '[Optional] truncate all records from the mysql table automatically without asking'
            )
            ->addOption(
                'batch-size',
                null,
                InputOption::VALUE_REQUIRED,
                '[Optional] Amount of documents to retrieve from couchbase in one batch'
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

            if (!class_exists('Couchbase'))
                throw new \ErrorException("Could not create an instance of the 'Couchbase' class, did you install the PHP 'couchbase' extension yet?");

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
     * @param OutputInterface $output
     */
    protected function parseInput($input)
    {
        if ($configFile = $input->getOption('config-file')) {
            if (!is_file($configFile))
                throw new \ErrorException('Could not read config file');

            $config = json_decode(file_get_contents($configFile), true);

            if (!is_array($config))
                throw new \ErrorException('Config is not valid JSON');

            foreach ($config as $key => $value) {
                $input->setOption($key, $value);
            }
        }

        $this->cbConfig = array(
            'host' => $input->getOption('cb-host'),
            'bucket' => $input->getOption('cb-bucket') ? $input->getOption('cb-bucket') : 'default',
            'user' => $input->getOption('cb-user'),
            'pass' => $input->getOption('cb-pass'),
            'design' => $input->getOption('cb-design'),
            'view' => $input->getOption('cb-view'),
        );

        if (!$this->cbConfig['host'])
            throw new \ErrorException('Please supply the couchbase host (--cb-host)');

        if (!$this->cbConfig['design'])
            throw new \ErrorException('Please supply the couchbase design (--cb-design)');

        if (!$this->cbConfig['view'])
            throw new \ErrorException('Please supply the couchbase view (--cb-view)');

        $this->mysqlConfig = array(
            'host' => $input->getOption('mysql-host'),
            'db' => $input->getOption('mysql-db'),
            'user' => $input->getOption('mysql-user'),
            'pass' => $input->getOption('mysql-password'),
            'table' => $input->getOption('mysql-table') ? $input->getOption('mysql-table') : $this->cbConfig['view'],
        );

        if (!$this->mysqlConfig['host'])
            throw new \ErrorException('Please supply the mysql host (--mysql-host)');

        if (!$this->mysqlConfig['db'])
            throw new \ErrorException('Please supply the mysql database (--mysql-db)');
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
        try {
            $db = new \PDO('mysql:host='.$mysqlConfig['host'].';dbname='.$mysqlConfig['db'],$mysqlConfig['user'],$mysqlConfig['pass']);
            $db->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        } catch (\Exception $ex) {
            throw new \ErrorException('Could not connect to mysql database');
        }

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

        if (!$allKeys)
        {
            $output->writeln('The view did not contain any documents');
            exit();
        }

        $output->writeln('Found a total of '.count($allKeys).' documents');

        // splice results using batch-size
        $offset = 0;
        $batchSize = $input->getOption('batch-size') ? $input->getOption('batch-size') : 1000;
        while ($offset < count($allKeys)) {
            $keys = array_slice($allKeys, $offset, $batchSize);

            // retrieve couchbase results, convert json to php array
            $results = $this->getMulti($keys);

            $output->writeln('Retrieved '.count($keys).' documents from couchbase');

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
            $inserted = 0;
            foreach ($results as $id => $row) {
                $binds = array_map(function($key){return ":$key";}, array_keys($row));
                $stmt = $db->prepare('insert into '.$mysqlConfig['table'].'('.implode(',',array_keys($row)).') VALUES ('.implode(',', $binds).')');
                foreach ($row as $key => $value)
                    $stmt->bindValue(":$key", $value);
                $inserted += $stmt->execute();
            }

            $output->writeln('Inserted '.$inserted.' rows into mysql table '.$mysqlConfig['table']);

            $offset += $batchSize;
        }
    }
}