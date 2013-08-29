<?php
namespace Cb2Mysql\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

use Cb2Mysql\model\PDOModel;
use Cb2Mysql\model\CouchbaseModel;
use Cb2Mysql\ExportCouchbaseMysql;

class ExportCouchbaseMysqlCommand extends Command
{
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
     * parse input parameters, loading config file if needed
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

        $cbModel = new CouchbaseModel(
            $input->getOption('cb-host'),
            $input->getOption('cb-bucket'),
            $input->getOption('cb-user'),
            $input->getOption('cb-pass')
        );

        $PDOModel = new PDOModel(
            'mysql:host='.$input->getOption('mysql-host').';dbname='.$input->getOption('mysql-db'),
            $input->getOption('mysql-user'),
            $input->getOption('mysql-password')
        );

        $instance = new ExportCouchbaseMysql($cbModel, $PDOModel);
        if ($batchSize = $input->getOption('batch-size'))
            $instance->setBatchSize($batchSize);

        $nExported = $instance->export(
            $input->getOption('cb-design'),
            $input->getOption('cb-view'),
            $input->getOption('mysql-table') ? $input->getOption('mysql-table') : $input->getOption('cb-view'),
            $input->getOption('truncate')
        );

        $output->writeln("Exported $nExported rows from couchbase into mysql");
    }
}