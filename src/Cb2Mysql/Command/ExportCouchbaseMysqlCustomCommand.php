<?php
namespace Cb2Mysql\Command;

use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

use Cb2Mysql\model\MysqlModel;
use Cb2Mysql\model\CustomCouchbaseModel;
use Cb2Mysql\ExportCouchbaseMysql;

/**
 * Class ExportCouchbaseMysqlCustomCommand
 *
 * This command exports the data using the custom couchbase model
 *
 * @package Application\Command
 */
class ExportCouchbaseMysqlCustomCommand extends ExportCouchbaseMysqlCommand
{
    protected function configure()
    {
        parent::configure();

        $this
            ->setName('export-couchbase-mysql-custom')
            ->setDescription('Export data from a couchbase view to a mysql table using custom couchbase client')
            ->addOption(
                'cb-bucket-port',
                null,
                InputOption::VALUE_REQUIRED,
                'The couchbase bucket port for connecting the memcache driver'
            )
            ->addOption(
                'cb-ignore-cluster',
                null,
                InputOption::VALUE_NONE,
                'Connect to given node only, ignore the rest of the cluster (useful when other nodes are firewalled)'
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

        $cbModel = new CustomCouchbaseModel(
            $input->getOption('cb-host'),
            $input->getOption('cb-bucket'),
            $input->getOption('cb-bucket-port'),
            $input->getOption('cb-user'),
            $input->getOption('cb-pass'),
            $input->getOption('cb-ignore-cluster')
        );

        $mysqlModel = new MysqlModel(
            $input->getOption('mysql-host'),
            $input->getOption('mysql-db'),
            $input->getOption('mysql-user') ? '':'',
            $input->getOption('mysql-password') ? '':''
        );

        $instance = new ExportCouchbaseMysql($cbModel, $mysqlModel);
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