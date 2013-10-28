<?php
/**
 * Created by JetBrains PhpStorm.
 * User: seb
 * Date: 10/28/13
 * Time: 4:10 PM
 * To change this template use File | Settings | File Templates.
 */

namespace Cb2Mysql\Command;


use Cb2Mysql\ExportCouchbaseMysqlRaw;
use Cb2Mysql\Model\CustomCouchbaseModel;
use Cb2Mysql\Model\MysqlModel;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

use Cb2Mysql\ExportCouchbaseMysql;

/**
 * Class ExportRaw
 * @package Cb2Mysql\Command
 */
class ExportRaw extends ExportCouchbaseMysqlCommand
{
    protected function configure()
    {
        parent::configure();

        $this
            ->setName('export-couchbase-mysql-raw')
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
            )
         ->addOption(
        'cb-view-params',
        null,
        InputOption::VALUE_NONE,
        'view params'
        )
            ->addOption(
                'cb-view-offset',
                null,
                InputOption::VALUE_NONE,
                'cb view offset (skip)'
            )
            ->addOption(
                'cb-view-limit',
                null,
                InputOption::VALUE_NONE,
                'cb view limit '
            )

        ;
    }

    /**
     * Execute this command
     *
     * @param InputInterface $input
     * @param OutputInterface $output
     * @return int|null|void
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $this->parseInput($input);

        $cbViewParams = $input->getOption('cb-view-params');
        if(!is_array($cbViewParams)) {
            $cbViewParams = array();
        }
        $cbModel = new CustomCouchbaseModel(
            $input->getOption('cb-host'),
            $input->getOption('cb-bucket'),
            $input->getOption('cb-bucket-port'),
            $input->getOption('cb-user'),
            $input->getOption('cb-pass'),
            $input->getOption('cb-ignore-cluster'),
            $cbViewParams
        );

        $MysqlModel = new MysqlModel(
            'mysql:host='.$input->getOption('mysql-host').';dbname='.$input->getOption('mysql-db'),
            $input->getOption('mysql-user'),
            $input->getOption('mysql-password')
        );

        $instance = new ExportCouchbaseMysqlRaw($cbModel, $MysqlModel);
        if ($batchSize = $input->getOption('batch-size')) {
            $instance->setBatchSize($batchSize);
        }
        $cbViewLimit = (int)$input->getOption('cb-view-limit');
        if($cbViewLimit<1) {
            $cbViewLimit = 1000;
        }
        $cbViewOffset = (int)$input->getOption('cb-view-offset');
        if($cbViewOffset<1) {
            $cbViewOffset = 0;
        }

        $instance->setCbViewLimit($cbViewLimit);
        $instance->setCbViewOffset($cbViewOffset);

        try {
            $nExported = $instance->export(
                $input->getOption('cb-design'),
                $input->getOption('cb-view'),
                $input->getOption('mysql-table') ? $input->getOption('mysql-table') : $input->getOption('cb-view'),
                $input->getOption('truncate')
            );

            $output->writeln("Exported $nExported rows from couchbase into mysql");

        }catch(\Exception $e) {

            var_dump($e);

            //throw $e;


            $output->writeln($e->getMessage());

        }
    }
}