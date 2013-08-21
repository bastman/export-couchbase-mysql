<?php
namespace Application\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Application\Lib;

/**
 * Class ExportCouchbaseMysqlCustomCommand
 *
 * This class creates a custom couchbase client instance which uses memcache & json to retrieve the data from couchbase
 *
 * @package Application\Command
 */
class ExportCouchbaseMysqlCustomCommand extends ExportCouchbaseMysqlCommand
{
    private $customCb;

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
                'Connect to given node only, ignore the rest of the cluster'
            );
    }

    /**
     * return a custom client instance
     *
     * @return Lib\Couchbase\ClientJson
     */
    protected function getCustomCb()
    {
        if (!$this->customCb) {
            $cbConfig = $this->cbConfig;

            $this->customCb = new Lib\Couchbase\ClientJson();
            $this->customCb->setHost($cbConfig['host']);
            $this->customCb->setPort($cbConfig['bucket-port']);
            $this->customCb->setCurrentBucketName($cbConfig['bucket']);
            $this->customCb->setClusterNodesEnabled($cbConfig['ignore-cluster'] ? false : true);
            $this->customCb->init();
        }

        return $this->customCb;
    }

    /**
     * parse extra params
     * @param InputInterface $input
     */
    protected function parseInput($input)
    {
        parent::parseInput($input);

        $this->cbConfig = array_merge($this->cbConfig, array(
            'bucket-port' => $input->getOption('cb-bucket-port'),
            'ignore-cluster' => $input->getOption('cb-ignore-cluster')
        ));
    }

    /**
     * retrieve keys using our custom client instance
     * @return array keys
     */
    protected function getKeys()
    {
        $cbConfig = $this->cbConfig;

        $view = $this->getCustomCb()->fetchView($cbConfig['bucket'], $cbConfig['design'], $cbConfig['view'], null);

        return array_map(function($row) {
                return $row['id'];
            }, $view['rows']);
    }

    /**
     * retrieve documents using our custom client instance
     *
     * @param array of $keys
     * @return array of documents
     */
    protected function getMulti($keys)
    {
        return $this->getcustomCb()->fetchMultiKeys($keys);
    }
}