<?php
namespace Cb2Mysql;

use Cb2Mysql\model\CouchbaseModel;
use Cb2Mysql\model\MysqlModel;

class ExportCouchbaseMysql
{
    /**
     * @var CouchbaseModel
     */
    protected $cbModel;

    /**
     * @var MysqlModel
     */
    protected $mysqlModel;

    /**
     * @var int
     */
    protected $batchSize;

    public function __construct($cbModel, $mysqlModel, $batchSize=1000)
    {
        $this->setCbModel($cbModel);
        $this->setMysqlModel($mysqlModel);
        $this->setBatchSize($batchSize);
    }

    /**
     * @param int $batchSize
     * @throws \InvalidArgumentException
     */
    public function setBatchSize($batchSize)
    {
        if ($batchSize > 0)
            $this->batchSize = $batchSize;
        else
            throw new \InvalidArgumentException("Please supply a valid batch size");
    }

    /**
     * @return int
     */
    public function getBatchSize()
    {
        return $this->batchSize;
    }

    /**
     * @param \CouchbaseModel $cbModel
     */
    public function setCbModel($cbModel)
    {
        $this->cbModel = $cbModel;
    }

    /**
     * @return \CouchbaseModel
     */
    public function getCbModel()
    {
        return $this->cbModel;
    }

    /**
     * @param \MysqlModel $mysqlModel
     */
    public function setMysqlModel($mysqlModel)
    {
        $this->mysqlModel = $mysqlModel;
    }

    /**
     * @return \MysqlModel
     */
    public function getMysqlModel()
    {
        return $this->mysqlModel;
    }

    /**
     * export data from couchbase model into mysql model
     *
     * @param string $cbDesign
     * @param string $cbView
     * @param string $mysqlTable
     * @param bool $truncateTable
     * @return int
     */
    public function export($cbDesign, $cbView, $mysqlTable='', $truncateTable=false)
    {
        if (!$mysqlTable)
            $mysqlTable = $cbView;

        $mysqlModel = $this->mysqlModel;
        $mysqlModel->createTable($mysqlTable, $truncateTable);

        if($truncateTable)
            $mysqlModel->truncateTable($mysqlTable);

        $cbModel = $this->cbModel;

        // retrieve all keys from couchbase
        $allKeys = $cbModel->getKeys($cbDesign, $cbView);

        if (count($allKeys) == 0)
        {
            return 0;
        }

        // splice results using batch-size
        $offset = 0;
        while ($offset < count($allKeys)) {
            $keysSlice = array_slice($allKeys, $offset, $this->batchSize);

            // retrieve couchbase results, convert json to php array
            $results = $cbModel->getMulti($keysSlice);

            // retrieve a list of unique columns from the data
            $columns = array_reduce($results, function($v, $w) {
                    if (is_array($w))
                        $v = array_unique(array_merge($v, array_keys($w)));
                    return $v;
                }, array());

            // make sure all columns are added to the mysql table
            $mysqlModel->addColumns($mysqlTable, $columns);

            // insert data
            $inserted = 0;
            foreach ($results as $id => $row) {
                $mysqlModel->addRow($mysqlTable, $row);
                $inserted++;
            }

            $offset += $this->batchSize;
        }

        return $inserted;
    }
}