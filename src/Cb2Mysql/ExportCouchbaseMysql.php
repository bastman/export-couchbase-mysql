<?php
namespace Cb2Mysql;

use Cb2Mysql\model\CouchbaseModel;
use Cb2Mysql\model\PDOModel;

class ExportCouchbaseMysql
{
    /**
     * @var CouchbaseModel
     */
    protected $cbModel;

    /**
     * @var PDOModel
     */
    protected $PDOModel;

    /**
     * @var int
     */
    protected $batchSize;

    public function __construct($cbModel, $PDOModel, $batchSize=1000)
    {
        $this->setCbModel($cbModel);
        $this->setPDOModel($PDOModel);
        $this->setBatchSize($batchSize);
    }

    /**
     * @param int $batchSize
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
     * @param \PDOModel $PDOModel
     */
    public function setPDOModel($PDOModel)
    {
        $this->PDOModel = $PDOModel;
    }

    /**
     * @return \PDOModel
     */
    public function getPDOModel()
    {
        return $this->PDOModel;
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

        $PDOModel = $this->PDOModel;
        $PDOModel->setTable($mysqlTable, $truncateTable);

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
            $PDOModel->addColumns($columns);

            // insert data
            $inserted = 0;
            foreach ($results as $id => $row) {
                $PDOModel->addRow($row);
                $inserted++;
            }

            $offset += $this->batchSize;
        }

        return $inserted;
    }
}