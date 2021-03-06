<?php
namespace Cb2Mysql;

use Cb2Mysql\Model\CouchbaseModel;
use Cb2Mysql\Model\MysqlModel;

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

    /**
     * @param CouchbaseModel $cbModel
     * @param MysqlModel $mysqlModel
     * @param int $batchSize
     */
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
     * @param CouchbaseModel $cbModel
     */
    public function setCbModel($cbModel)
    {
        $this->cbModel = $cbModel;
    }

    /**
     * @return CouchbaseModel
     */
    public function getCbModel()
    {
        return $this->cbModel;
    }

    /**
     * @param MysqlModel $mysqlModel
     */
    public function setMysqlModel($mysqlModel)
    {
        $this->mysqlModel = $mysqlModel;
    }

    /**
     * @return MysqlModel
     */
    public function getMysqlModel()
    {
        return $this->mysqlModel;
    }

    /**
     * export data from couchbase model into mysql model
     * @param string $cbDesign
     * @param string $cbView
     * @param string $mysqlTable
     * @param bool $truncateTable
     * @return int
     * @throws \Exception
     */
    public function export($cbDesign, $cbView, $mysqlTable='', $truncateTable=false)
    {
        if (!$mysqlTable) {
            $mysqlTable = $cbView;
        }


        $mysqlModel = $this->mysqlModel;
        try {
            $mysqlModel->createTable($mysqlTable, $truncateTable);

        }catch(\Exception $e) {

            throw new \Exception(
                'mysql.createTable failed! '.$e->getMessage()
            );

        }

        if($truncateTable) {
            try {
                $mysqlModel->truncateTable($mysqlTable);
            }catch(\Exception $e) {

                throw new \Exception(
                    'mysql.truncateTable failed! '.$e->getMessage()
                );

            }

        }


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
            try {

                $mysqlModel->addColumns($mysqlTable, $columns);

            }catch (\Exception $e) {

                throw new \Exception(
                    'mysql.addColumns failed! '
                    .$e->getMessage()
                    .' table='.json_encode($mysqlTable)
                    .' columns='.json_encode($columns)

                );
            }

            // insert data
            $inserted = 0;
            foreach ($results as $id => $row) {

                try {
                    $mysqlModel->addRow($mysqlTable, $row);

                }catch(\Exception $e) {

                    throw new \Exception(
                        'mysql.addRow failed! '.$e->getMessage()
                    );
                }

                $inserted++;
            }

            $offset += $this->batchSize;
        }

        return $inserted;
    }
}