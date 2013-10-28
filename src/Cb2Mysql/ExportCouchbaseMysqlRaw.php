<?php
/**
 * Created by JetBrains PhpStorm.
 * User: seb
 * Date: 10/28/13
 * Time: 4:13 PM
 * To change this template use File | Settings | File Templates.
 */

namespace Cb2Mysql;
use Cb2Mysql\Model\CouchbaseModel;
use Cb2Mysql\Model\MysqlModel;

/**
 * Class ExportCouchbaseMysqlRaw
 * @package Cb2Mysql
 */
class ExportCouchbaseMysqlRaw
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
        $cbLimit = $this->getCbViewLimit();
        $cbOffset = $this->getCbViewOffset();

        echo 'start export: '.$cbDesign.'/'.$cbView.' offset='.$cbOffset.' limit: '.$cbLimit.PHP_EOL;



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


        $columns = array(
            'cbType',
            'cbCreated',
            'cbModified',

            'cbValue',
            'cbValueHash',
            'createdInSql',
            'modifiedInSql',
        );
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



        // retrieve all keys from couchbase
        $cbProcessedCountTotal = 0;



        $processedCount = $this->cbPage($cbDesign, $cbView, $cbLimit, $cbOffset, $mysqlModel, $mysqlTable);
        $cbProcessedCountTotal+=$processedCount;
        while($processedCount>0) {
            $cbOffset+=$processedCount;

            $processedCount = $this->cbPage($cbDesign, $cbView, $cbLimit, $cbOffset, $mysqlModel, $mysqlTable);
            $cbProcessedCountTotal+=$processedCount;
        }

        echo 'cb keys processed: '
            .$cbProcessedCountTotal.' '.PHP_EOL
            .' inserted to mysql: '.$this->mysqlInsertCount.' '.PHP_EOL;

        return $this->mysqlInsertCount;
    }


    protected function cbPage(
        $cbDesign,
        $cbView,
        $cbLimit,
        $cbOffset,
        $mysqlModel,
        $mysqlTable
    )
    {

        echo 'fetch view: '.$cbDesign.'/'.$cbView.' limit: '.$cbLimit.' offset: '.$cbOffset. ' ...'.PHP_EOL;


        $processedCount = 0;

        $cbModel = $this->cbModel;
        $allKeys = $cbModel->getKeys($cbDesign, $cbView, $cbLimit, $cbOffset);

        if (count($allKeys) == 0)
        {
            return 0;
        }

        // splice results using batch-size
        $offset = 0;
        $lastSuccessfulCbKey = null;
        while ($offset < count($allKeys)) {
            $keysSlice = array_slice($allKeys, $offset, $this->batchSize);

            // retrieve couchbase results, convert json to php array
            $results = $cbModel->getMulti($keysSlice);

            // insert data


            foreach ($results as $id => $cbValue) {


                if($processedCount===0) {
                    echo 'cb key: '.$id.PHP_EOL;
                    echo ' more to come ...';
                }

                $processedCount++;

                $cbType = '';
                $cbCreated=0;
                $cbModified=0;
                if(is_array($cbValue)) {

                    if(isset($cbValue['type'])) {
                        $cbType = (string)$cbValue['type'];
                    }
                    if(isset($cbValue['created'])) {
                        $cbCreated = (string)$cbValue['created'];
                    }
                    if(isset($cbValue['modified'])) {
                        $cbModified = (string)$cbValue['modified'];
                    }
                }

                $cbValueText = json_encode($cbValue);
                $cbValueHash = md5($cbValueText);

                $currentTimestamp = time();

                $row = array(
                    'id'=>$id,
                    'cbValue'=>$cbValueText,
                    'cbValueHash'=>$cbValueHash,
                    'cbType'=>$cbType,
                    'cbCreated'=>$cbCreated,
                    'cbModified'=>$cbModified,
                    'createdInSql'=>$currentTimestamp,
                    'modifiedInSql'=>$currentTimestamp,
                );

                try {
                    //$mysqlModel->addRow()
                    // $rowFetched = $mysqlModel->getRowById($mysqlTable, $id);
                    $mysqlModel->addRow($mysqlTable, $row);

                }catch(\Exception $e) {

                    throw new \Exception(
                        'mysql.addRow failed! for cbKey='.$id.' ! '
                        .' lastSuccessful cb key: '.$lastSuccessfulCbKey
                        .$e->getMessage()
                    );
                }

                $this->mysqlInsertCount++;
                $lastSuccessfulCbKey = $id;

            }

            $offset += $this->batchSize;
        }

        return $processedCount;
    }

    /**
     * @var int
     */
    private $mysqlInsertCount= 0;

    /**
     * @var int
     */
    private $cbViewLimit = -1;
    /**
     * @var int
     */
    private $cbViewOffset = 0;

    /**
     * @param int $value
     * @return $this
     */
    public function setCbViewLimit($value)
    {
        $this->cbViewLimit = (int)$value;

        return $this;
    }

    /**
     * @return int
     */
    public function getCbViewLimit()
    {
        return (int)$this->cbViewLimit;

    }

    /**
     * @param int $value
     * @return $this
     */
    public function setCbViewOffset($value)
    {
        $this->cbViewOffset = (int)$value;

        return $this;
    }

    /**
     * @return int
     */
    public function getCbViewOffset()
    {
        return (int)$this->cbViewOffset;

    }

}