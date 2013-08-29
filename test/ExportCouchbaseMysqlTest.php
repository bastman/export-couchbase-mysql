<?php
/**
 * Created by JetBrains PhpStorm.
 * User: wouter
 * Date: 8/29/13
 * Time: 2:30 PM
 * To change this template use File | Settings | File Templates.
 */

use Cb2Mysql\ExportCouchbaseMysql;

class ExportCouchbaseMysqlTest extends PHPUnit_Framework_TestCase
{
    /**
     * @var ExportCouchbaseMysql
     */
    private $exportCouchbaseMysql;

    /**
     * @var array
     */
    private $sampleData;

    public function setUp()
    {
        $this->sampleData = array();
        foreach (range(1, 1000) as $id)
            $this->sampleData[$id] = array('id' => $id, 'field' => 'value');

        $cbModel = Phake::mock('Cb2Mysql\model\CouchbaseModel');

        Phake::when($cbModel)->getKeys(Phake::anyParameters())->thenReturn(array_keys($this->sampleData));
        Phake::when($cbModel)->getMulti(Phake::anyParameters())->thenReturn($this->sampleData);

        $mysqlModel = Phake::mock('Cb2Mysql\model\MysqlModel');

        $this->exportCouchbaseMysql = new ExportCouchbaseMysql($cbModel, $mysqlModel);
    }

    /**
     * test empty batchsize throwing exception
     *
     * @expectedException InvalidArgumentException
     */
    public function testEmptyBucket()
    {
        $this->exportCouchbaseMysql->setBatchSize(0);
    }

    /**
     * test export returns amount of records saved
     */
    public function testExport()
    {
        $this->assertEquals($this->exportCouchbaseMysql->export('design', 'view'), count($this->sampleData));
    }
}
