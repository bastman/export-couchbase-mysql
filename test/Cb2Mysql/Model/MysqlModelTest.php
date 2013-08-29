<?php
/**
 * Created by JetBrains PhpStorm.
 * User: wouter
 * Date: 8/28/13
 * Time: 11:14 AM
 * To change this template use File | Settings | File Templates.
 */

use Cb2Mysql\model\MysqlModel;

class MysqlModelTest extends PHPUnit_Framework_TestCase
{
    /**
     * @var MysqlModel
     */
    public $mysqlModel;

    const DEFAULT_COL = 'id';
    const TEST_TABLE = 'test_table';
    const TEST_COL = 'TEST_COL';

    /**
     * setup partial MysqlModel mock with PDO mocks
     */
    public function setUp()
    {
        $this->mysqlModel = Phake::partialMock('Cb2Mysql\model\MysqlModel', 'localhost');

        Phake::when($this->mysqlModel)->describeTable(self::TEST_TABLE)->thenReturn(array(self::DEFAULT_COL));

        $pdoMock = Phake::mock('PDO');
        Phake::when($pdoMock)->prepare(Phake::anyParameters())->thenReturn(Phake::mock('PDOStatement'));
        Phake::when($this->mysqlModel)->getPDOInstance()->thenReturn($pdoMock);
    }

    /**
     * test creation of empty table shows an exception
     *
     * @expectedException InvalidArgumentException
     */
    public function testInvalidTable()
    {
        $this->mysqlModel->createTable('');
    }

    /**
     * test adding of default column when creating table
     */
    public function testDefaultColumn()
    {
        $this->mysqlModel->createTable(self::TEST_TABLE);

        $columns = $this->mysqlModel->getColumns(self::TEST_TABLE);

        $this->assertNotEmpty($columns);
        $this->assertTrue(in_array(self::DEFAULT_COL, $columns));
    }

    /**
     * test adding of column
     */
    public function testAddColumn()
    {
        $this->mysqlModel->addColumns(self::TEST_TABLE, array(self::TEST_COL));

        $columns = $this->mysqlModel->getColumns(self::TEST_TABLE);

        $this->assertNotEmpty($columns);
        $this->assertTrue(in_array(self::TEST_COL, $columns), 'columns should contain test column');
    }

    /**
     * test adding of row automatically adding column
     *
     * @depends testAddColumn
     */
    public function testAddColumnFromRow()
    {
        $this->mysqlModel->addRow(self::TEST_TABLE, array(
            self::TEST_COL => 'value'
        ));

        $columns = $this->mysqlModel->getColumns(self::TEST_TABLE);

        $this->assertNotEmpty($columns);
        $this->assertTrue(in_array(self::TEST_COL, $columns), 'columns should contain test column');
    }
}