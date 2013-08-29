<?php
/**
 * Created by JetBrains PhpStorm.
 * User: wouter
 * Date: 8/29/13
 * Time: 2:25 PM
 * To change this template use File | Settings | File Templates.
 */

use Cb2Mysql\model\CouchbaseModel;

class CouchbaseModelTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var CouchbaseModel
     */
    private $cbModel;

    /**
     * test empty host throwing exception
     *
     * @expectedException InvalidArgumentException
     */
    public function testMissingHost()
    {
        $this->cbModel = new CouchbaseModel('','');
    }

    /**
     * test empty bucket throwing exception
     *
     * @expectedException InvalidArgumentException
     */
    public function testMissingBucket()
    {
        $this->cbModel = new CouchbaseModel('localhost','');
    }
}
