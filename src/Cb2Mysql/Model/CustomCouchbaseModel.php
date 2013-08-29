<?php
namespace Cb2Mysql\model;

use Cb2Mysql\Couchbase\ClientJson;

class CustomCouchbaseModel extends CouchbaseModel
{
    /**
     * @var int
     */
    protected $bucketPort;

    /**
     * @var bool
     */
    protected $ignoreCluster;

    /**
     * @var string
     */
    protected $clientJson;

    /**
     * @param $host
     * @param $bucket
     * @param string $bucketPort
     * @param string $user
     * @param string $password
     * @param bool $ignoreCluster
     */
    public function __construct($host, $bucket, $bucketPort, $user='', $password='', $ignoreCluster=false)
    {
        parent::__construct($host, $bucket, $user, $password);

        $this->setBucketPort($bucketPort);
        $this->setIgnoreCluster($ignoreCluster);
    }

    /**
     * @param int $bucketPort
     * @throws \InvalidArgumentException
     */
    protected function setBucketPort($bucketPort)
    {
        if ($bucketPort > 0)
            $this->bucketPort = $bucketPort;
        else
            throw new \InvalidArgumentException ("Please specify a valid bucket port");
    }

    /**
     * @return int
     */
    public function getBucketPort()
    {
        return $this->bucketPort;
    }

    /**
     * @param boolean $ignoreCluster
     */
    protected function setIgnoreCluster($ignoreCluster)
    {
        $this->ignoreCluster = $ignoreCluster;
    }

    /**
     * @return boolean
     */
    public function getIgnoreCluster()
    {
        return $this->ignoreCluster;
    }

    /**
    /**
     * @return ClientJson
     */
    protected function getClientJson()
    {
        if (!$this->clientJson) {

            $this->clientJson = new ClientJson();
            $this->clientJson->setHost($this->host);
            $this->clientJson->setPort($this->bucketPort);
            $this->clientJson->setCurrentBucketName($this->bucket);
            $this->clientJson->setClusterNodesEnabled(!$this->ignoreCluster);
            $this->clientJson->init();
        }

        return $this->clientJson;
    }

    /***
     * retrieve list of couchbase keys from a view
     * @param $design
     * @param $view
     * @return array of keys
     */
    public function getKeys($design, $view)
    {
        $view = $this->getClientJson()->fetchView($this->bucket, $design, $view, null);

        return array_map(function($row) {
                return $row['id'];
            }, $view['rows']);
    }

    /**
     * @param $keys of the documents we want to get
     * @return array of documents
     */
    public function getMulti($keys)
    {
        return $this->getClientJson()->fetchMultiKeys($keys);
    }
}