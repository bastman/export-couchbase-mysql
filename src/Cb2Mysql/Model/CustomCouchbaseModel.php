<?php
namespace Cb2Mysql\Model;

use Cb2Mysql\Couchbase\ClientJson;

/**
 * Class CustomCouchbaseModel
 * @package Cb2Mysql\Model
 */
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
    public function __construct(
        $host, $bucket, $bucketPort, $user='', $password='', $ignoreCluster=false,
        $viewParams
    )
    {
        parent::__construct($host, $bucket, $user, $password);

        $this->setBucketPort($bucketPort);
        $this->setIgnoreCluster($ignoreCluster);
        $this->setViewParams($viewParams);
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
    public function getKeys($design, $view, $limit, $offset )
    {
        $viewParams = $this->getViewParams();
        $isValid = is_int($offset) && ($offset>=0);
        if($isValid) {
            $viewParams['skip']=$offset;
        }
        $isValid = is_int($limit) && ($limit>0);
        if($isValid) {
            $viewParams['limit']=$limit;
        }

        $view = $this->getClientJson()->fetchView($this->bucket, $design, $view, $viewParams);

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

    /**
     * @var array
     */
    protected $viewParams = array();

    /**
     * @param array|null $value
     * @return $this
     * @throws \Exception
     *
     */
    public function setViewParams($value)
    {
        if($value===null) {
            $value = array();
        }

        if(!is_array($value)) {

            throw new \Exception('Invalid value! '.__METHOD__);
        }

        $this->viewParams = $value;

        return $this;
    }

    /**
     * @return array
     */
    public function getViewParams()
    {
        $value = $this->viewParams;
        if(!is_array($value)) {

            return array();
        }

        return $value;
    }

}