<?php
namespace Cb2Mysql\Model;

/**
 * Class CouchbaseModel
 * @package Cb2Mysql\Model
 */
class CouchbaseModel
{
    /**
     * @var Couchbase
     */
    protected $nativeInstance;

    /**
     * @var string
     */
    protected $host;

    /**
     * @var string
     */
    protected $bucket;

    /**
     * @var string
     */
    protected $user;

    /**
     * @var string
     */
    protected $password;

    /**
     * @param $host
     * @param $bucket
     * @param string $user
     * @param string $password
     */
    public function __construct($host, $bucket, $user='', $password='')
    {
        $this->setHost($host);
        $this->setBucket($bucket);
        $this->setUser($user);
        $this->setPassword($password);
    }

    /**
     * get native couchbase instance
     * @throws \ErrorException
     * @return Couchbase
     */
    protected function getNativeInstance()
    {
        if (!$this->nativeInstance)
        {
            if (!class_exists('Couchbase'))
                throw new \ErrorException("Could not create an instance of the 'Couchbase' class, did you install the PHP 'couchbase' extension yet?");

            // connect to couchbase
            $this->nativeInstance = new \Couchbase($this->host, $this->user, $this->password, $this->bucket);
        }

        return $this->nativeInstance;
    }

    /**
     * @param mixed $host
     * @throws \InvalidArgumentException
     */
    protected function setHost($host)
    {
        if ($host)
            $this->host = $host;
        else
            throw new \InvalidArgumentException('Please supply a valid couchbase host');
    }

    /**
     * @return mixed
     */
    public function getHost()
    {
        return $this->host;
    }

    /**
     * @param mixed $bucket
     * @throws \InvalidArgumentException
     */
    protected function setBucket($bucket)
    {
        if ($bucket)
            $this->bucket = $bucket;
        else
            throw new \InvalidArgumentException("Please supply the couchbase bucket");
    }

    /**
     * @return mixed
     */
    public function getBucket()
    {
        return $this->bucket;
    }

    /**
     * @param string $user
     */
    protected function setUser($user)
    {
        $this->user = $user;
    }

    /**
     * @return string
     */
    public function getUser()
    {
        return $this->user;
    }

    /**
     * @param string $password
     */
    protected function setPassword($password)
    {
        $this->password = $password;
    }

    /**
     * @return string
     */
    public function getPassword()
    {
        return $this->password;
    }

    /***
     * retrieve list of couchbase keys from a view
     * @param $design
     * @param $view
     * @return array of keys
     */
    public function getKeys($design, $view)
    {
        // throws exception if view does not exists
        $view = $this->getNativeInstance()->view($design, $view);

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
        $data = $this->getNativeInstance()->getMulti($keys);

        return array_map(function($json) {
                return json_decode($json, true);
            }, $data);
    }
}