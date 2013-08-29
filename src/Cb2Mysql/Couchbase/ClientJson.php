<?php
/**
 * Created by JetBrains PhpStorm.
 * User: seb
 * Date: 4/23/13
 * Time: 2:15 PM
 * To change this template use File | Settings | File Templates.
 */
namespace Cb2Mysql\Couchbase;

use Cb2Mysql\Utils\Id;

class ClientJson
{

    /**
     * @var \Memcached
     */
    protected $memcachedClient;

    /**
     * @var ViewHelper
     */
    protected $viewHelper;

    /**
     * @var
     */
    protected $host = '';

    /**
     * @var
     */
    protected $port;

    /**
     * @var bool
     */
    private $isInitialized = false;

    /**
     * @var string
     */
    protected $currentBucketName = '';

    /**
     * @var bool
     */
    protected $clusterNodesEnabled;

    /**
     * @var array|null
     */
    protected $clusterInfo;

    /**
     * @var array
     */
    private $historyLog = array();


    /**
     * @return bool
     */
    public function getIsInitialized()
    {

        return ($this->isInitialized === true);
    }

    /**
     * @param bool $preferredValue
     * @param $errorDetailsText
     * @return $this
     * @throws \Exception
     */
    private function requireInitialized($preferredValue, $errorDetailsText)
    {
        $result = $this;
        $givenValue = ($this->getIsInitialized() === true);

        if (!is_bool($preferredValue)) {

            throw new \Exception(
                'Parameter "isInitialized must be bool". '
                . __METHOD__ . ''
                . get_class($this)
                . ' '
                . $errorDetailsText
            );
        }
        if ($preferredValue !== $givenValue) {

            throw new \Exception(
                'Client.isInitialized=' . json_encode($givenValue)
                . ' , but expected to be initialized='
                . json_encode($preferredValue) . ' !'
                . ' ' . __METHOD__ . ''
                . ' ' . get_class($this)
                . ' ' . $errorDetailsText
            );
        }

        return $result;
    }

    /**
     * @param bool $enabled
     * @return $this
     */
    public function setClusterNodesEnabled($enabled)
    {
        $this->clusterNodesEnabled = ($enabled === true);

        return $this;
    }

    /**
     * @return bool
     */
    public function getClusterNodesEnabled()
    {
        return ($this->clusterNodesEnabled == true);
    }


    /**
     * @param string $name
     * @return self
     * @throws \Exception
     */
    public function setCurrentBucketName($name)
    {
        $isValid = (is_string($name) && (!empty($name)));
        if (!$isValid) {

            throw new \Exception('Invalid parameter "name".');
        }

        $this->currentBucketName = $name;

        return $this;
    }


    /**
     * @param $host
     * @return self
     * @throws \Exception
     */
    public function setHost($host)
    {
        $isValid = (is_string($host) && (!empty($host)));
        if (!$isValid) {

            throw new \Exception('Invalid parameter "host".');
        }
        $this->requireInitialized(false, __METHOD__);
        $this->host = $host;

        return $this;
    }

    /**
     * @param int $port
     * @return $this
     * @throws \Exception
     */
    public function setPort($port)
    {
        $result = $this;
        $port = Id::castUnsignedInt($port, -1);
        if ($port < 1) {

            throw new \Exception('Invalid parameter "port".');
        }
        $this->requireInitialized(false, __METHOD__);
        $this->port = $port;

        return $result;
    }


    /**
     * @return int
     */
    public function getPort()
    {
        return (int)$this->port;
    }

    /**
     * @return string
     */
    public function getCurrentBucketName()
    {
        return (string)$this->currentBucketName;
    }

    /**
     * @return string
     */
    public function getHost()
    {
        return (string)$this->host;
    }

    /**
     * @return \Memcached
     */
    public function getMemcachedClient()
    {
        return $this->memcachedClient;
    }

    /**
     * @return ViewHelper
     */
    public function createViewHelper()
    {
        $this->requireInitialized(true, __METHOD__);

        $viewHelper = new ViewHelper();
        $viewHelper->setHost($this->getHost());

        return $viewHelper;
    }

    /**
     * @return ViewHelper
     */
    public function getViewHelper()
    {
        $this->requireInitialized(true, __METHOD__);
        if (!$this->viewHelper) {
            $this->viewHelper = $this->createViewHelper();
        }

        return $this->viewHelper;
    }


    /**
     * @param array $item
     * @return ClientJson
     */
    private function addHistoryLog($item)
    {
        $this->historyLog[] = $item;

        return $this;
    }

    /**
     * @return array
     */
    public function getHistoryLog()
    {
        return (array)$this->historyLog;
    }

    /**
     * @return self
     */
    public function unsetHistoryLog()
    {
        $this->historyLog = array();

        return $this;
    }

    /**
     * @param string $key
     * @param $value
     * @return $this
     */
    public function setOption($key, $value)
    {
        $this->getMemcachedClient()->setOption($key, $value);

        return $this;
    }

    /**
     * @return $this
     */
    public function init()
    {
        $this->initClient();

        return $this;
    }

    /**
     * @return ClientJson
     * @throws \Exception
     */
    protected function initClient()
    {
        $result = $this;

        if ($this->isInitialized) {

            throw new \Exception('Client already initialized.');
        }
        $host = $this->getHost();
        $isValid = ((is_string($host)) && (!empty($host)));
        if (!$isValid) {

            throw new \Exception('Invalid property "host".');
        }
        $port = $this->getPort();
        $port = Id::castUnsignedInt($port, 0);
        if ($port < 1) {

            throw new \Exception('Invalid property "port".');
        }

        $this->memcachedClient = new \Memcached();

        $this->setOption(\Memcached::OPT_COMPRESSION, false);
        $this->setOption(\Memcached::OPT_CONNECT_TIMEOUT, 500);
        $this->setOption(\Memcached::OPT_TCP_NODELAY, true);
        $this->setOption(\Memcached::OPT_NO_BLOCK, true);
        $this->setOption(\Memcached::OPT_POLL_TIMEOUT, 500);

        $this->isInitialized = true;
        // fetch cluster nodes and add memcached servers
        if ($this->getClusterNodesEnabled()) {
            $clusterInfo = $this->getClusterInfo();
            $nodesInfo = $clusterInfo['nodes'];

            foreach ($nodesInfo as $node) {
                $splitted = explode(":", $node['hostname']);

                $this->getMemcachedClient()->addServer($splitted[0], $port);
            }
        } else {
            $this->getMemcachedClient()->addServer($host, $port);
        }

        return $result;
    }

    /**
     * @param bool $delegateExceptions
     * @return bool
     * @throws \Exception
     */
    public function testClusterInfoIsAvailable($delegateExceptions)
    {
        $this->requireInitialized(true, __METHOD__);
        $delegateExceptions = ($delegateExceptions === true);

        $result = false;

        try {
            $clusterInfo = $this->fetchClusterInfo();

            return is_array($clusterInfo);
        } catch (\Exception $e) {
            if ($delegateExceptions) {
                $exception = new \Exception(
                    'Failed to fetch CB ClusterInfo. details: '
                    . $e->getMessage()
                );

                throw $exception;
            }
        }

        return $result;
    }

    /**
     * @return array
     * @throws \Exception
     */
    public function fetchClusterInfo()
    {
        $this->requireInitialized(true, __METHOD__);
        $clusterInfo = json_decode(
            file_get_contents(
                "http://" . $this->getHost() . ":8091/pools/default"
            ),
            true
        );

        if (!is_array($clusterInfo)) {

            throw new \Exception('Fetch ClusterInfo failed');
        }

        return $clusterInfo;
    }

    /**
     * @return array|null
     * @throws \Exception
     */
    public function getClusterInfo()
    {
        if (!is_array($this->clusterInfo)) {
            $clusterInfo = $this->fetchClusterInfo();
            $this->clusterInfo = $clusterInfo;
            if (!is_array($clusterInfo)) {

                throw new \Exception('Fetch ClusterInfo failed');
            }
        }

        return $this->clusterInfo;
    }

    /**
     * @return array
     */
    public function getServerList()
    {
        return (array)$this->getMemcachedClient()->getServerList();
    }

    /**
     * @return array
     */
    public function getStats()
    {
        $this->requireInitialized(true, __METHOD__);

        return $this->getMemcachedClient()->getStats();
    }


    /**
     * @param string $host
     * @param string $port
     * @param int $weight
     *
     * @return $this
     * @throws \Exception
     */
    public function addServer($host, $port, $weight)
    {
        $result = $this;

        $isValid = (is_string($host) && (!empty($host)));
        if (!$isValid) {

            throw new \Exception('Invalid parameter "host".');
        }
        $port = Id::castUnsignedInt($port, -1);
        if ($port < 1) {

            throw new \Exception('Invalid parameter "port".');
        }
        $weight = Id::castUnsignedInt($weight, -1);
        if ($weight < 0) {

            throw new \Exception('Invalid parameter "weight".');
        }

        $this->getMemcachedClient()->addServer($host, $port, $weight);

        return $result;
    }


    /**
     * @param string $bucket
     * @param string $design
     * @param string $view
     * @param array|null $params
     * @return array
     * @throws \Exception
     */
    public function fetchView($bucket, $design, $view, $params)
    {
        $this->addHistoryLog(
            array(
                'method' => __METHOD__,
                'args' => (array)func_get_args()
            )
        );

        $this->requireInitialized(true, __METHOD__);

        $isValid = ((is_string($bucket)) && (!empty($bucket)));
        if (!$isValid) {

            throw new \Exception('Invalid parameter "bucket".');
        }
        $isValid = ((is_string($design)) && (!empty($design)));
        if (!$isValid) {

            throw new \Exception('Invalid parameter "design".');
        }
        $isValid = ((is_string($view)) && (!empty($view)));
        if (!$isValid) {

            throw new \Exception('Invalid parameter "view".');
        }
        if ($params === null) {
            $params = array();
        }
        $isValid = (is_array($params));
        if (!$isValid) {

            throw new \Exception('Invalid parameter "params".');
        }

        $result = $this->getViewHelper()->setBucketName($bucket)
            ->setDesign($design)
            ->setView($view)
            ->fetchView($params);

        if (!is_array($result)) {

            throw new \Exception('Fetch View Failed. ' . json_encode(
                array(
                    'view' => $view,
                    'design' => $design,
                    'bucket' => $bucket
                )
            ));
        }

        return $result;
    }

    /**
     * @param string $design
     * @param string $view
     * @param array|null $params
     * @return array|null
     * @throws \Exception
     */
    public function fetchViewFromCurrentBucket($design, $view, $params)
    {
        $bucket = $this->getCurrentBucketName();
        $result = $this->fetchView($bucket, $design, $view, $params);

        return $result;
    }


    // ----------------------------------------------

    /**
     * @param string $key
     * @throws \Exception
     * @return null|array|string|mixed
     */
    public function fetchKey($key)
    {
        $this->addHistoryLog(
            array(
                'method' => __METHOD__,
                'args' => (array)func_get_args()
            )
        );

        $result = null;

        $isValid = (is_string($key) && (!empty($key)));
        if (!$isValid) {

            throw new \Exception('Invalid parameter "key".');
        }

        $this->requireInitialized(true, __METHOD__);
        $memcachedResult = $this->getMemcachedClient()->get($key);
        if ($memcachedResult === false) {
            // key does not ex. or server down ?

            return $result;
        }
        if (!is_string($memcachedResult)) {

            // no json value
            return $result;
        }
        $result = json_decode($memcachedResult, true);

        return $result;
    }

    /**
     * @param string $key
     * @param array|mixed $value
     * @param int $expired
     * @return bool
     * @throws \Exception
     */
    public function saveKey($key, $value, $expired = 1)
    {
        $this->addHistoryLog(
            array(
                'method' => __METHOD__,
                'args' => (array)func_get_args()
            )
        );

        $isValid = (is_string($key) && (!empty($key)));
        if (!$isValid) {

            throw new \Exception('Invalid parameter "key".');
        }

        $expired = Id::castUnsignedInt($expired, -1);
        if ($expired < 0) {

            throw new \Exception('Invalid parameter "expired".');
        }

        if ($value === null) {

            throw new \Exception(
                'Parameter "value" must not be null. Use delete method to force'
            );
        }
        $isValid = (
            (is_array($value))
            || (is_string($value))
            || ($value instanceof \stdClass)
        );
        if (!$isValid) {

            throw new \Exception(
                'Invalid parameter value. unsupported type: ' . gettype($value)
            );
        }

        $valueText = json_encode($value);
        if (!is_string($valueText)) {

            throw new \Exception(
                'Invalid parameter "value". json_encode failed.'
            );
        }
        $this->requireInitialized(true, __METHOD__);
        $result = $this->getMemcachedClient()->set($key, $valueText, $expired);

        $success = ($result === true);
        if ($success) {

            return $result;
        }

        throw new \Exception('CB Write Operation failed.');
    }

    /**
     * @param string $key
     * @param array $value
     * @param int $expired
     * @param int $maxRetryCount
     * @param int $usleepOnError
     * @return bool
     * @throws \Exception
     */
    public function trySaveKey(
        $key,
        $value,
        $expired,
        $maxRetryCount,
        $usleepOnError
    ) {
        $this->addHistoryLog(
            array(
                'method' => __METHOD__,
                'args' => (array)func_get_args()
            )
        );

        $isValid = (is_string($key) && (!empty($key)));
        if (!$isValid) {

            throw new \Exception('Invalid parameter "key".');
        }

        $expired = Id::castUnsignedInt($expired, -1);
        if ($expired < 0) {

            throw new \Exception('Invalid parameter "expired".');
        }

        if ($value === null) {

            throw new \Exception(
                'Parameter "value" must not be null. Use delete method to force'
            );
        }
        $isValid = (
            (is_array($value))
            || (is_string($value))
            || ($value instanceof \stdClass)
        );
        if (!$isValid) {

            throw new \Exception(
                'Invalid parameter value. unsupported type: ' . gettype($value)
            );
        }

        $valueText = json_encode($value);
        if (!is_string($valueText)) {

            throw new \Exception(
                'Invalid parameter "value". json_encode failed.'
            );
        }

        $isValid = (
            (is_int($maxRetryCount))
            && ($maxRetryCount >= 0)
            && ($maxRetryCount <= 100)
        );
        if (!$isValid) {

            throw new \Exception(
                'Invalid parameter "maxTryCount" ' . __METHOD__
            );
        }

        $isValid = (is_int($usleepOnError) && ($usleepOnError >= 0));
        if (!$isValid) {

            throw new \Exception(
                'Invalid parameter "usleepOnError" ' . __METHOD__
            );
        }

        $this->requireInitialized(true, __METHOD__);

        $maxTryCount = $maxRetryCount + 1;
        $tryCount = 0;
        while (true) {
            $tryCount++;
            if ($tryCount > $maxTryCount) {

                break;
            }

            $result = $this->getMemcachedClient()
                ->set($key, $valueText, $expired);
            $success = ($result === true);
            if ($success) {

                return $success;
            }
            if ($usleepOnError > 0) {
                usleep($usleepOnError);
            }

        }

        $errorMessage = 'CB Insert Operation failed. tries: '
            . $tryCount . '/' . $maxTryCount;

        throw new \Exception(
            $errorMessage
        );
    }

    /**
     * @param string $key
     * @throws \Exception
     * @return bool
     */
    public function deleteKey($key)
    {
        $this->addHistoryLog(
            array(
                'method' => __METHOD__,
                'args' => (array)func_get_args()
            )
        );

        $isValid = (is_string($key) && (!empty($key)));
        if (!$isValid) {

            throw new \Exception('Invalid parameter "key".');
        }

        $this->requireInitialized(true, __METHOD__);

        $result = $this->getMemcachedClient()->delete($key);
        $success = ($result === true);
        if ($success) {

            return $result;
        }

        throw new \Exception('CB Write Operation failed.');
    }


    /**
     * @param array $keys
     * @return array
     * @throws \Exception
     */
    public function fetchMultiKeys($keys)
    {
        $this->addHistoryLog(
            array(
                'method' => __METHOD__,
                'args' => (array)func_get_args()
            )
        );

        $this->requireInitialized(true, __METHOD__);
        if ($keys === null) {
            $keys = array();
        }
        if (!is_array($keys)) {

            throw new \Exception('Parameter "keys" must be array');
        }
        $result = array();
        if (count($keys) < 1) {

            return $result;
        }
        $data = $this->getMemcachedClient()->getMulti($keys);
        if (!is_array($data)) {

            throw new \Exception('Invalid result while trying to fetch data');
        }
        foreach ($keys as $key) {
            $result[$key] = null; // default value
        }
        foreach ($data as $key => $value) {
            if (is_string($value)) {
                $value = json_decode($value, true);
            } else {
                $value = null;
            }
            $result[$key] = $value;
        }

        return $result;
    }


    /**
     * @param $key
     * @param int $offset
     * @return int
     * @throws \Exception
     */
    public function incrementKey($key, $offset = 1)
    {
        $this->addHistoryLog(
            array(
                'method' => __METHOD__,
                'args' => (array)func_get_args()
            )
        );

        $isValid = (is_string($key) && (!empty($key)));
        if (!$isValid) {

            throw new \Exception('Invalid parameter "key".');
        }

        $offset = Id::castUnsignedInt($offset, 0);
        if ($offset < 1) {

            throw new \Exception('Invalid parameter "offset".');
        }

        $this->requireInitialized(true, __METHOD__);
        if ($this->getMemcachedClient()->get($key) == null) {
            $this->getMemcachedClient()->set($key, 1, 0);
        }
        $result = $this->getMemcachedClient()->increment($key, $offset);
        $success = ($result === true);
        if ($success) {

            return $result;
        }

        throw new \Exception('CB Write Operation failed.');
    }

    /**
     * @param $key
     * @param int $offset
     * @return int
     * @throws \Exception
     */
    public function decrementKey($key, $offset = 1)
    {
        $this->addHistoryLog(
            array(
                'method' => __METHOD__,
                'args' => (array)func_get_args()
            )
        );

        $isValid = (is_string($key) && (!empty($key)));
        if (!$isValid) {

            throw new \Exception('Invalid parameter "key".');
        }
        $offset = Id::castUnsignedInt($offset, 0);
        if ($offset < 1) {

            throw new \Exception('Invalid parameter "offset".');
        }
        $this->requireInitialized(true, __METHOD__);

        $result = $this->getMemcachedClient()->decrement($key, $offset);
        $success = ($result === true);
        if ($success) {

            return $result;
        }

        throw new \Exception('CB Write Operation failed.');

    }


}