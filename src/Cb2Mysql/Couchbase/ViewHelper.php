<?php
/**
 * Created by JetBrains PhpStorm.
 * User: seb
 * Date: 4/23/13
 * Time: 4:43 PM
 * To change this template use File | Settings | File Templates.
 */

namespace Cb2Mysql\Couchbase;

use Cb2Mysql\Utils\Id;

class ViewHelper
{
    /** @var string */
    private $design;
    /**
     * @var string
     */
    private $view;
    /**
     * @var int
     */
    private $restApiPort = 8092;
    /**
     * @var string
     */
    private $host;
    /**
     * @var string
     */
    private $bucketName = '';

    /**
     * @var array
     */
    private $historyLog = array();

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
     * @return string
     */
    public function getCouchbaseApiUrl()
    {
        return 'http://'
        . $this->getHost()
        . ':' . $this->getRestApiPort()
        . '/' . $this->getBucketName()
        . '/_design/'
        . $this->getDesign()
        . '/_view/'
        . $this->getView();
    }

    /**
     * @return array|null
     */
    public function getLastHistoryLogItem()
    {
        $result = null;
        $historyLog = $this->getHistoryLog();
        if (!is_array($historyLog)) {

            return $result;
        }
        $result = array_pop($historyLog);

        return $result;
    }

    /**
     * @param string $design
     * @return $this
     */
    public function setDesign($design)
    {
        $this->design = $design;

        return $this;
    }

    /**
     * @return string
     */
    public function getDesign()
    {
        return $this->design;
    }

    /**
     * @param string $view
     * @return $this
     */
    public function setView($view)
    {
        $this->view = $view;

        return $this;
    }

    /**
     * @return string
     */
    public function getView()
    {
        return $this->view;
    }

    /**
     * @return int
     */
    public function getRestApiPort()
    {
        return (int)$this->restApiPort;
    }

    /**
     * @param $value
     *
     * @return $this
     * @throws \Exception
     */
    public function setRestApiPort($value)
    {
        $port = Id::castUnsignedInt($value, -1);
        if ($port < 1) {

            throw new \Exception(
                'Invalid parameter "value"! ' . __METHOD__ . ' ' . get_class(
                    $this
                )
            );
        }

        $this->restApiPort = $port;

        return $this;
    }

    /**
     * @return string
     * @throws \Exception
     */
    public function getHost()
    {

        return (string)$this->host;
    }

    /**
     * @param $value
     *
     * @return $this
     * @throws \Exception
     */
    public function setHost($value)
    {
        $isValid = is_string($value) && (!empty($value));
        if (!$isValid) {

            throw new \Exception(
                'Invalid parameter "value". ' . __METHOD__ . ' ' . get_class(
                    $this
                )
            );
        }
        $this->host = $value;

        return $this;
    }

    /**
     * @param $value
     *
     * @return $this
     * @throws \Exception
     */
    public function setBucketName($value)
    {
        $isValid = is_string($value) && (!empty($value));
        if (!$isValid) {

            throw new \Exception(
                'Invalid parameter "value". ' . __METHOD__ . ' ' . get_class(
                    $this
                )
            );
        }
        $this->bucketName = $value;

        return $this;
    }

    /**
     * @return string
     */
    public function getBucketName()
    {

        return (string)$this->bucketName;
    }

    /**
     * @param array|null $params
     *
     * @return mixed
     * @throws \Exception
     */
    public function fetchView($params)
    {
        if ($params === null) {
            $params = array();
        }
        if (!is_array($params)) {

            throw new \Exception('Invalid parameter "params".');
        }

        $queryString = $this->optionsToQueryString($params);
        $dataUrl = $this->getCouchbaseApiUrl() . "?" . $queryString;
        // for debugging
        $this->addHistoryLog(
            array(
                'url' => $dataUrl,
            )
        );
        $response = json_decode(file_get_contents($dataUrl), true); // curl???

        return $response;
    }

    /**
     * @param array $options
     * @return string
     * @throws \Exception
     */
    private function optionsToQueryString($options)
    {
        $queryParts = array();
        foreach ($options as $key => $value) {
            if ($value === null) {

                continue;
            }

            $isValid = (is_array($value)
                || is_scalar($value));
            if (!$isValid) {

                throw new \Exception('Invalid type of value for option '
                    . json_encode($key) . '!'
                );
            }

            switch ($key) {
                case 'stale':
                    if (in_array($value, array(false, 'false'), true)) {
                        $value = 'false';
                    }
                    if (!in_array(
                        $value,
                        array('false', 'ok', 'update_after'),
                        true
                    )
                    ) {

                        throw new \Exception(
                            'Invalid cbOption: stale='
                            . json_encode($value)
                        );
                    }

                    break;

                default:
                    if (is_bool($value)) {
                        $value = json_encode($value);
                    } else {
                        $value = urlencode(json_encode($value));
                    }

                    break;
            }
            $queryParts[] = '' . $key . '=' . $value;
        }
        $queryString = join('&', $queryParts);

        return $queryString;
    }

}
