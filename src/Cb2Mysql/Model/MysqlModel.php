<?php
namespace Cb2Mysql\model;

/**
 * Class MysqlModel responsible for saving data to mysql
 * @package Cb2Mysql\model
 */
class MysqlModel
{
    /**
     * @var PDO
     */
    protected $pdoInstance;

    /**
     * @var string
     */
    protected $dsn;

    /**
     * @var string
     */
    protected $user;

    /**
     * @var string
     */
    protected $password;

    /**
     * @var array cache table columns
     */
    protected $tableColumns;

    /**
     * @param $dsn
     * @param string $user
     * @param string $password
     * @internal param $host
     * @internal param $db
     */
    public function __construct($dsn, $user = '', $password = '')
    {
        $this->setDsn($dsn);
        $this->setUser($user);
        $this->setPassword($password);
    }

    /**
     * @throws \ErrorException
     * @return PDO
     */
    protected function getPDOInstance()
    {
        if (!$this->pdoInstance) {
            // connect to mysql
            try {
                $this->pdoInstance = new \PDO($this->dsn, $this->user, $this->password);
                $this->pdoInstance->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
            } catch (\Exception $ex) {
                throw new \ErrorException('Could not connect to mysql database');
            }
        }

        return $this->pdoInstance;
    }

    /**
     * @param string $dsn
     */
    protected function setDsn($dsn)
    {
        if ($dsn) {
            $this->dsn = $dsn;
        } else {
            throw new \InvalidArgumentException("Please supply the dsn");
        }
    }

    /**
     * @return string
     */
    public function getDsn()
    {
        return $this->dsn;
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
     * @param $table
     * @return array
     */
    public function getColumns($table)
    {
        if (!isset($this->tableColumns[$table])) {
            $this->tableColumns[$table] = $this->describeTable($table);
        }

        return $this->tableColumns[$table];
    }

    /**
     * @param $tableName
     * @internal param $table
     * @return array
     */
    public function describeTable($tableName)
    {
        $rows = $this->getPdoInstance()->query('describe ' . $tableName)->fetchAll();

        return array_map(
            function ($row) {
                return $row['Field'];
            },
            $rows
        );
    }

    /**
     * @param $tableName
     * @throws \InvalidArgumentException
     */
    public function createTable($tableName)
    {
        if (!$tableName) {
            throw new \InvalidArgumentException('Please supply the mysql table');
        }

        try {
            $this->getPdoInstance()->query('create table ' . $tableName . '(id varchar(255))');
        } catch (\exception $ignored) {
        }
    }

    /**
     * @param $tableName
     */
    public function truncateTable($tableName)
    {
        $this->getPdoInstance()->query('truncate ' . $tableName);
    }

    /**
     * add columns to table
     *
     * @param $table
     * @param $columns array
     */
    public function addColumns($table, $columns)
    {
        foreach (array_diff($columns, $this->getColumns($table)) as $column) {
            $this->getPdoInstance()->exec('alter table ' . $table . ' add column ' . $column . ' TEXT');
        }

        $this->tableColumns[$table] = array_unique(array_merge($this->tableColumns[$table], $columns));
    }

    /**
     * insert a new row
     *
     * @param $table
     * @param $row
     * @return bool
     */
    public function addRow($table, $row)
    {
        // make sure all columns are added
        $this->addColumns($table, array_keys($row));

        $binds = array_map(
            function ($key) {
                return ":$key";
            },
            array_keys($row)
        );
        $stmt = $this->getPdoInstance()->prepare(
            'insert into ' . $table . ' (' . implode(',', array_keys($row)) . ') VALUES (' . implode(',', $binds) . ')'
        );
        foreach ($row as $key => $value) {
            $stmt->bindValue(":$key", $value);
        }

        return $stmt->execute();
    }

}