<?php
namespace Cb2Mysql\model;

/**
 * Class PDOModel responsible for saving data to mysql
 *
 */
class PDOModel
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
    protected $table;

    /**
     * @var string
     */
    protected $user;

    /**
     * @var string
     */
    protected $password;

    /**
     * @var array
     */
    protected $columns;

    /**
     * @param $host
     * @param $db
     * @param string $user
     * @param string $password
     */
    public function __construct($dsn, $user='', $password='')
    {
        $this->setDsn($dsn);
        $this->setUser($user);
        $this->setPassword($password);
    }

    /**
     * @return PDO
     * @throws ErrorException
     */
    protected function getPDOInstance()
    {
        if (!$this->pdoInstance)
        {
            // connect to mysql
            try {
                $this->pdoInstance = new \PDO($this->dsn,$this->user,$this->password);
                $this->pdoInstance->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
            } catch (\Exception $ex) {
                throw new \ErrorException('Could not connect to mysql database');
            }
        }

        return $this->pdoInstance;
    }

    /**
     * @param string $db
     */
    protected function setDb($db)
    {
        if ($db)
            $this->db = $db;
        else
            throw new \InvalidArgumentException('Please supply the mysql database');
    }

    /**
     * @return string
     */
    public function getDb()
    {
        return $this->db;
    }

    /**
     * @param string $dsn
     */
    protected function setDsn($dsn)
    {
        $this->dsn = $dsn;
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
     * @return array
     */
    public function getColumns()
    {
        return $this->columns;
    }

    /**
     * set table, create automatically, optionally truncate
     *
     * @param $tableName string
     * @param $truncate bool
     */
    public function setTable($tableName, $truncate=false)
    {
        if ($tableName)
            $this->table = $tableName;
        else
            throw new \InvalidArgumentException('Please supply the mysql table');

        try {
            $this->columns = $this->describeTable($tableName);

            if ($truncate)
                $this->truncateTable($tableName);

        } catch (\Exception $ex) {
            throw $ex;
            //$output->writeln('Creating mysql table '.$mysqlConfig['table']);

            $this->createTable($tableName);

            $this->columns = $this->describeTable($tableName);
        }
    }

    /**
     * @return string
     */
    public function getTable()
    {
        return $this->table;
    }

    /**
     * add columns where needed
     *
     * @param $columns array
     */
    public function addColumns($columns)
    {
        foreach (array_diff($columns, $this->columns) as $column) {
            //$output->writeln("Adding mysql table column ".$column.' TEXT');
            $this->getPdoInstance()->exec('alter table '.$this->table.' add column '.$column.' TEXT');
        }

        $this->columns = array_unique(array_merge($this->columns, $columns));
    }

    /**
     * insert a new row
     *
     * @param $row
     * @return bool
     * @throws ErrorException
     */
    public function addRow($row)
    {
        // make sure all columns are added
        $this->addColumns(array_keys($row));

        $binds = array_map(function($key){return ":$key";}, array_keys($row));
        $stmt = $this->getPdoInstance()->prepare('insert into '.$this->table.'('.implode(',',array_keys($row)).') VALUES ('.implode(',', $binds).')');
        foreach ($row as $key => $value)
            $stmt->bindValue(":$key", $value);

        return $stmt->execute();
    }


    /**
     * @param $table
     * @return array
     */
    protected function describeTable($tableName)
    {
        $rows = $this->getPdoInstance()->query('describe '.$tableName)->fetchAll();

        return array_map(function($row){
            return $row['Field'];
        }, $rows);
    }

    protected function createTable($tableName)
    {
        $this->getPdoInstance()->query('create table '.$tableName. '(id varchar(255))');
    }

    protected function truncateTable($tableName)
    {
        $this->getPdoInstance()->query('truncate '.$tableName);
    }
}