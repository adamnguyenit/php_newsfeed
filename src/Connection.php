<?php

namespace Newsfeed;

/**
 * Class Connection for Cassandra communication
 *
 * @package Newsfeed
 * @author Adam Nguyen <adamnguyen.itdn@gmail.com>
 */
class Connection
{

    /**
     * Cassandra connection
     *
     * @var \Cassandra\DefaultSession
     */
    protected static $_connection = null;

    /**
     * Configuration
     *
     * @var array
     */
    public static $config = [];

    /**
     * gets cassandra connection
     *
     * @return \Cassandra\DefaultSession
     */
    public static function connection()
    {
        if (empty(static::$_connection)) {
            if (empty(static::$config) || empty(static::$config['keyspace'])) {
                return null;
            }
            $builder = \Cassandra::cluster();
            if (isset(static::$config['hosts'])) {
                $hosts = is_array(static::$config['hosts']) ? static::$config['hosts'] : [static::$config['hosts']];
                call_user_func_array([$builder, 'withContactPoints'], $hosts);
            }
            if (isset(static::$config['port'])) {
                $builder->withPort(static::$config['port']);
            }
            if (isset(static::$config['username']) && isset(static::$config['password'])) {
                $builder->withCredentials(static::$config['username'], static::$config['password']);
            }
            static::$_connection = $builder->build()->connect(static::$config['keyspace']);
        }
        return static::$_connection;
    }

    /**
     * closes current connection
     */
    public static function close()
    {
        if (!empty(static::$_connection)) {
            static::$_connection->close();
        }
        static::$_connection = null;
    }

    /**
     * executes cql
     *
     * @param string $cql CQL query
     * @param array $opt Options
     *
     * @return \Cassandra\Rows
     */
    public static function execCql($cql, $opt = [])
    {
        return static::connection()->execute(new \Cassandra\SimpleStatement($cql), new \Cassandra\ExecutionOptions($opt));
    }

    /**
     * batchs cqls
     *
     * @param string[] $cqls CQL queries
     * @param array $opt Options
     *
     * @return \Cassandra\Rows
     */
    public static function batchCql($cqls, $opt = [])
    {
        $batch = new \Cassandra\BatchStatement();
        foreach ($cqls as $cql) {
            if (!empty($cql)) {
                $batch->add(new \Cassandra\SimpleStatement($cql));
            }
        }
        return static::connection()->execute($batch, new \Cassandra\ExecutionOptions($opt));
    }

    /**
     * exports a value to cassandra value
     *
     * @param mixed $val Value
     * @param string $type Type of value
     *
     * @return mixed
     */
    public static function cassVal($val, $type)
    {
        if ($val === null) {
            return 'null';
        }
        if ($type === 'uuid') {
            return $val;
        }
        if (in_array($type, ['int', 'bigint'])) {
            return intval($val);
        }
        if (in_array($type, ['float', 'double']) && is_numeric($val)) {
            return doubleval($val);
        }
        if (in_array($type, ['ascii', 'text', 'varchar', 'timestamp'])) {
            return "'" . str_replace("'", "''", strval($val)) . "'";
        }
        return null;
    }

    /**
     * exports values to cassandra values
     *
     * @param array $schema Schema
     * @param array $vals Values
     *
     * @return string
     */
    public static function cassVals($schema, $vals)
    {
        $cassVals = [];
        foreach ($vals as $col => $val) {
            if (!empty($schema[$col])) {
                $cassVals[] = static::cassVal($val, $schema[$col]);
            }
        }
        return implode(',', $cassVals);
    }

    /**
     * exports column value pair
     *
     * @param array $schema Schema
     * @param array $colValPair Columns values pairs
     *
     * @return string[]
     */
    public static function exportedColVal($schema, $colValPair)
    {
        $arr = [];
        foreach ($colValPair as $col => $val) {
            if (!empty($schema[$col])) {
                $cassVal = static::cassVal($val, $schema[$col]);
                $arr[] = "$col=$cassVal";
            }
        }
        return $arr;
    }

    /**
     * selects
     *
     * @param string $tbl Table name
     * @param array $schema Schema
     * @param string|string[] $cols Selected columns
     * @param array $cond Conditions
     * @param array $opt Options
     * @param bool $toCql Return cql instead selected result
     *
     * @return \Cassandra\Rows|string
     */
    public static function select($tbl, $schema = [], $cols = '*', $cond = [], $opt = [], $toCql = false)
    {
        $cql = 'SELECT ';
        if (is_array($cols)) {
            $cql .= implode(',', $cols);
        } else {
            $cql .= '*';
        }
        $cql .= " FROM $tbl";
        if (!empty($cond)) {
            $exported = static::exportedColVal($schema, $cond);
            $exported = implode(' AND ', $exported);
            if (!empty($exported)) {
                $cql .= " WHERE $exported";
            }
        }
        if (!empty($opt['filtering']) && $opt['filtering'] === true) {
            $cql .= ' ALLOW FILTERING';
            unset($opt['filtering']);
        }
        if ($toCql) {
            return $cql;
        }
        return static::execCql($cql, $opt);
    }

    /**
     * inserts
     *
     * @param string $tbl Table name
     * @param array $schema Schema
     * @param array $vals Values
     * @param bool $toCql Return cql instead selected result
     *
     * @return bool|string
     */
    public static function insert($tbl, $schema, $vals, $toCql = false)
    {
        $keys = implode(',', array_keys($schema));
        $cassVals = static::cassVals($schema, $vals);
        $cql = "INSERT INTO $tbl ($keys) VALUES ($cassVals)";
        if ($toCql) {
            return $cql;
        }
        static::execCql($cql);
        return true;
    }

    /**
     * updates
     *
     * @param string $tbl Table name
     * @param array $schema Schema
     * @param array $cond Conditions
     * @param array $vals Values
     * @param bool $toCql Return cql instead selected result
     *
     * @return bool|string
     */
    public static function update($tbl, $schema, $cond, $vals, $toCql = false)
    {
        $exported_vals = implode(',', static::exportedColVal($schema, $vals));
        $exportedCond = implode(' AND ', static::exportedColVal($schema, $cond));
        $cql = "UPDATE $tbl SET $exported_vals WHERE $exportedCond";
        if ($toCql) {
            return $cql;
        }
        static::execCql($cql);
        return true;
    }

    /**
     * deletes
     *
     * @param string $tbl Table name
     * @param array $schema Schema
     * @param array $cond Conditions
     * @param bool $toCql Return cql instead selected result
     *
     * @return bool|string
     */
    public static function delete($tbl, $schema, $cond, $toCql = false)
    {
        $exportedCond = implode(' AND ', static::exportedColVal($schema, $cond));
        $cql = "DELETE FROM $tbl WHERE $exportedCond";
        if ($toCql) {
            return $cql;
        }
        static::execCql($cql);
        return true;
    }
}
