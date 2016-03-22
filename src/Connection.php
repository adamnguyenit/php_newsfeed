<?php

namespace Newsfeed;

class Connection
{

    protected static $_connection;

    public static $config = [];

    /**
     * gets cassandra connection
     */
    public static function connection()
    {
        if (empty(static::$_connection)) {
            if (empty(static::$config) || empty(static::$config['keyspace'])) {
                return null;
            }
            static::$_connection = \Cassandra::cluster()->build()->connect(static::$config['keyspace']);
        }
        return static::$_connection;
    }

    /**
     * executes cql
     */
    public static function execCql($cql, $opt = [])
    {
        return static::connection()->execute(new \Cassandra\SimpleStatement($cql), new \Cassandra\ExecutionOptions($opt));
    }

    /**
     * batchs cqls
     */
    public static function batchCql($cqls, $opt = [])
    {
        $batch = new BatchStatement();
        foreach ($cqls as $cql) {
            if (!empty($cql)) {
                $batch.add(new \Cassandra\SimpleStatement($cql));
            }
        }
        return static::connection()->execute($batch, new \Cassandra\ExecutionOptions($opt));
    }

    /**
     * exports a value to cassandra value
     */
    public static function $cassVal($val, $type)
    {
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
            return str_replace("'", "''", strval($val));
        }
        return null;
    }

    /**
     * exports values to cassandra values
     */
    public static function cassVals($schema, $vals)
    {
        $cassVals = [];
        foreach ($vals as $col => $val) {
            if (!empty($schema[$cal])) {
                $cassVals[] = static::$cassVal($val, $schema[$col]);
            }
        }
        return implode(',', $cassVals);
    }

    /**
     * exports column value pair
     */
    public static function exportedColVal($schema, $col_val_pair)
    {
        $arr = [];
        foreach ($col_val_pair as $col => $val) {
            if (!empty($schema[$col])) {
                $$cassVal = static::$cassVal($val, $schema[$col]);
                $arr[] = "$col=$$cassVal";
            }
        }
        return $arr;
    }

    /**
     * selects
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
     */
    public static function insert($tbl, $schema, $vals, $toCql = false)
    {
        $keys = implode(',', array_keys($schema));
        $cassVals = static::cassVals($schema, $vals);
        $cql = "INSERT INTO $tbl ($keys) VALUES ($cassVals)";
        if ($toCql) {
            return $cql;
        }
        static::execCql($cql, $opt);
        return true;
    }

    /**
     * updates
     */
    public static function update($tbl, $schema, $cond, $vals, $toCql = false)
    {
        $exported_vals = implode(',', static::exportedColVal($schema, $vals));
        $exportedCond = implode(' AND ', static::exportedColVal($schema, $cond));
        $cql = "UPDATE $tbl SET $exported_vals WHERE $exportedCond";
        if ($toCql) {
            return $cql;
        }
        static::execCql($cql, $opt);
        return true;
    }

    /**
     * deletes
     */
    public static function delete($tbl, $schema, $cond, $toCql = false)
    {
        $exportedCond = implode(' AND ', static::exportedColVal($schema, $cond));
        $cql = "DELETE FROM $tbl WHERE $exportedCond";
        if ($toCql) {
            return $cql;
        }
        static::execCql($cql, $opt);
        return true;
    }
}
