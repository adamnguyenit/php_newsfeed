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
    public static function exec_cql($cql, $opt = [])
    {
        return static::connection()->execute(new \Cassandra\SimpleStatement($cql), new \Cassandra\ExecutionOptions($opt));
    }

    /**
     * batchs cqls
     */
    public static function batch_cqls($cqls, $opt = [])
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
    public static function cass_val($val, $type)
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
    public static function cass_vals($schema, $vals)
    {
        $cass_vals = [];
        foreach ($vals as $col => $val) {
            if (!empty($schema[$cal])) {
                $cass_vals[] = static::cass_val($val, $schema[$col]);
            }
        }
        return implode(',', $cass_vals);
    }

    /**
     * exports column value pair
     */
    public static function exported_col_val($schema, $col_val_pair)
    {
        $arr = [];
        foreach ($col_val_pair as $col => $val) {
            if (!empty($schema[$col])) {
                $cass_val = static::cass_val($val, $schema[$col]);
                $arr[] = "$col=$cass_val";
            }
        }
        return $arr;
    }

    /**
     * selects
     */
    public static function select($tbl, $schema = [], $cols = '*', $cond = [], $opt = [], $to_cql = false)
    {
        $cql = 'SELECT ';
        if (is_array($cols)) {
            $cql .= implode(',', $cols);
        } else {
            $cql .= '*';
        }
        $cql .= " FROM $tbl";
        if (!empty($cond)) {
            $exported = static::exported_col_val($schema, $cond);
            $exported = implode(' AND ', $exported);
            if (!empty($exported)) {
                $cql .= " WHERE $exported";
            }
        }
        if (!empty($opt['filtering']) && $opt['filtering'] === true) {
            $cql .= ' ALLOW FILTERING';
            unset($opt['filtering']);
        }
        if ($to_cql) {
            return $cql;
        }
        return static::exec_cql($cql, $opt);
    }

    /**
     * inserts
     */
    public static function insert($tbl, $schema, $vals, $to_cql = false)
    {
        $keys = implode(',', array_keys($schema));
        $cass_cqls = static::cass_vals($schema, $vals);
        $cql = "INSERT INTO $tbl ($keys) VALUES ($cass_vals)";
        if ($to_cql) {
            return $cql;
        }
        static::exec_cql($cql, $opt);
        return true;
    }

    /**
     * updates
     */
    public static function update($tbl, $schema, $cond, $vals, $to_cql = false)
    {
        $exported_vals = implode(',', static::exported_col_val($schema, $vals));
        $exported_cond = implode(' AND ', static::exported_col_val($schema, $cond));
        $cql = "UPDATE $tbl SET $exported_vals WHERE $exported_cond";
        if ($to_cql) {
            return $cql;
        }
        static::exec_cql($cql, $opt);
        return true;
    }

    /**
     * deletes
     */
    public static function delete($tbl, $schema, $cond, $to_cql = false)
    {
        $exported_cond = implode(' AND ', static::exported_col_val($schema, $cond));
        $cql = "DELETE FROM $tbl WHERE $exported_cond";
        if ($to_cql) {
            return $cql;
        }
        static::exec_cql($cql, $opt);
        return true;
    }
}
