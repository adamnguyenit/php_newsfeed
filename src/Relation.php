<?php

namespace Newsfeed;

class Relation
{
    public static function tableName()
    {
        return 'relation';
    }

    public static function indexTableName()
    {
        return static::tableName() . '_index';
    }

    public static function schema()
    {
        return [
            'id' => 'uuid',
            'from_class' => 'text',
            'from_id' => 'text',
            'to_class' => 'text',
            'to_id' => 'text'
        ];
    }

    public static function create($from, $to, $opt = [])
    {
        $id = (new Cassandra::Timeuuid())->uuid();
        $r = [
            'id' => $id,
            'from_class' => get_class($from),
            'from_id' => $from->id,
            'to_class' => get_class($to),
            'to_id' => $to->id
        ];
        if (!Connection::insert(static::tableName(), static::schema(), $r)) {
            return false;
        }
        if (!Connection::insert(static::indexTableName(), static::schema(), $r)) {
            $cond = [
                'id' => $id,
                'from_class' => get_class($from),
                'from_id' => $from->id,
            ];
            Connection::delete(static::tableName(), static::schema(), $cond);
            return false;
        }
        if (!empty($opt['side']) && $opt['side'] === 'both') {
            if (!static::create($to, $from)) {
                static::delete($from, to);
                return false;
            }
        }
        return true;
    }

    public static function delete($from, $to, $opt = [])
    {
        $cond = [
            'from_class' => get_class($from),
            'from_id' => $from->id,
            'to_class' => get_class($to),
            'to_id' => $to->id
        ];
        $t = Connection::select(static::indexTableName(), static::schema(), '*', $cond)->first();
        if (!empty($t)) {
            $c = [
                'id' => $t['id']->uuid(),
                'from_class' => $t['from_class'],
                'from_id' => $t['from_id']
            ];
            Connection::delete(static::tableName(), static::schema(), $c);
            Connection::delete(static::indexTableName(), static::schema(), $cond);
        }
        if (!empty($opt['side']) && $opt['side'] === 'both') {
            return static::delete($to, $from);
        }
        return true;
    }

    public static function relatedOf($from)
    {
        $relateds = [];
        $cond = [
            'from_class' => get_class($from),
            'from_id' => $from->id
        ];
        foreach (Connection::select(static::indexTableName(), static::schema(), '*', $cond) as $r) {
            if (class_exists($r['to_class'])) {
                $related[] = new $r['to_class'](['id' => $r['to_id']]);
            }
        }
        return $relateds;
    }

    public static function isRelated($from, $to)
    {
        $cond = [
            'from_class' => get_class($from),
            'from_id' => $from->id,
            'to_class' => get_class($to),
            'to_id' => $to->id
        ];
        $t = Connection::select(static::indexTableName(), static::schema(), '*', $cond)->first();
        if (empty($t)) {
            return false;
        }
        $c = [
            'id' => $t['id']->uuid(),
            'from_class' => $t['from_class'],
            'from_id' => $t['from_id']
        ];
        return !empty(Connection.select(static::tableName(), static::schema(), '*', $c)->first());
    }
}
