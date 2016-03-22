<?php

namespace Newsfeed;

class FeedTable
{

    public static function tableName()
    {
        return 'feed_table';
    }

    public static function schema()
    {
        return [
            'table_class' => 'text'
        ];
    }

    public static function create($tbl_class)
    {
        return Connection::insert(static::tableName(), static::schema(), ['table_class' => $tbl_class]);
    }

    public static function delete($tbl_class)
    {
        return Connection::delete(static::tableName(), static::schema(), ['table_class' => $tbl_class]);
    }

    public static function all()
    {
        $items = [];
        foreach (Connection::select(static::tableName()) as $r) {
            if (class_exists($r['table_class'])) {
                $items[] = new $r['table_class']();
            }
        }
        return $items;
    }

}
