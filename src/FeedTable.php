<?php

namespace Newsfeed;

/**
 * Class FeedTable for store all feed models
 *
 * @package Newsfeed
 * @author Adam Nguyen <adamnguyen.itdn@gmail.com>
 */
class FeedTable
{

    /**
     * gets table name
     *
     * @return string
     */
    public static function tableName()
    {
        return 'feed_table';
    }

    /**
     * gets schema
     *
     * @return array
     */
    public static function schema()
    {
        return [
            'table_class' => 'text'
        ];
    }

    /**
     * adds a model
     *
     * @param string $tblClass Classname of model
     *
     * @return bool
     */
    public static function create($tblClass)
    {
        return Connection::insert(static::tableName(), static::schema(), ['table_class' => $tblClass]);
    }

    /**
     * deletes a model
     *
     * @param string $tblClass Classname of model
     *
     * @return bool
     */
    public static function delete($tblClass)
    {
        return Connection::delete(static::tableName(), static::schema(), ['table_class' => $tblClass]);
    }

    /**
     * gets all models
     *
     * @return NewsfeedModel[]
     */
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
