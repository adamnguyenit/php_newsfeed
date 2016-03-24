<?php

namespace Newsfeed;

/**
 * Class Activity instance of activities
 *
 * @package Newsfeed
 * @author Adam Nguyen <adamnguyen.itdn@gmail.com>
 */
class Activity
{

    /**
     * Id of activity
     *
     * @var string
     */
    public $id;

    /**
     * Content of activity
     *
     * @var string
     */
    public $content;

    /**
     * Object of activity
     *
     * @var string
     */
    public $object;

    /**
     * Time of activity
     *
     * @var string
     */
    public $time;

    /**
     * Is new record
     *
     * @var bool
     */
    public $new_record;

    /**
     * gets table name
     *
     * @return string
     */
    public static function tableName()
    {
        return 'activity';
    }

    /**
     * gets index table name
     *
     * @return string
     */
    public static function indexTableName()
    {
        return static::tableName() . '_index';
    }

    /**
     * gets schema
     *
     * @return array
     */
    public static function schema()
    {
        return [
            'id' => 'uuid',
            'content' => 'text',
            'object' => 'text',
            'time' => 'timestamp'
        ];
    }

    /**
     * finds by id
     *
     * @return static
     */
    public static function find($id)
    {
        $r = Connection::select(static::tableName(), static::schema(), '*', ['id' => $id], ['page_size' => 1])->first();
        if (empty($r)) {
            return null;
        }
        if ($r['time'] === null) {
            $time = null;
        } else {
            $time = static::formatTimestamp($r['time']->time());
        }
        return new static(['id' => $r['id']->uuid(), 'content' => $r['content'], 'time' => $time, 'object' => $r['object'], 'new_record' => false]);
    }

    /**
     * hides all feeds of object
     *
     * @return bool
     */
    public static function hideAllOf($object)
    {
        if (empty($this->object)) {
            return true;
        }
        foreach (Connection::select(static::indexTableName(), static::schema(), '*', ['object' => $object]) as $r) {
            $id = $r['id']->uuid();
            Connection::delete(static::tableName(), static::schema(), ['id' => $id]);
            static::deleteFromFeed($id);
        }
        return true;
    }

    /**
     * deletes from feeds
     *
     * @param mixed $id Id of model
     * @param \Cassandra\Rows $last Last activity will be showed
     *
     * @return bool
     */
    public static function deleteFromFeed($id, $last = null)
    {
        $cqls = [];
        foreach (FeedTable::all() as $i) {
            foreach (Connection::select($i->tableName(), $i->schema(), '*', ['activity_id' => $id], ['filtering' => true]) as $r) {
                $cqls[] = Connection::delete($i->tableName(), $i->schema(), ['id' => $r['id'], 'activity_id' => $id], true);
                if (!empty($last)) {
                    $n = [
                        'id' => $r['id'],
                        'activity_id' => $last['id']->uuid(),
                        'activity_content' => $last['content'],
                        'activity_object' => $last['object'],
                        'activity_time' => $last['time']
                    ];
                    $cqls[] = Connection::insert($i->tableName(), $i->schema(), $n, true);
                }
            }
        }
        Connection::batchCql(array_unique($cqls));
        return true;
    }

    /**
     * initials a new instance
     */
    public function __construct($opt = [])
    {
        if (!empty($opt['id'])) {
            $this->id = $opt['id'];
        } else {
            $this->id = (new \Cassandra\Timeuuid())->uuid();
        }
        if (!empty($opt['content'])) {
            $this->content = $opt['content'];
        }
        if (!empty($opt['object'])) {
            $this->object = $opt['object'];
        }
        if (!empty($opt['time'])) {
            $this->time = $opt['time'];
        } else {
            $this->time = static::formatTimestamp(time());
        }
        if (!empty($opt['new_record']) && $opt['new_record'] === false) {
            $this->new_record = false;
        } else {
            $this->new_record = true;
        }
    }

    /**
     * saves
     *
     * @return bool
     */
    public function save()
    {
        if ($this->new_record) {
            return $this->insert();
        } else {
            return $this->update();
        }
    }

    /**
     * deletes
     *
     * @return bool
     */
    public function delete($showLast = true)
    {
        if ($this->new_record === true) {
            return false;
        }
        if (!Connection::delete(static::tableName(), static::schema(), ['id' => $this->id])) {
            return false;
        }
        $last = null;
        if (!empty($this->object)) {
            Connection::delete(static::indexTableName(), static::schema(), ['object' => $this->object, 'id' => $this->id]);
            if ($showLast) {
                $last = Connection::select(static::indexTableName(), static::schema(), '*', ['object' => $this->object])->first();
            }
        }
        return static::deleteFromFeed($this->id, $last);
    }

    /**
     * converts to array
     *
     * @return array
     */
    public function toArray()
    {
        return [
            'id' => $this->id,
            'content' => $this->content,
            'object' => $this->object,
            'time' => $this->time
        ];
    }

    /**
     * inserts
     *
     * @return bool
     */
    protected function insert()
    {
        if (!Connection::insert(static::tableName(), static::schema(), $this->toArray())) {
            return false;
        }
        if (!empty($this->object)) {
            if (!Connection::insert(static::indexTableName(), static::schema(), $this->toArray())) {
                Connection::delete(static::tableName(), static::schema(), ['id' => $this->id]);
                return false;
            }
        }
        $this->new_record = false;
        return true;
    }

    /**
     * updates
     *
     * @return bool
     */
    protected function update()
    {
        return Connection::update(static::tableName(), static::schema(), ['id' => $this->id], $this->toArray());
    }

    protected static function formatTimestamp($time)
    {
        return date('Y-m-d H:i:sO', $time);
    }
}
