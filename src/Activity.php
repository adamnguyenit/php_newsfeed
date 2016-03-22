<?php

namespace Newsfeed;

class Activity
{

    public $id;
    public $content;
    public $object;
    public $time;
    public $new_record;

    public static function tableName()
    {
        return 'activity';
    }

    public static function indexTableName()
    {
        return static::tableName() . '_index';
    }

    public static function schema()
    {
        return [
            'id' => 'uuid',
            'content' => 'text',
            'object' => 'text',
            'time' => 'timestamp'
        ];
    }

    public static function find($id)
    {
        $r = Connection::select(static::tableName(), static::schema(), '*', ['id' => $id], ['page_size' => 1])->first();
        if (empty($r)) {
            return null;
        }
        return new static(['id' => $r['id']->uuid(), 'content' => $r['content'], 'time' => $r['time'], 'object' => $r['object'], 'new_record' => false]);
    }

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
                    ]
                    $cqls[] = Connection::insert($i->tableName(), $i->schema(), $n, true);
                }
            }
        }
        Connection::batchCql(array_unique($cqls));
        return true;
    }

    public static function delete($id, $showLast = true)
    {
        $act = static::find($id);
        if (empty($act)) {
            return true;
        }
        return $act->delete();
    }

    public function __construct($opt = [])
    {
        if (!empty($opt['id'])) {
            $this->id = $opt['id'];
        } else {
            $this->id = (new Cassandra::Timeuuid())->uuid();
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
            $this->time = date('Y-m-d H:i:sO');
        }
        if (!empty($opt['new_record']) && $opt['new_record'] === false) {
            $this->new_record = false;
        } else {
            $this->new_record = true;
        }
    }

    public function save()
    {
        if ($this->new_record) {
            return $this->insert();
        } else {
            return $this->update();
        }
    }

    public function delete()
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
                $last = Connection::select(static::indexTableName(), static::schema(), '*', ['object' => $object])->first();
            }
        }
        return static::deleteFromFeed($this->id, $last);
    }

    public function toArray()
    {
        return [
            'id' => $this->id,
            'content' => $this->content,
            'object' => $this->object,
            'time' => $this->time
        ];
    }

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

    protected function update()
    {
        return Connection::update((static::tableName(), static::schema(), ['id' => $this->id], $this->toArray());
    }
}
