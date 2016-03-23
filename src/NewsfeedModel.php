<?php

namespace Newsfeed;

/**
 * Class NewsfeedModel is feed model
 *
 * @package Newsfeed
 * @author Adam Nguyen <adamnguyen.itdn@gmail.com>
 */
class NewsfeedModel
{

    const SECRET_KEY = 'sL6PySf7Ijo8L0ZhU7R2';

    /**
     * Id of model
     * Define the type of id in the class static variable $typeId
     *
     * @var mixed
     */
    public $id;

    /**
     * Next page token
     *
     * @var string
     */
    public $nextPageToken;

    /**
     * Feeds
     *
     * @var Activity[]
     */
    public $feeds;

    /**
     * Type of id
     *
     * @static string
     */
    public static $typeId = 'bigint';

    /**
     * gets table name
     *
     * @return string
     */
    public static function tableName()
    {
        $fullClassName = explode("\\", get_called_class());
        $className = end($fullClassName);
        return strtolower(preg_replace('/(?<=\\w)(?=[A-Z])/',"_$1", $className));
    }

    /**
     * gets schema
     *
     * @return array
     */
    public static function schema()
    {
        return [
            'id' => static::$typeId,
            'activity_id' => 'uuid',
            'activity_content' => 'text',
            'activity_object' => 'text',
            'activity_time' => 'timestamp'
        ];
    }

    /**
     * inserts
     *
     * @param mixed $id Id of model
     * @param Activity $act Activity
     * @param bool $related Is add for related models
     * @param bool $hideOld Is hide the old feed of object
     *
     * @return bool
     */
    public static function insert($id, $act, $related = true, $hideOld = true)
    {
        return (new static(['id' => $id]))->insert($act, $related, $hideOld);
    }

    /**
     * deletes
     *
     * @param mixed $id Id of model
     * @param string $actId Activity Id
     * @param bool $related Is delete from related models
     *
     * @return bool
     */
    public static function delete($id, $actId, $related = true)
    {
        return (new static(['id' => $id]))->delete($actId, $related);
    }

    /**
     * gets feeds
     *
     * @param mixed $id Id of model
     * @param int $pageSize Size per page
     * @param string|null $nextPageToken Next page token
     *
     * @return Activity[]
     */
    public static function feeds($id, $pageSize = 10, $nextPageToken = null)
    {
        return (new static(['id' => $id, 'nextPageToken' => $nextPageToken]))->feeds($pageSize);
    }

    /**
     * initials a new instance
     */
    public function __construct($opt = [])
    {
        if (!empty($opt['id'])) {
            $this->id = $opt['id'];
        }
        if (!empty($opt['nextPageToken'])) {
            $this->nextPageToken = $opt['nextPageToken'];
        }
    }

    /**
     * deletes
     *
     * @param string|null $actId Activity Id
     * @param bool $related Is delete from related models
     *
     * @return bool
     */
    public function delete($actId = null, $related = true)
    {
        if (!empty($actId)) {
            if ($related) {
                return Activity::delete($actId, true);
            } else {
                return Connection::delete(static::tableName(), static::schema(), ['id': $this->id, 'activity_id': $actId);
            }
        }
        $cqls = [];
        foreach (Connection::select(static::tableName(), static::schema(), '*', ['id' => $this->id]) as $r) {
            if ($related) {
                Activity::delete($r['id']->uuid(), true);
            } else {
                $cqls[] = Connection::delete(static::tableName(), static::schema(), ['id': $this->id, 'activity_id': $r['id']->uuid()], true);
            }
        }
        Connection::batchCqls(array_unique($cqls));
        return true;
    }

    /**
     * inserts
     *
     * @param Activity $act Activity
     * @param bool $related Is insert for related models
     * @param bool $hideOld Is hide old feeds of object
     *
     * @return bool
     */
    public function insert($act, $related = true, $hideOld = true)
    {
        $n = [
            'id': $this->id,
            'activity_id': $act->id,
            'activity_content': $act->content,
            'activity_object': $act->object,
            'activity_time': $act->time
        ];
        if (!Connection::insert(static::tableName(), static::schema(), $n)) {
            return false;
        }
        $insArr = [];
        $cqls = [];
        if ($related) {
            foreach (Relation::related_of($this) as $i) {
                $n['id'] = $i->id;
                $cqls[] = Connection::insert($i->tableName(), $i->schema(), $n, true);
                if ($hideOld) {
                    $insArr[] = $i;
                }
            }
        }
        if ($hideOld) {
            $insArr[] = $this;
            $cqls = array_merge($cqls, $this->cqlHideOldFeedsOf($act, $insArr));
        }
        Connection::batchCqls($cqls);
        return true;
    }

    /**
     * registers
     *
     * @param static $to Related model
     * @param array $opt Options
     *
     * @return bool
     */
    public function register($to, $opt = [])
    {
        return Relation::create($this, $to, $opt);
    }

    /**
     * deregisters
     *
     * @param static $to Related model
     * @param array $opt Options
     *
     * @return bool
     */
    public function deregister($to, $opt = [])
    {
        return Relation::delete($this, $to, $opt);
    }

    /**
     * checks is related
     *
     * @param static $to
     *
     * @return bool
     */
    public function isRegistered($to)
    {
        return Relation::isRelated($this, $to);
    }

    /**
     * get feeds
     *
     * @param int $pageSize Size per page
     *
     * @return Activity[]
     */
    public function feeds($pageSize = 10)
    {
        $this->feeds = [];
        $opt = ['page_size' => intval($pageSize)];
        if (!empty($this->nextPageToken)) {
            $opt['paging_state_token'] = $this->decodedNextPageToken();
        }
        $res = Connection::select(static::tableName(), static::schema(), '*', ['id' => $this->id], $opt);
        foreach ($res as $r) {
            $n = [
                'id' => $r['activity_id']->uuid(),
                'content' => $r['content'],
                'object' =? $r['object'],
                'time' => $r['time'],
            ];
            $this->feeds[] = new Activity($n);
        }
        $this->encodedNextPageToken($res);
        $this->afterFeeds();
        return $this->feeds;
    }

    /**
     * processes after get feeds
     * should override this to handle result, for example: decode activity content
     */
    protected function afterFeeds()
    {
    }

    /**
     * encodes next page token
     *
     * @param \Cassandra\Row $res Result
     *
     * @return string
     */
    protected function encodedNextPageToken($res)
    {
        if (empty($res) || $res->isLastPage()) {
            $this->nextPageToken = null;
        }
        $key = base64_encode($res->pagingStateToken());
        $this->nextPageToken = base64_encode($key . static::SECRET_KEY);
        return $this->nextPageToken;
    }

    /**
     * decodes next page token
     *
     * @return string
     */
    protected function decodedNextPageToken()
    {
        if (empty($this->nextPageToken)) {
            return null;
        }
        $key = substr(base64_decode($this->nextPageToken), -1, strlen(static::SECRET_KEY));
        return base64_decode($key);
    }

    /**
     * generates hide old feed cqls
     *
     * @param Activity $act Latest activity
     * @param static[] $insArr Array of models
     *
     * @return string[]
     */
    protected function cqlHideOldFeedsOf($act, $insArr)
    {
        if (empty($act->object)) {
            return [];
        }
        $cqls = [];
        foreach (Connection::select($act->indexTableName(), $act->schema(), '*', ['object' => $act->object]) as $r) {
            $id = $r['id']->uuid();
            if ($act->id === $id) {
                continue;
            }
            foreach ($insArr as $i) {
                $cqls[] = Connection::delete($i->tableName(), $t->schema(), ['id' => $i->id, 'activity_id' => $id], true);
            }
        }
        return array_unique($cqls);
    }

}
