<?php

class ActivityTest extends PHPUnit_Framework_TestCase
{

    private static $config = [
        'hosts' => ['127.0.0.1'],
        'port' => 9042,
        'keyspace' => 'php_newsfeed_test'
    ];

    public function testClassAttributes()
    {
        $this->assertClassHasAttribute('id', '\Newsfeed\Activity');
        $this->assertClassHasAttribute('content', '\Newsfeed\Activity');
        $this->assertClassHasAttribute('object', '\Newsfeed\Activity');
        $this->assertClassHasAttribute('time', '\Newsfeed\Activity');
    }

    public function testClassFunctionsToArray()
    {
        $arr = (new \Newsfeed\Activity())->toArray();
        $this->assertNotEmpty($arr);
        $this->assertArrayHasKey('id', $arr);
        $this->assertArrayHasKey('content', $arr);
        $this->assertArrayHasKey('object', $arr);
        $this->assertArrayHasKey('time', $arr);
    }

    public function testClassFunctionsSave()
    {
        $this->prepareData();
        $act = new \Newsfeed\Activity();
        $saved = $act->save();
        $this->assertTrue($saved);
    }

    public function testGetNonExistedAct()
    {
        $this->prepareData();
        $act = \Newsfeed\Activity::find('e4eaaaf2-d142-11e1-b3e4-080027620cdd');
        $this->assertNull($act);
    }

    public function testGetExistedAct()
    {
        $this->prepareData();
        $act = new \Newsfeed\Activity();
        $act->save();
        $getAct = \Newsfeed\Activity::find($act->id);
        $this->assertNotNull($getAct);
        $this->assertSame($act->id, $getAct->id);
        $this->assertSame($act->content, $getAct->content);
        $this->assertSame($act->object, $getAct->object);
        $this->assertSame($act->time, $getAct->time);
    }

    public function testAddNewActWithoutObject()
    {
        $this->prepareData();
        $content = 'user 1 post photo 1';
        $act = new \Newsfeed\Activity(['content' => $content]);
        $saved = $act->save();
        $this->assertTrue($saved);
        $this->assertNotEmpty($act->id);
        $this->assertRegExp('/^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$/i', $act->id);
        $this->assertSame($content, $act->content);
        $this->assertNull($act->object);
        $this->assertNotEmpty($act->time);
        $getAct = \Newsfeed\Activity::find($act->id);
        $this->assertNotNull($getAct);
        $this->assertSame($act->id, $getAct->id);
        $this->assertSame($act->content, $getAct->content);
        $this->assertSame($act->object, $getAct->object);
        $this->assertSame($act->time, $getAct->time);
    }

    public function testAddNewActWithObject()
    {
        $this->prepareData();
        $content = 'user 1 post photo 1';
        $object = 'photo 1';
        $act = new \Newsfeed\Activity(['content' => $content, 'object' => $object]);
        $saved = $act->save();
        $this->assertTrue($saved);
        $this->assertNotEmpty($act->id);
        $this->assertRegExp('/^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$/i', $act->id);
        $this->assertSame($content, $act->content);
        $this->assertSame($object, $act->object);
        $this->assertNotEmpty($act->time);
        $getAct = \Newsfeed\Activity::find($act->id);
        $this->assertNotNull($getAct);
        $this->assertSame($act->id, $getAct->id);
        $this->assertSame($act->content, $getAct->content);
        $this->assertSame($act->object, $getAct->object);
        $this->assertSame($act->time, $getAct->time);
    }

    public function testDeleteActWithoutObject()
    {
        $this->prepareData();
        $content = 'user 1 post photo 1';
        $act = new \Newsfeed\Activity(['content' => $content]);
        $act->save();
        $getAct = \Newsfeed\Activity::find($act->id);
        $this->assertNotNull($getAct);
        $this->assertSame($act->id, $getAct->id);
        $this->assertSame($act->content, $getAct->content);
        $this->assertSame($act->object, $getAct->object);
        $this->assertSame($act->time, $getAct->time);
        $act->delete();
        $getAct = \Newsfeed\Activity::find($act->id);
        $this->assertNull($getAct);
    }

    public function testDeleteActWithObject()
    {
        $this->prepareData();
        $content = 'user 1 post photo 1';
        $object = 'photo 1';
        $act = new \Newsfeed\Activity(['content' => $content, 'object' => $object]);
        $act->save();
        $getAct = \Newsfeed\Activity::find($act->id);
        $this->assertNotNull($getAct);
        $this->assertSame($act->id, $getAct->id);
        $this->assertSame($act->content, $getAct->content);
        $this->assertSame($act->object, $getAct->object);
        $this->assertSame($act->time, $getAct->time);
        $act->delete();
        $getAct = \Newsfeed\Activity::find($act->id);
        $this->assertNull($getAct);
    }

    protected function prepareData()
    {
        \Newsfeed\Connection::$config = self::$config;
        \Newsfeed\Connection::execCql('TRUNCATE ' . \Newsfeed\Activity::tableName());
        \Newsfeed\Connection::execCql('TRUNCATE ' . \Newsfeed\Activity::indexTableName());
    }

}
