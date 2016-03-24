<?php

class ConnectionTest extends PHPUnit_Framework_TestCase
{

    public function testConnectionEmptyConfig()
    {
        \Newsfeed\Connection::$config = [];
        \Newsfeed\Connection::close();
        $connection = \Newsfeed\Connection::connection();
        $this->assertEquals(null, $connection);
    }

}
