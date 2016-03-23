<?php

class ConnectionTest extends PHPUnit_Framework_TestCase
{

    public function testConnectionEmptyConfig()
    {
        $connection = \Newsfeed\Connection::connection();
        $this->assertEquals(null, $connection);
    }

}
