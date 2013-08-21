#!/usr/bin/env php
<?php

require_once(dirname(__FILE__).'/../vendor/autoload.php');

set_error_handler(function ($errno, $errstr, $errfile, $errline) {
    throw new \ErrorException($errstr, 0, $errno, $errfile, $errline);
});

use Symfony\Component\Console\Application;

$console = new Application();
$console->add(new \Application\Command\exportCouchbaseMysqlCommand());
$console->add(new \Application\Command\exportCouchbaseMysqlCustomCommand());
$console->run();