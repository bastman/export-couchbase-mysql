#!/usr/bin/env php
<?php

require_once(dirname(__FILE__) . '/../vendor/autoload.php');

set_error_handler(function ($errno, $errstr, $errfile, $errline) {
    throw new \ErrorException($errstr, 0, $errno, $errfile, $errline);
});

use Symfony\Component\Console\Application;
use Cb2Mysql\Command\ExportCouchbaseMysqlCommand;
use Cb2Mysql\Command\ExportCouchbaseMysqlCustomCommand;
use Cb2Mysql\Command\ExportRaw;
$console = new Application();
$console->add(new ExportCouchbaseMysqlCommand());
$console->add(new ExportCouchbaseMysqlCustomCommand());
$console->add(new ExportRaw());
$console->run();