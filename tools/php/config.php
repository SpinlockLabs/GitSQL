<?php
$gitsql_config_path = getcwd() . "/../../work/gitsql-php.ini";
$config = parse_ini_file($gitsql_config_path, true);

if (!$config) {
    throw new RuntimeException('Failed to load configuration.');
}
