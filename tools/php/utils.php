<?php
require_once 'config.php';

function die_not_exists($msg) {
    http_response_code(404);
    die($msg . "\n");
}

function die_bad_request($msg) {
    http_response_code(400);
    die($msg . "\n");
}

function die_server_error($msg) {
    http_response_code(500);
    die($msg . "\n");
}

function load_get_param($name, $required = true, $default = null) {
    $value = $_GET[$name];

    if ($value == null && $default != null) {
        $value = $default;
    }

    if ($value == null && $required) {
        die_bad_request("Parameter " . $name . " is missing.");
    }
    return $value;
}

function get_conn($repo_name) {
    global $config;

    if ($repo_name == null) {
        http_response_code(404);
        die_not_exists('Repository not found.');
    }

    $repos = $config['serve'];
    $db_name = $repos[$repo_name];

    if ($db_name == null) {
        http_response_code(404);
        die_not_exists('Repository not found.');
    }

    $postgres = $config['postgres'];
    $str = "dbname=" . $db_name;
    foreach ($postgres as $key => $value) {
        $str .= " " . $key . "=" . $value;
    }
    $db = pg_connect($str);
    return $db;
}

function git_load_raw_obj($db, $hash) {
    $hash_needed = pg_escape_string($hash);
    $result = pg_query(
        $db,
        "SELECT content FROM objects WHERE hash = '" . $hash_needed . "'"
    ) or die_not_exists('Failed to query database.');

    $row = pg_fetch_array($result, null, PGSQL_ASSOC);
    if ($row) {
        return pg_unescape_bytea($row['content']);
    } else {
        return null;
    }
}

function git_load_obj($db, $hash) {
    $hash_needed = pg_escape_string($hash);
    $result = pg_query(
        $db,
        "SELECT content FROM contents WHERE hash = '" . $hash_needed . "'"
    ) or die_not_exists('Failed to query database.');

    $row = pg_fetch_array($result, null, PGSQL_ASSOC);
    if ($row) {
        return pg_unescape_bytea($row['content']);
    } else {
        return null;
    }
}

function git_load_refs($db) {
    $result = pg_query(
      $db,
      "SELECT name, target FROM refs"
    ) or die_server_error('Failed to query database for refs.');

    return $result;
}
