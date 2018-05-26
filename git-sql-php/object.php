<?php
require_once 'utils.php';

$conn = get_conn(load_get_param("repo"));
if ($conn == null) {
    die_not_exists('Repository not found.');
}

$obj = git_load_raw_obj($conn, load_get_param('hash'));
if ($obj != null) {
    header('Content-Type: application/octet-stream');
    $raw = load_get_param("raw", true, "false");
    if ($raw == "true") {
        echo $obj;
    } else {
        echo gzcompress($obj);
    }
} else {
    die_not_exists("Object not found.");
}
