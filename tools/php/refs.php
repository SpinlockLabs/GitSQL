<?php
require_once 'utils.php';

$conn = get_conn(load_get_param('repo'));

header('Content-Type: text/plain');
foreach (pg_fetch_all(git_load_refs($conn)) as $row) {
    echo $row['target'];
    echo "\t";
    echo $row['name'];
    echo "\n";
}
