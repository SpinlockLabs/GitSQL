<?php
require_once 'utils.php';

$conn = get_conn(load_get_param('repo'));
$found = false;

header('Content-Type: text/plain');

foreach (pg_fetch_all(git_load_refs($conn)) as $row) {
    if ($row['name'] == 'HEAD') {
        $found = true;
        $target = $row['target'];
        echo $target . "\n";
        break;
    }
}

if (!$found) {
    http_response_code(404);
}
