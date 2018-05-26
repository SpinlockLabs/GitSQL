<?php
$uri = $_SERVER['REQUEST_URI'];
$url = parse_url($uri);
$path = $url['path'];

$match = null;
if (preg_match(
    '/\/([^\/]+)\/objects\/([0-9a-f]{2})\/([0-9a-f]+)$/',
    $path, $match
)) {
    $_GET['repo'] = $match[1];
    $_GET['hash'] = $match[2] . $match[3];
    require_once 'object.php';
} else if (preg_match(
    '/\/([^\/]+)\/info\/refs$/',
    $path, $match
)) {
    $_GET['repo'] = $match[1];
    require_once 'refs.php';
} else if (preg_match(
    '/\/([^\/]+)\/HEAD$/',
    $path, $match
)) {
    $_GET['repo'] = $match[1];
    require_once 'head.php';
} else {
    http_response_code(404);
}
