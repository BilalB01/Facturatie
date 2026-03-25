<?php
require_once __DIR__ . '/vendor/autoload.php';
require_once __DIR__ . '/load.php';

$di = include PATH_ROOT . '/di.php';
$di['translate']();

try {
    $id = $di['api_admin']->client_create([
        'first_name' => 'Test',
        'last_name' => 'Gebruiker',
        'email' => 'test' . rand(100,999) . '@ehb.be',
        'password' => 'Test@123456!'
    ]);
    echo "SUCCES: Klant succesvol aangemaakt! De nieuwe ID is: " . $id . "\n";
} catch (\Exception $e) {
    echo "FOUT: " . $e->getMessage() . "\n";
}
