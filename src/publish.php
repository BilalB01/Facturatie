<?php

require_once __DIR__ . '/vendor/autoload.php';

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

$host = 'rabbitmq'; 
$user = 'devuser';
$pass = 'devpass';

try {
    $connection = new AMQPStreamConnection($host, 5672, $user, $pass);
    $channel = $connection->channel();

    $exchange = 'ehb.events';
    $routingKey = 'kassa.closing.finalized';

    $channel->exchange_declare($exchange, 'topic', false, true, false);

    $xmlData = <<<XML
<?xml version="1.0" encoding="UTF-8"?>
<KassaBatch>
    <batchId>batch-1001</batchId>
    <closedAt>2026-05-14T23:59:59Z</closedAt>
    <users>
        <user>
            <userId>contact-9001</userId>
            <transactions>
                <transaction>
                    <description>Drankbonnen receptie</description>
                    <amount>8</amount>
                </transaction>
            </transactions>
        </user>
        <user>
            <userId>contact-9002</userId>
            <transactions>
                <transaction>
                    <description>Catering snackbox</description>
                    <amount>15</amount>
                </transaction>
            </transactions>
        </user>
    </users>
</KassaBatch>
XML;

    $msg = new AMQPMessage($xmlData);
    $channel->basic_publish($msg, $exchange, $routingKey);

    echo " [x] Sent 'kassa.closing.finalized' message\n";

    $channel->close();
    $connection->close();

} catch (\Exception $e) {
    echo "Fout: " . $e->getMessage() . "\n";
}
