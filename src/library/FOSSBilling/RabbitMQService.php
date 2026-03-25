<?php

declare(strict_types=1);
/**
 * Copyright 2022-2025 FOSSBilling
 * Copyright 2011-2021 BoxBilling, Inc.
 * SPDX-License-Identifier: Apache-2.0.
 *
 * @copyright FOSSBilling (https://www.fossbilling.org)
 * @license http://www.apache.org/licenses/LICENSE-2.0 Apache-2.0
 */

namespace FOSSBilling;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

class RabbitMQService
{
    private ?AMQPStreamConnection $connection = null;
    private ?AMQPChannel $channel = null;

    private string $host;
    private int $port;
    private string $user;
    private string $password;
    private string $vhost;
    private string $exchange;

    /** @var array<string, string> */
    private array $queues;

    /** @var array<string, string> */
    private array $schemaPaths;

    public function __construct(?array $config = null)
    {
        $config ??= [];

        $this->host = (string) ($config['host'] ?? getenv('RABBITMQ_HOST') ?: 'rabbitmq');
        $this->port = (int) ($config['port'] ?? getenv('RABBITMQ_PORT') ?: 5672);
        $this->user = (string) ($config['user'] ?? getenv('RABBITMQ_USER') ?: getenv('RABBITMQ_DEFAULT_USER') ?: 'devuser');
        $this->password = (string) ($config['password'] ?? getenv('RABBITMQ_PASSWORD') ?: getenv('RABBITMQ_DEFAULT_PASS') ?: 'devpass');
        $this->vhost = (string) ($config['vhost'] ?? getenv('RABBITMQ_VHOST') ?: '/');
        $this->exchange = (string) ($config['exchange'] ?? getenv('RABBITMQ_EXCHANGE') ?: 'ehb.events');

        $defaultSchemaPath = dirname(__DIR__, 2) . '/data/contracts/facturatie_contract.xsd';

        $this->queues = $config['queues'] ?? [
            'facturatie.invoice.finalized' => 'facturatie.invoice.finalized',
        ];

        $this->schemaPaths = $config['schema_paths'] ?? [
            'facturatie.invoice.finalized' => $defaultSchemaPath,
        ];
    }

    public function publishXML(string $routingKey, string $xml): void
    {
        $schemaPath = $this->schemaPaths[$routingKey] ?? null;
        if ($schemaPath === null) {
            throw new \InvalidArgumentException(sprintf('No XML schema configured for routing key "%s".', $routingKey));
        }

        $this->validateXml($xml, $schemaPath);

        $message = new AMQPMessage($xml, [
            'content_type' => 'application/xml',
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
        ]);

        $this->getChannel()->basic_publish($message, $this->exchange, $routingKey);
    }

    public function getChannel(): AMQPChannel
    {
        if ($this->channel instanceof AMQPChannel) {
            return $this->channel;
        }

        $this->connection = new AMQPStreamConnection(
            $this->host,
            $this->port,
            $this->user,
            $this->password,
            $this->vhost
        );

        $this->channel = $this->connection->channel();
        $this->channel->exchange_declare($this->exchange, 'topic', false, true, false);

        foreach ($this->queues as $routingKey => $queueName) {
            $this->channel->queue_declare($queueName, false, true, false, false);
            $this->channel->queue_bind($queueName, $this->exchange, $routingKey);
        }

        return $this->channel;
    }

    public function close(): void
    {
        if ($this->channel instanceof AMQPChannel) {
            $this->channel->close();
            $this->channel = null;
        }

        if ($this->connection instanceof AMQPStreamConnection) {
            $this->connection->close();
            $this->connection = null;
        }
    }

    public function __destruct()
    {
        $this->close();
    }

    private function validateXml(string $xml, string $schemaPath): void
    {
        if (!is_file($schemaPath)) {
            throw new \RuntimeException(sprintf('XSD schema file not found: %s', $schemaPath));
        }

        $dom = new \DOMDocument('1.0', 'UTF-8');
        $previousUseInternalErrors = libxml_use_internal_errors(true);

        try {
            if (!$dom->loadXML($xml, LIBXML_NONET)) {
                throw new \InvalidArgumentException('Invalid XML payload: ' . $this->formatLibxmlErrors());
            }

            if (!$dom->schemaValidate($schemaPath)) {
                throw new \InvalidArgumentException('XML does not match schema: ' . $this->formatLibxmlErrors());
            }
        } finally {
            libxml_clear_errors();
            libxml_use_internal_errors($previousUseInternalErrors);
        }
    }

    private function formatLibxmlErrors(): string
    {
        $errors = libxml_get_errors();
        if ($errors === []) {
            return 'Unknown libxml validation error';
        }

        $messages = [];
        foreach ($errors as $error) {
            $messages[] = trim($error->message);
        }

        return implode(' | ', array_unique($messages));
    }
}
