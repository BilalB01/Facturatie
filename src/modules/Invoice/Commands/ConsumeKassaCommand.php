<?php

namespace Box\Mod\Invoice\Commands;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use PhpAmqpLib\Message\AMQPMessage;

class ConsumeKassaCommand extends Command
{
    private $di;

    public function setDi($di)
    {
        $this->di = $di;
    }

    protected function configure()
    {
        $this
            ->setName('invoice:consume-kassa')
            ->setDescription('Luistert naar Kassa XML batches (kassa.closing.finalized) op RabbitMQ en bundelt alle consumpties per bedrijf in facturen.');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        // Volgens de documentatie de correcte routing key en queue voor de kassa batch
        $queueName = 'kassa.closing.finalized';
        $routingKey = 'kassa.closing.finalized';
        
        $config = [
            'queues' => [
                $routingKey => $queueName
            ]
        ];

        try {
            $rabbitMQService = new \FOSSBilling\RabbitMQService($config);
            $channel = $rabbitMQService->getChannel();

            $output->writeln(" [*] Wachten op kassa transacties in '{$queueName}'. Druk op CTRL+C om te stoppen.");

            $callback = function (AMQPMessage $msg) use ($output, $rabbitMQService) {
                $output->writeln(' [x] KassaBatch XML ontvangen.');
                
                try {
                    $xml = simplexml_load_string($msg->getBody());
                    if (!$xml || $xml->getName() !== 'KassaBatch') {
                        throw new \Exception("Ongeldige XML root of geen XML. Verwacht KassaBatch.");
                    }

                    $groupedTransactions = [];
                    $userNodes = isset($xml->users->user) ? $xml->users->user : $xml->user;

                    // 1. Loop over alle gebruikers uit de kassa batch
                    foreach ($userNodes as $nodeUser) {
                        $userId = (string) $nodeUser->userId;
                        
                        // Zoek gebruiker in de FOSSBilling database op interne id, Salesforce aid of custom_1
                        $userClient = $this->di['db']->findOne('Client', 'id = ? OR aid = ? OR custom_1 = ?', [$userId, $userId, $userId]);
                        
                        if (!$userClient) {
                            $output->writeln(" [!] Waarschuwing: Kan medewerker/particulier met geselecteerde ID {$userId} niet vinden in de database. Overslaan.");
                            continue;
                        }

                        // Bepaal de bedrijfs-ID waartoe deze individuele gebruiker behoort
                        $companyId = $userClient->company_id; 
                        if (!$companyId && !empty($userClient->company)) {
                            $companyId = $userClient->company; 
                        }

                        if (!$companyId) {
                            $output->writeln(" [!] Info: Gebruiker {$userId} is een particulier zonder bedrijf. Consumpties worden ter plaatse verrekend, geen factuur vereist.");
                            continue;
                        }

                        // Leg de associatie tussen gebruiker en bedrijfsfactuur vast indien nog niet gebeurd
                        if (!isset($groupedTransactions[$companyId])) {
                            // Zoek het profiel van het uiteindelijke bedrijf dat zal betalen
                            $companyClient = $this->di['db']->findOne('Client', "type = 'company' AND (id = ? OR aid = ? OR custom_1 = ?)", [$companyId, $companyId, $companyId]);
                            
                            $billingClientId = $companyClient ? $companyClient->id : $userClient->id;

                            $groupedTransactions[$companyId] = [
                                'billing_client_id' => $billingClientId,
                                'items' => []
                            ];
                        }

                        $txNodes = isset($nodeUser->transactions->transaction) ? $nodeUser->transactions->transaction : $nodeUser->transactions;

                        // Voeg elke gedronken/gegeten consumptie toe aan het totaalplaatje van dit bedrijf
                        foreach ($txNodes as $tx) {
                            $desc = (string) $tx->description;
                            $amt = (float) $tx->amount;
                            
                            $userName = trim($userClient->first_name . ' ' . $userClient->last_name);
                            $groupedTransactions[$companyId]['items'][] = [
                                'title' => $desc . " (Medewerker: " . $userName . ")",
                                'price' => $amt,
                                'quantity' => 1
                            ];
                        }
                    }

                    $apiAdmin = $this->di['api_admin'];

                    // 2. Genereer en verzend de factuur PER bedrijf!
                    foreach ($groupedTransactions as $data) {
                        if (empty($data['items'])) continue;

                        $billingClientId = $data['billing_client_id'];
                        
                        // Maak letterlijk de lege conceptfactuur aan
                        $invoiceId = $apiAdmin->invoice_prepare(['client_id' => $billingClientId]);
                        $output->writeln(" [v] Factuur concept #{$invoiceId} gecreëerd voor klant met id {$billingClientId}.");

                        // Voeg alle samengestelde drankjes/consumpties van alle medewerkers erbovenop!
                        foreach ($data['items'] as $item) {
                            $apiAdmin->invoice_update([
                                'id' => $invoiceId,
                                'new_item' => [
                                    'title' => $item['title'],
                                    'price' => $item['price'],
                                    'quantity' => $item['quantity'],
                                    'taxed' => false
                                ]
                            ]);
                        }

                        // Keur de bedrijfsfactuur officieel goed conform correctiebeleid!
                        $apiAdmin->invoice_approve(['id' => $invoiceId]);
                        $output->writeln(" [v] Volledige factuur #{$invoiceId} bewaard inclusief " . count($data['items']) . " lijnen!");

                        // 3. Informeer het Mailing Team dat de PDF factuur klaarstaat ("facturatie.invoice.finalized")
                        try {
                            $finalizedXml = $this->buildInvoiceFinalizedXml($invoiceId);
                            $rabbitMQService->publishXML('facturatie.invoice.finalized', $finalizedXml);
                            $output->writeln(" [v] Outbound synchronisatie: 'facturatie.invoice.finalized' verstuurd naar Team Mailing.");
                        } catch (\Exception $e) {
                             $output->writeln(" [!] Waarschuwing: Versturen van finalization zendbericht mislukt: " . $e->getMessage());
                        }
                    }

                    // Aangeven dat Kassa's bericht succesvol werd leeggezogen
                    $msg->ack();

                } catch (\Exception $e) {
                    $output->writeln(' [X] Verwerkingsfout: ' . $e->getMessage());
                    // Verwerp berichten als de data volledig corrupt is ter voorkoming van zombie loops
                    $msg->nack(false, false);
                }
            };

            $channel->basic_consume($queueName, '', false, false, false, false, $callback);

            while ($channel->is_open()) {
                $channel->wait();
            }

            $rabbitMQService->close();
            return Command::SUCCESS;

        } catch (\Exception $e) {
            $output->writeln("Kritieke fout: " . $e->getMessage());
            return Command::FAILURE;
        }
    }

    private function buildInvoiceFinalizedXml(int $invoiceId): string
    {
        // Laad benodigde entiteiten uit de database
        $invoice = $this->di['db']->load('invoice', $invoiceId);
        $client = $this->di['db']->load('client', $invoice->client_id);
        
        $invoiceNumber = $invoice->serie . sprintf('%05d', $invoice->nr);
        $email = $client->email;
        $total = number_format((float)$invoice->total, 2, '.', '');
        $hash = $invoice->hash;
        
        // Formatteer de URL link naar het openbare PDF document
        $pdfUrl = "http://localhost:8080/invoice/pdf/" . $hash; // Note: if FOSSBilling URL scheme differ, this assumes standard route.
        
        // Bouw de structuur exact na zoals in facturatie_contract.xsd beschreven is
        $xml = new \SimpleXMLElement('<?xml version="1.0" encoding="UTF-8"?><invoice_finalized></invoice_finalized>');
        $xml->addChild('invoiceNumber', htmlspecialchars($invoiceNumber));
        $xml->addChild('recipientEmail', htmlspecialchars($email));
        $xml->addChild('pdfUrl', htmlspecialchars($pdfUrl));
        $xml->addChild('totalAmount', $total);
        // Type van notification ("invoice_finalized") conform schema requirements
        $xml->addChild('type', 'invoice_finalized');
        
        return $xml->asXML();
    }
}
