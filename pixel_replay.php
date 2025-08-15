#!/usr/bin/env php
<?php
/**
 * Facebook Pixel Event Replay Tool
 * 
 * Backfills historical purchase events to Facebook's Conversions API for improved
 * ad attribution and audience building. Supports both website and offline event modes
 * with configurable time windows and data sources.
 * 
 * Dependencies (install via Composer):
 *   - guzzlehttp/guzzle: ^7.5 (HTTP client for Facebook API)
 *   - league/csv: ^9.8 (CSV file processing)
 *   - PHP 8.0+ with PDO-ODBC extension (for Snowflake connectivity)
 * 
 * Installation:
 *   composer install
 * 
 * Usage Examples:
 *   Web Events (7-day window):
 *     php pixel_replay.php --mode=web7d --pixel_id=123456 --access_token=xyz --csv=data.csv
 * 
 *   Offline Events (62-day window):
 *     php pixel_replay.php --mode=offline62d --dataset_id=789 --access_token=xyz --csv=data.csv
 * 
 *   Snowflake Data Source:
 *     php pixel_replay.php --mode=web7d --pixel_id=123456 --access_token=xyz \
 *       --snowflake_dsn=mydsn --snowflake_user=user --snowflake_pass=pass \
 *       --sql="SELECT * FROM purchases WHERE date >= '2024-01-01'"
 * 
 * @author Luis B. Mata <mataluis2k@gmail.com>
 * @version 1.0.0
 * @requires PHP >= 8.0
 */
declare(strict_types=1);

// Ensure dependencies are installed
if (!file_exists(__DIR__ . '/vendor/autoload.php')) {
    fwrite(STDERR, "Dependencies not installed. Run: composer install\n");
    exit(1);
}

require __DIR__ . '/vendor/autoload.php';

use League\Csv\Reader;
use League\Csv\Statement;
use GuzzleHttp\Client;

function arg(string $key, $default=null) {
    foreach ($GLOBALS['argv'] as $a) {
        if (strpos($a, "--{$key}=") === 0) return substr($a, strlen($key)+3);
    }
    return $default;
}
function fail(string $m, int $c=1){ fwrite(STDERR, $m.PHP_EOL); exit($c); }
function logInfo(string $msg): void { fwrite(STDERR, "[INFO] " . date('Y-m-d H:i:s') . " - {$msg}\n"); }
function logProgress(int $current, int $total, string $label = 'Progress'): void {
    if ($total > 0) {
        $percent = round(($current / $total) * 100, 1);
        fwrite(STDERR, "[PROGRESS] {$label}: {$current}/{$total} ({$percent}%)\n");
    }
}

$mode         = arg('mode');                  // web7d | offline62d
$pixelId      = arg('pixel_id');
$datasetId    = arg('dataset_id');
$accessToken  = arg('access_token');
$apiVersion   = arg('api_version', 'v20.0');
$csvPath      = arg('csv');
$dsn          = arg('snowflake_dsn');
$sfUser       = arg('snowflake_user');
$sfPass       = arg('snowflake_pass');
$sql          = arg('sql');
$batchSize    = (int) arg('batch', '400');
$test         = (int) arg('test', '1');       // web7d only
$timeout      = (int) arg('timeout', '30');
$strictAttr   = (int) arg('strict_attr', '1'); // require fbc/fbp/fbclid/utm_source

if (!$mode || !$accessToken) fail("Missing --mode or --access_token");
if ($mode === 'web7d' && !$pixelId) fail("web7d requires --pixel_id");
if ($mode === 'offline62d' && !$datasetId) fail("offline62d requires --dataset_id");
if (!$csvPath && !($dsn && $sfUser && $sfPass && $sql)) {
    fail("Provide --csv=PATH or Snowflake flags: --snowflake_dsn --snowflake_user --snowflake_pass --sql=\"...\"");
}

$http = new Client([
    'base_uri' => "https://graph.facebook.com/{$apiVersion}/",
    'timeout'  => $timeout,
]);

function sha256_lower(string $v): string { return hash('sha256', trim(mb_strtolower($v))); }
function validateEmail(?string $email): ?string {
    if (!$email) return null;
    $email = trim($email);
    return filter_var($email, FILTER_VALIDATE_EMAIL) ? $email : null;
}
function validateNumeric(?string $value): ?float {
    if ($value === null || $value === '') return null;
    if (!is_numeric($value)) return null;
    $num = (float)$value;
    return $num > 0 ? $num : null;
}
function normalizePhone(?string $p): ?string {
    if (!$p) return null;
    $digits = preg_replace('/\D+/', '', $p);
    if (!$digits || $digits === '' || strlen($digits) < 10) return null;
    
    // US/Canada numbers starting with 1
    if ($digits[0] === '1' && strlen($digits) === 11) return '+'.$digits;
    
    // International numbers - always use cleaned digits with +
    return '+'.$digits;
}
function ts($v): ?int {
    if ($v === null || $v === '') return null;
    if (is_numeric($v)) return (int)$v;
    $t = strtotime((string)$v);
    return $t ?: null;
}
function inWindow(int $eventTime, string $window): bool {
    $limit = match ($window) {
        '7d'  => strtotime('-7 days'),
        '62d' => strtotime('-62 days'),
        default => 0
    };
    return $eventTime >= $limit && $eventTime <= time();
}
function eventId(string $namespace, string $orderId): string {
    // Use full SHA256 to prevent collisions with large datasets
    return hash('sha256', $namespace.'|Purchase|'.$orderId);
}

function readCsvRows(string $path): iterable {
    $csv = Reader::createFromPath($path, 'r');
    $csv->setHeaderOffset(0);
    $stmt = Statement::create();
    foreach ($stmt->process($csv) as $row) yield array_change_key_case($row, CASE_LOWER);
}
function readSnowflakeRows(string $dsn, string $user, string $pass, string $sql): iterable {
    $pdo = new PDO("odbc:DSN={$dsn}", $user, $pass, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    ]);
    $stmt = $pdo->query($sql);
    while ($row = $stmt->fetch()) yield array_change_key_case($row, CASE_LOWER);
}

/* -------- builders -------- */

function buildWebEvent(array $r, string $pixelId, bool $strictAttr): ?array {
    $orderId  = trim($r['order_id'] ?? $r['id'] ?? '');
    $email    = validateEmail($r['email'] ?? null);
    $phoneRaw = $r['phone'] ?? null;
    $zip      = trim($r['zip'] ?? $r['zipcode'] ?? '');
    $value    = validateNumeric($r['value'] ?? $r['amount'] ?? null);
    $currency = strtoupper(trim($r['currency'] ?? 'USD'));
    $fbc      = trim($r['fbc'] ?? '');
    $fbp      = trim($r['fbp'] ?? '');
    $ip       = $r['ip'] ?? $r['ip_address'] ?? null;
    $ua       = $r['user_agent'] ?? null;
    $source   = $r['event_source_url'] ?? $r['source_url'] ?? null;
    $etime    = ts($r['event_time'] ?? $r['created_at'] ?? null);

    if (!$orderId || !$value || !$etime) return null;
    if (!inWindow($etime, '7d')) return null;

    if ($strictAttr) {
        $hasAttr = !empty($fbc) || !empty($fbp) || !empty($r['fbclid']) || !empty($r['utm_source']);
        if (!$hasAttr) return null;
    }

    $ud = [];
    if ($email)  $ud['em'] = [ sha256_lower($email) ];
    if (!empty($phoneRaw)){
        $phone = normalizePhone($phoneRaw);
        if ($phone) $ud['ph'] = [ sha256_lower($phone) ];
    }
    if (!empty($zip))    $ud['zp'] = [ sha256_lower($zip) ];
    if (!empty($ip))     $ud['client_ip_address'] = $ip;
    if (!empty($ua))     $ud['client_user_agent'] = $ua;
    if (!empty($fbc))    $ud['fbc'] = $fbc;
    if (!empty($fbp))    $ud['fbp'] = $fbp;

    $cd = [
        'currency' => $currency,
        'value'    => $value, // Already validated as float
        'order_id' => $orderId,
    ];

    $payload = [
        'event_name'       => 'Purchase',
        'event_time'       => $etime,
        'action_source'    => 'website',
        'event_source_url' => $source ?: null,
        'event_id'         => eventId($pixelId, (string)$orderId),
        'user_data'        => $ud,
        'custom_data'      => $cd,
    ];
    return array_filter($payload, fn($v) => !is_null($v));
}

function buildOfflineEvent(array $r, string $datasetId): ?array {
    $orderId  = trim($r['order_id'] ?? $r['id'] ?? '');
    $email    = validateEmail($r['email'] ?? null);
    $phoneRaw = $r['phone'] ?? null;
    $zip      = trim($r['zip'] ?? $r['zipcode'] ?? '');
    $value    = validateNumeric($r['value'] ?? $r['amount'] ?? null);
    $currency = strtoupper(trim($r['currency'] ?? 'USD'));
    $ip       = $r['ip'] ?? $r['ip_address'] ?? null;
    $ua       = $r['user_agent'] ?? null;
    $etime    = ts($r['event_time'] ?? $r['created_at'] ?? null);

    if (!$orderId || !$value || !$etime) return null;
    if (!inWindow($etime, '62d')) return null;

    $mk = [];
    if ($email)  $mk['em'] = sha256_lower($email);
    if (!empty($phoneRaw)){
        $phone = normalizePhone($phoneRaw);
        if ($phone) $mk['ph'] = sha256_lower($phone);
    }
    if (!empty($zip))    $mk['zp'] = sha256_lower($zip);
    if (!empty($ip))     $mk['client_ip_address'] = $ip;
    if (!empty($ua))     $mk['client_user_agent'] = $ua;

    return [
        'event_name'     => 'Purchase',
        'event_time'     => $etime,
        'value'          => $value, // Already validated as float
        'currency'       => $currency,
        'order_id'       => $orderId,
        'match_keys'     => $mk,
        'event_id'       => eventId($datasetId, $orderId),
        'action_source'  => 'other',
    ];
}

/* -------- senders -------- */

function postWebBatch(Client $http, string $pixelId, string $token, array $events, int $test): array {
    $body = [
        'access_token' => $token,
        'data'         => array_values($events),
    ];
    if ($test) $body['test_event_code'] = 'TEST_' . substr(hash('crc32b', $pixelId), 0, 8);
    
    $res = $http->post("{$pixelId}/events", ['json' => $body]);
    $response = json_decode((string)$res->getBody(), true) ?? [];
    
    // Validate response for errors
    if (isset($response['error'])) {
        throw new Exception("Facebook API Error: {$response['error']['message']} (Code: {$response['error']['code']})");
    }
    
    // Log warnings if present
    if (!empty($response['messages'])) {
        foreach ($response['messages'] as $msg) {
            if ($msg['type'] === 'warning') {
                fwrite(STDERR, "API Warning: {$msg['message']}\n");
            }
        }
    }
    
    return $response;
}

function postOfflineBatch(Client $http, string $datasetId, string $token, array $events, string $uploadTag='offline_backfill'): array {
    $body = [
        'access_token' => $token,
        'upload_tag'   => $uploadTag,
        'data'         => array_values($events),
    ];
    
    $res = $http->post("{$datasetId}/events", ['json' => $body]);
    $response = json_decode((string)$res->getBody(), true) ?? [];
    
    // Validate response for errors
    if (isset($response['error'])) {
        throw new Exception("Facebook API Error: {$response['error']['message']} (Code: {$response['error']['code']})");
    }
    
    // Log warnings if present
    if (!empty($response['messages'])) {
        foreach ($response['messages'] as $msg) {
            if ($msg['type'] === 'warning') {
                fwrite(STDERR, "API Warning: {$msg['message']}\n");
            }
        }
    }
    
    return $response;
}

/* -------- source rows -------- */

logInfo("Starting Facebook Pixel Replay - Mode: {$mode}");
logInfo("Batch size: {$batchSize}, Timeout: {$timeout}s, Test mode: " . ($test && $mode === 'web7d' ? 'ON' : 'OFF'));

$iter = $csvPath ? readCsvRows($csvPath) : readSnowflakeRows($dsn, $sfUser, $sfPass, $sql);

/* -------- main loop -------- */

$sent=0; $kept=0; $skipped=0; $failed=0; $batch=[]; $processed=0;
$startTime = time();

foreach ($iter as $row) {
    $processed++;
    $evt = null;
    if ($mode === 'web7d')       $evt = buildWebEvent($row, $pixelId, (bool)$strictAttr);
    if ($mode === 'offline62d')  $evt = buildOfflineEvent($row, $datasetId);
    if (!$evt) { $skipped++; continue; }
    $batch[] = $evt; $kept++;

    if (count($batch) >= $batchSize) {
        try {
            logInfo("Sending batch of " . count($batch) . " events (Processed: {$processed}, Kept: {$kept}, Skipped: {$skipped})");
            if ($mode === 'web7d')      postWebBatch($http, $pixelId, $accessToken, $batch, $test);
            if ($mode === 'offline62d') postOfflineBatch($http, $datasetId, $accessToken, $batch);
            $sent += count($batch);
            logInfo("Batch sent successfully - Total sent: {$sent}");
        } catch (Throwable $e) { 
            $failed += count($batch); 
            fwrite(STDERR, "[ERROR] Batch failed: {$e->getMessage()}\n");
        }
        $batch=[]; usleep(250000);
    }
}

// Send final batch if any events remain
if (!empty($batch)) {
    try {
        logInfo("Sending final batch of " . count($batch) . " events");
        if ($mode === 'web7d')      postWebBatch($http, $pixelId, $accessToken, $batch, $test);
        if ($mode === 'offline62d') postOfflineBatch($http, $datasetId, $accessToken, $batch);
        $sent += count($batch);
        logInfo("Final batch sent successfully");
    } catch (Throwable $e) { 
        $failed += count($batch); 
        fwrite(STDERR, "[ERROR] Final batch failed: {$e->getMessage()}\n");
    }
}

$endTime = time();
$duration = $endTime - $startTime;
logInfo("Processing completed in {$duration} seconds");

echo json_encode([
    'mode'      => $mode,
    'processed' => $processed,
    'kept'      => $kept,
    'sent'      => $sent,
    'skipped'   => $skipped,
    'failed'    => $failed,
    'test'      => $test && $mode === 'web7d',
    'duration'  => $duration,
    'success'   => $failed === 0,
], JSON_PRETTY_PRINT) . PHP_EOL;
