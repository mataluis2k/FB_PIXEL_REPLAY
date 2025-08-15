# Facebook Pixel Event Replay Tool

A robust PHP tool for backfilling historical purchase events to Facebook's Conversions API, supporting both website and offline event modes.

## Features

- **Dual Mode Support**: Web events (7-day window) and Offline events (62-day window)
- **Multiple Data Sources**: CSV files and Snowflake database connectivity
- **Batch Processing**: Configurable batch sizes for optimal API performance
- **Data Validation**: Automatic time window filtering and data normalization
- **Error Handling**: Comprehensive error tracking and reporting
- **Privacy Compliant**: Automatic hashing of PII data (emails, phones, zip codes)

## Requirements

- PHP 8.0 or higher
- Composer for dependency management
- PDO with ODBC extension (for Snowflake connectivity)

## Installation

```bash
# Install dependencies
composer install

# For Snowflake support, ensure ODBC driver is installed
# Ubuntu/Debian: apt-get install unixodbc-dev
# CentOS/RHEL: yum install unixODBC-devel
```

## Usage

### Web Events (7-day attribution window)

```bash
# From CSV file
php pixel_replay.php \
  --mode=web7d \
  --pixel_id=YOUR_PIXEL_ID \
  --access_token=YOUR_ACCESS_TOKEN \
  --csv=purchases.csv

# Test mode (doesn't affect live data)
php pixel_replay.php \
  --mode=web7d \
  --pixel_id=YOUR_PIXEL_ID \
  --access_token=YOUR_ACCESS_TOKEN \
  --csv=purchases.csv \
  --test=1
```

### Offline Events (62-day attribution window)

```bash
php pixel_replay.php \
  --mode=offline62d \
  --dataset_id=YOUR_DATASET_ID \
  --access_token=YOUR_ACCESS_TOKEN \
  --csv=purchases.csv
```

### Snowflake Data Source

```bash
php pixel_replay.php \
  --mode=web7d \
  --pixel_id=YOUR_PIXEL_ID \
  --access_token=YOUR_ACCESS_TOKEN \
  --snowflake_dsn=your_dsn \
  --snowflake_user=username \
  --snowflake_pass=password \
  --sql="SELECT * FROM purchases WHERE created_at >= '2024-01-01'"
```

## Configuration Options

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| `--mode` | Operation mode: `web7d` or `offline62d` | ✅ | - |
| `--pixel_id` | Facebook Pixel ID (web7d mode) | ✅ | - |
| `--dataset_id` | Dataset ID (offline62d mode) | ✅ | - |
| `--access_token` | Facebook API access token | ✅ | - |
| `--csv` | Path to CSV data file | ⚠️* | - |
| `--snowflake_dsn` | Snowflake DSN | ⚠️* | - |
| `--snowflake_user` | Snowflake username | ⚠️* | - |
| `--snowflake_pass` | Snowflake password | ⚠️* | - |
| `--sql` | SQL query for Snowflake | ⚠️* | - |
| `--api_version` | Facebook API version | ❌ | v20.0 |
| `--batch` | Batch size for API requests | ❌ | 400 |
| `--timeout` | HTTP timeout in seconds | ❌ | 30 |
| `--test` | Test mode (web7d only) | ❌ | 1 |
| `--strict_attr` | Require attribution parameters | ❌ | 1 |

*Either `--csv` OR all Snowflake parameters are required

## Expected Data Format

### CSV/Database Columns (case-insensitive)

**Required:**
- `order_id` or `id` - Unique order identifier
- `value` or `amount` - Purchase value (numeric)
- `event_time` or `created_at` - Event timestamp

**Optional:**
- `email` - Customer email (will be hashed)
- `phone` - Customer phone (will be normalized and hashed)
- `zip` or `zipcode` - Customer zip code (will be hashed)
- `currency` - Currency code (default: USD)
- `ip` or `ip_address` - Customer IP address
- `user_agent` - Browser user agent
- `event_source_url` or `source_url` - Source URL (web events)
- `fbc` - Facebook click ID cookie
- `fbp` - Facebook browser ID cookie
- `fbclid` - Facebook click ID parameter
- `utm_source` - UTM source parameter

## Output

The tool provides a JSON summary of the processing results:

```json
{
    "mode": "web7d",
    "kept": 1250,
    "sent": 1200,
    "skipped": 50,
    "failed": 0,
    "test": true
}
```

## Error Handling

- **Skipped events**: Outside time window or missing required data
- **Failed events**: API errors or network issues
- **Processing continues**: Individual batch failures don't stop the entire process

## Security Notes

- All PII (emails, phones, zip codes) is automatically hashed using SHA256
- Access tokens should be kept secure and have appropriate permissions
- Test mode is recommended before production runs

## Author

Luis B. Mata (mataluis2k@gmail.com)

## License

MIT License
