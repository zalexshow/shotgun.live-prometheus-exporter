#!/usr/bin/env python3

import os
import sys
import sqlite3
import argparse
import requests
from datetime import datetime
from typing import List, Dict, Tuple
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DB_FILE = Path('/data/shotgun_tickets.db')
VICTORIA_METRICS_URL = os.getenv('VICTORIA_METRICS_URL', 'http://victoria-metrics:8428')


def get_all_events(conn: sqlite3.Connection) -> List[Tuple[str, str, int]]:
    """Get all events with ticket counts from database"""
    cursor = conn.cursor()
    cursor.execute('''
        SELECT event_id, event_name, COUNT(*) as ticket_count
        FROM tickets
        GROUP BY event_id, event_name
        ORDER BY event_name
    ''')
    return cursor.fetchall()


def list_events(conn: sqlite3.Connection):
    """Display all available events"""
    events = get_all_events(conn)

    if not events:
        print("No events found in database")
        return

    print("\nAvailable events:")
    print("-" * 80)
    for event_id, event_name, count in events:
        print(f"  {event_id:10} | {event_name[:50]:50} | {count:5} tickets")
    print("-" * 80)
    print(f"Total: {len(events)} events")


def get_event_tickets(conn: sqlite3.Connection, event_id: str) -> List[Dict]:
    """Get all tickets for an event with their full data"""
    cursor = conn.cursor()
    cursor.execute('''
        SELECT ticket_id, event_id, event_name, ticket_title, ticket_status,
               ticket_price, channel, ticket_redeemed_at, ticket_data, first_seen_at
        FROM tickets
        WHERE event_id = ?
        ORDER BY first_seen_at
    ''', (event_id,))

    tickets = []
    for row in cursor.fetchall():
        import json
        ticket_data = json.loads(row[8]) if row[8] else {}
        tickets.append({
            'ticket_id': row[0],
            'event_id': row[1],
            'event_name': row[2],
            'ticket_title': row[3],
            'ticket_status': row[4],
            'ticket_price': row[5],
            'channel': row[6],
            'ticket_redeemed_at': row[7],
            'ticket_data': ticket_data,
            'first_seen_at': row[9]
        })

    return tickets


def delete_event_metrics(event_id: str, event_name: str, dry_run: bool = False) -> bool:
    """Delete all metrics for an event from VictoriaMetrics"""
    metrics_to_delete = [
        'shotgun_tickets_sold_total',
        'shotgun_tickets_revenue_euros_total',
        'shotgun_tickets_by_channel_total',
        'shotgun_tickets_refunded_total',
        'shotgun_tickets_scanned_total'
    ]

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Deleting metrics for event {event_id} ({event_name})...")

    for metric in metrics_to_delete:
        match_filter = f'{metric}{{event_id="{event_id}"}}'

        if dry_run:
            print(f"  Would delete: {match_filter}")
        else:
            try:
                url = f"{VICTORIA_METRICS_URL}/api/v1/admin/tsdb/delete_series"
                params = {'match[]': match_filter}
                response = requests.post(url, params=params, timeout=30)
                response.raise_for_status()
                print(f"  ✓ Deleted: {match_filter}")
            except Exception as e:
                print(f"  ✗ Error deleting {metric}: {e}")
                return False

    return True


def format_prometheus_line(metric_name: str, labels: Dict[str, str], value: float, timestamp_ms: int) -> str:
    """Format a single Prometheus metric line with timestamp"""
    # Escape label values
    escaped_labels = {k: v.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
                     for k, v in labels.items()}

    labels_str = ','.join(f'{k}="{v}"' for k, v in escaped_labels.items())
    return f"{metric_name}{{{labels_str}}} {value} {timestamp_ms}"


def get_timestamp_ms(iso_string: str) -> int:
    """Convert ISO timestamp string to milliseconds since epoch"""
    if not iso_string:
        return None

    try:
        # Handle both formats: with and without timezone
        if iso_string.endswith('Z'):
            dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
        else:
            dt = datetime.fromisoformat(iso_string)

        # Convert to UTC timestamp in milliseconds
        return int(dt.timestamp() * 1000)
    except Exception as e:
        print(f"  Warning: Could not parse timestamp '{iso_string}': {e}")
        return None


def reimport_event_data(conn: sqlite3.Connection, event_id: str, dry_run: bool = False) -> bool:
    """Re-import all metrics for an event with original timestamps"""
    tickets = get_event_tickets(conn, event_id)

    if not tickets:
        print(f"No tickets found for event {event_id}")
        return False

    event_name = tickets[0]['event_name']
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Re-importing {len(tickets)} tickets for event {event_id} ({event_name})...")

    # Build Prometheus exposition format data with timestamps
    lines = []

    for ticket in tickets:
        ticket_data = ticket['ticket_data']
        ordered_at = ticket_data.get('ordered_at')

        if not ordered_at:
            print(f"  Warning: Ticket {ticket['ticket_id']} has no ordered_at timestamp, skipping")
            continue

        timestamp_ms = get_timestamp_ms(ordered_at)
        if not timestamp_ms:
            continue

        labels = {
            'event_id': ticket['event_id'],
            'event_name': ticket['event_name'],
            'ticket_title': ticket['ticket_title']
        }

        # Sold tickets (only if valid)
        if ticket['ticket_status'] == 'valid':
            lines.append(format_prometheus_line(
                'shotgun_tickets_sold_total',
                labels,
                1,
                timestamp_ms
            ))

            # Revenue
            lines.append(format_prometheus_line(
                'shotgun_tickets_revenue_euros_total',
                labels,
                ticket['ticket_price'] or 0,
                timestamp_ms
            ))

            # By channel
            channel_labels = {
                'event_id': ticket['event_id'],
                'event_name': ticket['event_name'],
                'channel': ticket['channel'] or 'unknown'
            }
            lines.append(format_prometheus_line(
                'shotgun_tickets_by_channel_total',
                channel_labels,
                1,
                timestamp_ms
            ))

        # Refunded tickets
        elif ticket['ticket_status'] in ['refunded', 'canceled']:
            # Use cancelled_at if available, otherwise ordered_at
            cancelled_at = ticket_data.get('cancelled_at', ordered_at)
            refund_timestamp_ms = get_timestamp_ms(cancelled_at)

            if refund_timestamp_ms:
                lines.append(format_prometheus_line(
                    'shotgun_tickets_refunded_total',
                    labels,
                    1,
                    refund_timestamp_ms
                ))

        # Scanned tickets
        if ticket['ticket_redeemed_at']:
            scan_timestamp_ms = get_timestamp_ms(ticket['ticket_redeemed_at'])
            if scan_timestamp_ms:
                scan_labels = {
                    'event_id': ticket['event_id'],
                    'event_name': ticket['event_name']
                }
                lines.append(format_prometheus_line(
                    'shotgun_tickets_scanned_total',
                    scan_labels,
                    1,
                    scan_timestamp_ms
                ))

    if not lines:
        print("  No valid data to import")
        return False

    print(f"  Generated {len(lines)} metric lines")

    if dry_run:
        print("\n  Sample lines (first 10):")
        for line in lines[:10]:
            print(f"    {line}")
        if len(lines) > 10:
            print(f"    ... and {len(lines) - 10} more")
        return True

    # Import to VictoriaMetrics
    try:
        url = f"{VICTORIA_METRICS_URL}/api/v1/import/prometheus"
        data = '\n'.join(lines)

        response = requests.post(
            url,
            data=data.encode('utf-8'),
            headers={'Content-Type': 'text/plain'},
            timeout=60
        )
        response.raise_for_status()
        print(f"  ✓ Successfully imported {len(lines)} metric points")
        return True

    except Exception as e:
        print(f"  ✗ Error importing data: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"  Response: {e.response.text[:500]}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Re-import Shotgun event metrics with original timestamps',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # List all events
  python reimport_event.py --list

  # Re-import specific event (dry run)
  python reimport_event.py --event 123456 --dry-run

  # Re-import specific event
  python reimport_event.py --event 123456

  # Re-import all events
  python reimport_event.py --all
        '''
    )

    parser.add_argument('--list', action='store_true', help='List all events')
    parser.add_argument('--event', type=str, help='Event ID to re-import')
    parser.add_argument('--all', action='store_true', help='Re-import all events')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without actually doing it')
    parser.add_argument('--db', type=str, default=str(DB_FILE), help=f'Path to SQLite database (default: {DB_FILE})')

    args = parser.parse_args()

    # Check database exists
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Error: Database not found at {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)

    try:
        if args.list:
            list_events(conn)
            return

        if not args.event and not args.all:
            parser.print_help()
            print("\nError: Must specify --list, --event, or --all")
            sys.exit(1)

        events_to_process = []

        if args.all:
            all_events = get_all_events(conn)
            events_to_process = [(event_id, event_name) for event_id, event_name, _ in all_events]
            print(f"\nProcessing {len(events_to_process)} events...")
        else:
            # Verify event exists
            cursor = conn.cursor()
            cursor.execute('SELECT event_name FROM tickets WHERE event_id = ? LIMIT 1', (args.event,))
            row = cursor.fetchone()
            if not row:
                print(f"Error: Event {args.event} not found in database")
                sys.exit(1)
            events_to_process = [(args.event, row[0])]

        # Process each event
        success_count = 0
        for event_id, event_name in events_to_process:
            print(f"\n{'='*80}")
            print(f"Processing event: {event_id} - {event_name}")
            print('='*80)

            # Delete existing metrics
            if delete_event_metrics(event_id, event_name, dry_run=args.dry_run):
                # Re-import with original timestamps
                if reimport_event_data(conn, event_id, dry_run=args.dry_run):
                    success_count += 1

        print(f"\n{'='*80}")
        if args.dry_run:
            print(f"[DRY RUN] Would process {success_count}/{len(events_to_process)} events successfully")
        else:
            print(f"✓ Successfully processed {success_count}/{len(events_to_process)} events")
        print('='*80)

    finally:
        conn.close()


if __name__ == '__main__':
    main()
