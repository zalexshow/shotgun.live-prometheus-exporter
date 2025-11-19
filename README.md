# Shotgun Exporter

Petit exporter Prometheus pour récupérer les chiffres d’une billetterie Shotgun (https://shotgun.live) et les afficher dans Grafana. Idéal pour suivre les ventes, les scans à l’entrée et les remboursements sans se prendre la tête.

Compte tenu des contraintes de l’API Shotgun, l’exporteur conserve un état persistant. Autant en profiter pour garder quelques informations non sensibles sur les billets, ça pourrait être utile pour la suite. Aucune donnée personnelle n’est stockée : uniquement des données ne permettant pas d’identifier l’acheteur.

## Pourquoi

- Suivre ça dans l’interface web Shotgun devient vite compliqué sur de gros événements (peu d’agrégations/historique, difficile à consolider).
- Ici, on pousse les données dans une TSDB, le requêtage est donc beaucoup plus simple et léger.
- On peut y faire converger d’autres revendeurs ou d’autres métriques au même endroit pour une vue unifiée.

## Stack de démo

- Python exporter (HTTP `/metrics`)
- vmagent → VictoriaMetrics (stockage TSDB)
- Grafana (dashboards)
- SQLite (cache local, historique tickets)

## Démarrage rapide

**Prérequis:** Docker + Docker Compose, une clé API Shotgun _(en réalité c'est un JWT)_ et votre Organizer ID (Settings > Integrations > Shotgun APIs).

`.env` minimal:

```env
SHOTGUN_API_KEY=xxx
SHOTGUN_ORGANIZER_ID=12345
EXPORTER_PORT=9090
SCRAPE_INTERVAL=300
INCLUDE_COHOSTED_EVENTS=false
```

```bash
mkdir -p data/{victoria-metrics,grafana}
docker compose up -d
```

- Exporter: http://localhost:9090/metrics
- Grafana: http://localhost:3000 (admin/admin)
- VictoriaMetrics: http://localhost:8428

## Métriques

- `shotgun_tickets_sold_total`: tickets vendus (labels: event, ticket_title, channel)
- `shotgun_tickets_revenue_euros_total`: revenus en euros
- `shotgun_tickets_refunded_total`: tickets remboursés/annulés
- `shotgun_tickets_scanned_total`: tickets scannés
- `shotgun_events_total`: événements par statut (active/past/cancelled)

Le reste est visible sur `/metrics` une fois lancé.

## Données & persistance

- `./data/shotgun_tickets.db` (SQLite): historique et statut des tickets
- `./data/victoria-metrics/`: données time‑series
- `./data/grafana/`: dashboards/config Grafana

## Dev rapide

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python shotgun_exporter.py
```

## Crédits

C'est un mini projet porté par SYNTAC (https://syntac.fr)
Pensé pour FZL (https://www.fzlprod.com/) & Foreztival (https://foreztival.com/)
Fonctionne avec Shotgun (https://shotgun.live/)


## Notes

Exemple de structure des données conservées dans l'état/cache local (sqlite) pour chaque billet vendu _(aucune donnée personnelle)_ :

```json
{
  "order_id": 10000001,
  "currency": "eur",
  "payment_method": "card",
  "utm_source": "example.com",
  "utm_medium": "website",
  "product_id": 200100,
  "ordered_at": "2025-01-15T18:54:55.242Z",
  "event_id": 300200,
  "event_name": "Concert Exemple au Grand Théâtre",
  "event_start_time": "2025-02-10T19:00:00.000Z",
  "event_end_time": "2025-02-10T22:00:00.000Z",
  "event_cancellation_date": null,
  "event_on_sale_date": "2024-12-09T14:49:14.120Z",
  "event_creation_date": "2024-12-01T12:00:00.000Z",
  "event_publication_date": "2024-12-13T09:00:00.837Z",
  "event_launch_date": "2024-12-13T09:00:02.020Z",
  "buyer_zip_code": "00000",
  "buyer_city": "Exempleville",
  "buyer_country": "France",
  "ticket_id": 400300,
  "ticket_barcode": "12345678901234",
  "ticket_redeemed_at": null,
  "shotguner_id": 500400,
  "cancelled_at": null,
  "ticket_updated_at": "2025-01-15T18:54:55.242Z",
  "ticket_visibilities": "{public,xpress_door,promoters}",
  "ticket_price": 30,
  "ticket_title": "Catégorie A",
  "channel": "online",
  "service_fee": 5,
  "user_service_fee": 0.99,
  "producer_cost": 0,
  "vat_rate": 0.055,
  "ticket_sub_category": null,
  "ticket_status": "valid",
  "sales_status": "OnSale"
}
```
