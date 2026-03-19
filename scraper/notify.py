"""
Buddhist Events Calendar — Push Notification Sender
Runs daily via GitHub Actions.
Sends push notifications (and optionally emails) for events
occurring in exactly 7 days or 1 day from today.
"""

import os
import json
import time
import base64
import hashlib
import hmac
import struct
from datetime import date, timedelta
from urllib.request import urlopen, Request
from urllib.error import HTTPError

# ── Config ────────────────────────────────────────────────────────────────────
SUPA_URL    = os.environ.get("SUPABASE_URL", "")
SUPA_KEY    = os.environ.get("SUPABASE_KEY", "")
VAPID_PUB   = os.environ.get("VAPID_PUBLIC_KEY", "")
VAPID_PRIV  = os.environ.get("VAPID_PRIVATE_KEY", "")
VAPID_EMAIL = os.environ.get("VAPID_CONTACT_EMAIL", "admin@example.com")
SITE_URL    = os.environ.get("SITE_URL", "https://delicate-taiyaki-14c563.netlify.app")
TODAY       = date.today()
SENT        = 0
ERRORS      = []

# ── Supabase helpers ───────────────────────────────────────────────────────────
def supa_get(path, params=""):
    url = f"{SUPA_URL}/rest/v1/{path}{params}"
    req = Request(url, headers={
        "apikey": SUPA_KEY,
        "Authorization": f"Bearer {SUPA_KEY}",
        "Accept": "application/json",
    })
    try:
        with urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except Exception as e:
        ERRORS.append(f"supa_get({path}): {e}")
        return []

def supa_delete(path, params=""):
    url = f"{SUPA_URL}/rest/v1/{path}{params}"
    req = Request(url, method="DELETE", headers={
        "apikey": SUPA_KEY,
        "Authorization": f"Bearer {SUPA_KEY}",
    })
    try:
        with urlopen(req, timeout=15) as r:
            return True
    except Exception as e:
        ERRORS.append(f"supa_delete: {e}")
        return False

# ── Fetch upcoming events with subscriptions ──────────────────────────────────
def get_events_needing_notification():
    """Return events happening in exactly 7 or 1 days with their subscriptions."""
    day7 = (TODAY + timedelta(days=7)).isoformat()
    day1 = (TODAY + timedelta(days=1)).isoformat()
    target_dates = [day7, day1]
    results = []

    for target in target_dates:
        days_away = 7 if target == day7 else 1
        # Get events on this date
        events = supa_get("events", f"?select=id,title,date,location,organization,source_url&date=eq.{target}&approved=eq.true")
        for ev in events:
            # Get subscriptions for this event
            subs = supa_get("push_subscriptions", f"?select=*&event_id=eq.{ev['id']}")
            if subs:
                results.append({"event": ev, "subscriptions": subs, "days_away": days_away})
    return results

# ── Web Push ──────────────────────────────────────────────────────────────────
def send_web_push(subscription, payload_dict):
    """Send a Web Push notification using the pywebpush-compatible approach."""
    try:
        from pywebpush import webpush, WebPushException
        webpush(
            subscription_info={
                "endpoint": subscription["endpoint"],
                "keys": {
                    "p256dh": subscription["p256dh"],
                    "auth":   subscription["auth"],
                }
            },
            data=json.dumps(payload_dict),
            vapid_private_key=VAPID_PRIV,
            vapid_claims={
                "sub": f"mailto:{VAPID_EMAIL}",
            }
        )
        return True
    except WebPushException as e:
        if e.response and e.response.status_code == 410:
            # Subscription expired — delete it
            supa_delete("push_subscriptions", f"?endpoint=eq.{subscription['endpoint']}")
            return False
        ERRORS.append(f"push_send({subscription.get('endpoint','?')[:40]}): {e}")
        return False
    except Exception as e:
        ERRORS.append(f"push_send: {e}")
        return False

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    global SENT
    print(f"Buddhist Events Notification Sender — {TODAY}")
    print("=" * 50)

    if not SUPA_URL or not SUPA_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set.")
        return

    items = get_events_needing_notification()
    if not items:
        print("No events need notifications today.")
        return

    for item in items:
        ev        = item["event"]
        subs      = item["subscriptions"]
        days_away = item["days_away"]
        label     = "tomorrow" if days_away == 1 else "in 1 week"

        print(f"\nEvent: {ev['title']} ({ev['date']}) — {len(subs)} subscriber(s)")

        payload = {
            "title": f"{'🕐 Tomorrow' if days_away==1 else '📅 In 1 week'}: {ev['title']}",
            "body":  f"{ev['location']} · {ev['organization'] or ''}\nTap to view details.",
            "url":   SITE_URL,
            "tag":   f"bec-{ev['id']}-{days_away}",
            "icon":  f"{SITE_URL}/icon-192.png",
        }

        for sub in subs:
            # Skip email-only subscriptions for push
            if sub.get("endpoint") == "email-only":
                continue
            if days_away == 7 and not sub.get("notify_7day", True):
                continue
            if days_away == 1 and not sub.get("notify_1day", True):
                continue

            ok = send_web_push(sub, payload)
            if ok:
                SENT += 1
                print(f"  ✓ Push sent to {sub['endpoint'][:40]}...")
            else:
                print(f"  – Push failed for {sub['endpoint'][:40]}...")

        time.sleep(0.5)

    print(f"\n{'='*50}")
    print(f"Done. Sent {SENT} notifications.")
    if ERRORS:
        print(f"\nErrors ({len(ERRORS)}):")
        for e in ERRORS:
            print(f"  • {e}")

if __name__ == "__main__":
    main()
