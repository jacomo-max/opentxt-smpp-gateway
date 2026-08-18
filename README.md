# OpenTxt SMPP Gateway (Railway)

A small, always-on Node service that lets customers bind over **SMPP 3.4** and sends
their traffic through the existing OpenTxt HTTP API. Nothing about the underlying
supplier is exposed to the customer.

```
Customer ESME ──SMPP(TCP)──▶ this gateway ──HTTPS──▶ api.opentxt.ai/api-send-sms ──▶ supplier
                     ▲                                          │
                     └────── deliver_sm (DLR / MO) ◀── OpenTxt webhook → /webhook/<token>
```

## What it does

- `bind_transceiver` / `transmitter` / `receiver` with per-customer `system_id` + `password`
- `submit_sm` → one HTTP send, paced at the carrier drain rate (default 17/sec/account)
- Returns a real `message_id`, kept mapped to the OpenTxt message id for 24h
- `enquire_link`, `unbind`, backpressure via `ESME_RTHROTTLED`
- Delivery receipts (`stat:DELIVRD` / `UNDELIV`) and inbound replies delivered back as `deliver_sm`
- `GET /health` for Railway healthchecks

## Deploy on Railway — step by step

1. **Create the repo.** Put this folder in a new GitHub repo (`opentxt-smpp-gateway`).
2. **New Railway project** → *Deploy from GitHub repo* → pick that repo. Nixpacks
   auto-detects Node 20 and runs `npm start`.
3. **Set variables** (Railway → Variables). Copy from `.env.example`:
   - `SMPP_ACCOUNTS` — JSON array; one entry per customer, each pointing at that
     customer's real OpenTxt API key (`otxt_...`).
   - `GATEWAY_WEBHOOK_TOKEN` — `openssl rand -hex 24`.
   - Leave `SMPP_PORT=2775` and `OPENTXT_API_BASE_URL=https://api.opentxt.ai`.
4. **Expose the SMPP TCP port.** Railway → Settings → Networking → **TCP Proxy** →
   target port `2775`. Railway gives you something like
   `containers-us-west-42.railway.app:31234`. That host+port is what the customer
   binds to.
5. **Expose HTTP** (for the webhook + health): Settings → Networking → *Generate Domain*.
   You get `https://opentxt-smpp-gateway-production.up.railway.app`.
6. **Point the customer's API key webhook at the gateway.** In the OpenTxt admin,
   set that API key's webhook URL to:
   `https://<your-railway-domain>/webhook/<GATEWAY_WEBHOOK_TOKEN>`
   Delivery receipts and inbound replies then flow back over SMPP automatically.
7. **Verify.** Open `https://<your-railway-domain>/health` — it should return
   `{"ok":true,"bound":[]}`. After the customer binds, their `system_id` appears in `bound`.

## Credentials to hand the customer

```
Host:        containers-us-west-42.railway.app   (from Railway TCP proxy)
Port:        31234                               (from Railway TCP proxy)
system_id:   customer1
password:    <what you put in SMPP_ACCOUNTS>
bind type:   transceiver (TRX)
system_type: (leave blank)
addr TON/NPI: 1 / 1
Encoding:    GSM-7 (data_coding 0) or UCS2 (data_coding 8)
Throughput:  17 messages/sec (ask before raising)
DLR:         set registered_delivery=1; receipts arrive as deliver_sm esm_class=4
```

## Local test

```bash
npm install
cp .env.example .env && $EDITOR .env
node --env-file=.env src/index.js
```

Then bind with any SMPP client, e.g. the included smoke test:

```bash
node scripts/test-bind.js localhost 2775 customer1 yourpassword +15551234567 "hello"
```

## Notes / limits

- **Sizing:** one Railway instance handles thousands of msg/min; throughput is
  capped by the per-account TPS pacer, not by Node.
- **Restarts drop the message_id map**, so DLRs for messages submitted right
  before a redeploy can't be matched. Redeploy during low traffic, or move the
  map to Redis if the customer needs strict DLR guarantees.
- **Billing/compliance/DNC** all stay in the OpenTxt API — the gateway never
  bypasses them; a DNC hit comes back as `ESME_RINVDSTADR`, and an empty balance
  comes back as `ESME_RTHROTTLED`.
- MMS is not part of SMPP; MMS customers stay on the HTTP API.
