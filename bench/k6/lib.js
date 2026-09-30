// Shared helpers for every k6 scenario.
//
// Every 202 response's notification id is logged as "A <id>" so the runner can
// load the full set of accepted ids into Postgres and prove, per id, that each
// one reached a terminal state. k6's summary alone can only count responses.
import http from 'k6/http';
import exec from 'k6/execution';

export const TARGET = __ENV.TARGET || 'http://api:8080';
export const RUN = __ENV.RUN || `run${Date.now()}`;
export const tenants = JSON.parse(open('/state/keys.json'));

const names = ['Aarav', 'Maya', 'Liam', 'Sofia', 'Kenji', 'Amara', 'Noah', 'Priya'];

// payload builds a realistic transactional notification: 60% email (~1 KB
// body), 25% SMS (one 160-char segment), 15% push. Deterministic per index.
export function payload(i) {
  const r = i % 100;
  const name = names[i % names.length];
  const order = 100000 + (i * 7919) % 900000;
  if (r < 60) {
    return {
      channel: 'email',
      recipient: `customer${i}@example.com`,
      payload: {
        subject: `Order #${order} confirmed`,
        body: `Hi ${name},\n\nThanks for your order #${order}. `.padEnd(1000, 'Your items are being prepared for shipment. '),
      },
    };
  }
  if (r < 85) {
    return {
      channel: 'sms',
      recipient: `+1555${String(i % 10000000).padStart(7, '0')}`,
      payload: { body: `Hi ${name}, order #${order} has shipped. Track: https://ex.co/t/${order} Reply STOP to opt out.` },
    };
  }
  return {
    channel: 'push',
    recipient: `device-token-${i}`,
    payload: { title: 'Your order is on its way', body: `${name}, your order ships today.`, data: { order_id: order } },
  };
}

// send POSTs one notification and logs its id when accepted.
export function send(tenantIndex, key, body, extraTags) {
  const t = tenants[tenantIndex];
  const res = http.post(`${TARGET}/v1/notifications`, JSON.stringify(body), {
    headers: {
      Authorization: `Bearer ${t.token}`,
      'Content-Type': 'application/json',
      'Idempotency-Key': key,
    },
    tags: Object.assign({ tenant: String(tenantIndex) }, extraTags || {}),
  });
  if (res.status === 202) {
    console.log(`A ${res.json('id')}`);
  }
  return res;
}

// uniqueKey is unique across the whole test: run id, scenario, iteration.
export function uniqueKey() {
  return `${RUN}-${exec.scenario.name}-${exec.scenario.iterationInTest}`;
}
