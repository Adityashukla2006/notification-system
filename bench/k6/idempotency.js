// Idempotency: DUPS virtual users each send the SAME request with the SAME
// Idempotency-Key once, released together. Then one request reuses the key
// with a different body, which must be rejected with 409.
import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { send, payload, RUN } from './lib.js';

const DUPS = Number(__ENV.DUPS || 1000);
const KEY = `${RUN}-idem`;

export const options = {
  scenarios: {
    duplicates: {
      executor: 'per-vu-iterations',
      vus: DUPS,
      iterations: 1,
      maxDuration: '2m',
      exec: 'duplicate',
    },
    conflict: {
      executor: 'per-vu-iterations',
      vus: 1,
      iterations: 1,
      startTime: '20s',
      exec: 'conflict',
    },
  },
};

// k6 starts VUs one after another; waiting for a common wall-clock instant
// releases all DUPS requests together, so they genuinely race.
const RELEASE_AT = Number(__ENV.RELEASE_AT || 0);

export function duplicate() {
  if (RELEASE_AT > 0) {
    const wait = RELEASE_AT - Date.now();
    if (wait > 0) sleep(wait / 1000);
  }
  send(0, KEY, payload(1), { kind: 'duplicate' });
}

export function conflict() {
  const body = payload(1);
  body.payload.subject = 'CHANGED';
  const res = send(0, KEY, body, { kind: 'conflict' });
  // Recorded as a check so the JSON summary proves the exact status code.
  check(res, { 'changed body on reused key is 409': (r) => r.status === 409 });
  console.log(`CONFLICT_STATUS ${res.status}`);
}
