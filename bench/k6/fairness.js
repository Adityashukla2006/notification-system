// Tenant fairness: a quiet tenant at QUIET_RATE/s and, unless NOISY_RATE is 0,
// a noisy tenant at NOISY_RATE/s, for the same DURATION.
import exec from 'k6/execution';
import { send, payload, uniqueKey } from './lib.js';

const QUIET_RATE = Number(__ENV.QUIET_RATE || 20);
const NOISY_RATE = Number(__ENV.NOISY_RATE || 0);
const DURATION = __ENV.DURATION || '60s';

const scenarios = {
  quiet: {
    executor: 'constant-arrival-rate', rate: QUIET_RATE, timeUnit: '1s', duration: DURATION,
    preAllocatedVUs: 20, maxVUs: 200, exec: 'quiet',
  },
};
if (NOISY_RATE > 0) {
  scenarios.noisy = {
    executor: 'constant-arrival-rate', rate: NOISY_RATE, timeUnit: '1s', duration: DURATION,
    preAllocatedVUs: 100, maxVUs: 1000, exec: 'noisy',
  };
}
// Always-passing thresholds on tagged metrics make k6 export per-tenant
// submetrics (requests sent, non-2xx share, latency) in its JSON summary.
const thresholds = {};
for (const t of ['0', '1']) {
  thresholds[`http_reqs{tenant:${t}}`] = ['count>=0'];
  thresholds[`http_req_failed{tenant:${t}}`] = ['rate>=0'];
  thresholds[`http_req_duration{tenant:${t}}`] = ['max>=0'];
}
export const options = { scenarios, thresholds };

export function quiet() {
  send(1, uniqueKey(), payload(exec.scenario.iterationInTest));
}

export function noisy() {
  send(0, uniqueKey(), payload(exec.scenario.iterationInTest));
}
