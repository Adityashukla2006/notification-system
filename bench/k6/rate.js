// Open-loop fixed rate: RATE requests/s for DURATION, spread round-robin over
// TENANTS tenants. Open loop (constant-arrival-rate) matters: a closed loop
// slows down when the server does, hiding exactly the saturation we measure.
// dropped_iterations > 0 means k6 ran out of VUs because the API fell behind.
//
// WARMUP (e.g. "10s") runs the same rate first as a separate scenario, so the
// measured scenario ("load") starts on warm connections and a warm pool. A
// per-second trace showed the first ~2 s of a cold run dominate its p99.
// Metrics for the measured window alone are exported as {scenario:load}.
import exec from 'k6/execution';
import { send, payload, uniqueKey } from './lib.js';

const RATE = Number(__ENV.RATE || 100);
const TENANTS = Number(__ENV.TENANTS || 4);
const WARMUP = __ENV.WARMUP || '0s';

function scenario(duration, startTime) {
  return {
    executor: 'constant-arrival-rate',
    rate: RATE,
    timeUnit: '1s',
    duration,
    startTime,
    preAllocatedVUs: Number(__ENV.VUS || Math.max(50, RATE / 5)),
    maxVUs: Number(__ENV.MAX_VUS || Math.max(200, RATE)),
  };
}

const scenarios = { load: scenario(__ENV.DURATION || '30s', WARMUP) };
if (WARMUP !== '0s') {
  scenarios.warmup = scenario(WARMUP, '0s');
}

export const options = {
  scenarios,
  // Always-passing thresholds make k6 export the measured window's submetrics.
  thresholds: {
    'http_reqs{scenario:load}': ['count>=0'],
    'http_req_failed{scenario:load}': ['rate>=0'],
    'http_req_duration{scenario:load}': ['max>=0'],
    'dropped_iterations{scenario:load}': ['count>=0'],
  },
};

export default function () {
  // Keys stay unique across both scenarios because uniqueKey includes the
  // scenario name; the payload index only needs to vary.
  const i = exec.scenario.iterationInTest;
  send(i % TENANTS, uniqueKey(), payload(i));
}
