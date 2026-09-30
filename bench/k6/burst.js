// Burst: N notifications sent as fast as VUS concurrent connections allow.
import exec from 'k6/execution';
import { send, payload, uniqueKey } from './lib.js';

const TENANTS = Number(__ENV.TENANTS || 4);

export const options = {
  scenarios: {
    burst: {
      executor: 'shared-iterations',
      iterations: Number(__ENV.N || 50000),
      vus: Number(__ENV.VUS || 200),
      maxDuration: '10m',
    },
  },
};

export default function () {
  const i = exec.scenario.iterationInTest;
  send(i % TENANTS, uniqueKey(), payload(i));
}
