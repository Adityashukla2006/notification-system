// Many keys, each sent COPIES times at once (http.batch issues them in
// parallel). One key gives the enqueue race one chance; this gives it N.
import http from 'k6/http';
import exec from 'k6/execution';
import { TARGET, RUN, tenants, payload } from './lib.js';

const COPIES = Number(__ENV.COPIES || 5);

export const options = {
  scenarios: {
    dupkeys: {
      executor: 'shared-iterations',
      iterations: Number(__ENV.N || 5000),
      vus: Number(__ENV.VUS || 100),
      maxDuration: '10m',
    },
  },
};

export default function () {
  const i = exec.scenario.iterationInTest;
  const t = tenants[i % tenants.length];
  const req = {
    method: 'POST',
    url: `${TARGET}/v1/notifications`,
    body: JSON.stringify(payload(i)),
    params: {
      headers: {
        Authorization: `Bearer ${t.token}`,
        'Content-Type': 'application/json',
        'Idempotency-Key': `${RUN}-${i}`,
      },
    },
  };
  const batch = [];
  for (let c = 0; c < COPIES; c++) batch.push(req);
  for (const res of http.batch(batch)) {
    if (res.status === 202) console.log(`A ${res.json('id')}`);
  }
}
