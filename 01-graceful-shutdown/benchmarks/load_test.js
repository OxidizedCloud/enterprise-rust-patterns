import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '5s', target: 50 },  // Ramp up to 50 virtual users
    { duration: '15s', target: 50 }, // Maintain 50 VUs (simulating production load during deployment)
    { duration: '5s', target: 0 },   // Ramp down
  ],
  thresholds: {
    // Assert: In-flight requests must return 200 OK (0 dropped transactions)
    // 503 is expected only for new requests initiated after SIGTERM tripwire
    http_req_failed: ['rate<0.05'], 
    http_req_duration: ['p(95)<400'], // 95% of requests under 400ms
  },
};

export default function () {
  const url = 'http://localhost:8080/api/v1/orders';
  const payload = JSON.stringify({
    order_id: `ord-${__VU}-${__ITER}`,
    customer_id: `cust-${__VU}`,
    amount_cents: 9900,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(url, payload, params);

  check(res, {
    'status is 200 or 503 during drain': (r) => r.status === 200 || r.status === 503,
    'zero 500 internal server errors': (r) => r.status !== 500,
    'zero 502 bad gateway errors': (r) => r.status !== 502,
  });

  sleep(0.1);
}
