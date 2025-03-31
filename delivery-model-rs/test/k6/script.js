import http from 'k6/http';
import { sleep, check } from 'k6';

export const options = {
  vus: 10,
  duration: '10s',
};

export function generateRandomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

export function generateRandomFloat(min, max) {
  return Math.random() * (max - min + 1) + min;
}

export default function() {
  const url = "http://0.0.0.0:8000"
  let payload = JSON.stringify({
    age: generateRandomFloat(20.0, 42.0),
    rating: generateRandomFloat(1.0, 5.0),
    dist: generateRandomFloat(10.0, 30.0),
    order_type: generateRandomInt(1, 4),
    vehicle_type: generateRandomInt(1, 4),
  });
  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  let res = http.post(url, payload, params);
  check(res, { "status is 200": (res) => res.status === 200 });
  if (Math.random() > 0.98) {
    let res_reload = http.get(url + "/reload");
    check(res_reload, { "reload status is 200": (res_reload) => res_reload.status === 200 });
  }
  sleep(0.2);
}
