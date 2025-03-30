# Delivery Events Producer

## Notes

Get unique vehicle and food types

```bash
gojq -s 'reduce .[] as {$order_type, $vehicle_type} (null; .order_type += [$order_type] | .vehicle_type += [$vehicle_type]) | .order_type = (.order_type | unique) | .vehicle_type = (.vehicle_type | unique) ' deliverytime.jsonl
```
