# Delivery Events Producer

## Notes

Get unique vehicle and food types

```bash
gojq -sR '. | [split("\n")[] | split(",") | {"food": (.[-3] | trim | ascii_downcase), "vehicle": (.[-2] | trim | ascii_downcase)}][1:] | reduce .[] as {$food, $vehicle} (null; .food += [$food] | .vehicle += [$vehicle]) |.food = (.food | unique) | .vehicle = (.vehicle | unique)' deliverytime.txt
```
