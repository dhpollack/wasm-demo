# Notes

Created `dataset.jsonl` with the following:

```bash
gojq -c '.[] | .key.payload as $key | .value.payload | split("\n")[1] | [split(",")[] | tonumber] | {"age": .[0], "rating": .[1], "dist": .[2], "delivery_time":  $key}' ~/Downloads/messages.json > dataset.jsonl
```
