## Generate Bindings

```shell
bindgen /usr/local/include/avro.h -o src/bindings.rs --whitelist-function '^avro_.*' --whitelist-var '^avro_.*'
```