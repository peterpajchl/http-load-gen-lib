### Manual verification
E.g. we could add query parameters `client_id` and `request_num` which would represent the client's memory address (should be unique enough) and the request counter for set client. Reducing the speed (sleep) on the server side should then help confirm that no more then specific number of clients are probing the service as well as confirming that one client does not affect the execution of the others (they are infact concurrent).

### List

#### HTTP Status code 
* 100 - 299 `not fatal` - mostly treating 200+
* 300 - 308 `not fatal` - might want to treat redirects and proxy with extra care
* 400 - 418 `not fatal` - without addressing the errors the results will be useless
* 418+ - 5xx `fatal` - a bit broad range but mostly useless results

### Connection
* these are connection errors at the Hyper level
* find odd to terminate completely when single JoinHandle throws

### Timeouts
* timeout can occur at resource and gateway/service level