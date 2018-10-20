# Functional tests
This directory contains tests which are supposed to run against a simulated clusters using docker containers managed by docker compose. The tests are supposed to simulate scenarious that are 
otherwise difficult to test with normal unit tests. 

### Rebuild
Run this after making changes to make a clean restart 

```
docker-compose stop && docker-compose rm -f && docker-compose build && docker-compose up -d
```

This could be done faster by building locally and copy the compiled code only. 