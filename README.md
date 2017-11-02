Attemp to create a small event sourcing framework on top of kafka

# Running the tests

  1. Update `KAFKA_ADVERTISED_HOST_NAME` with you local ip address in `docker-compose.yml`
  2. Start the test kafka env(requires docker): `./sbt dev-env`
  3. Run the tests: `sbt test`
