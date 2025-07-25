Some Commands to run and work with the Docker image

- Prepare the code and build the project

```console
mvn fmt:format
mvn xml-format:xml-format
mvn clean test-compile package -Dmaven.test.skip=true -DskipTests=true
```

- Build the Image

```console
podman build --platform linux/amd64  -f src/main/docker/Dockerfile.jvm -t quarkus/sgv2-jsonapi-jvm .
```

- Tag the Image
```console
podman tag localhost/quarkus/sgv2-jsonapi-jvm clunvendatastax/stargate-builds:latest
```

Test the image
```
podman run -i --rm -p 8181:8181 quarkus/sgv2-jsonapi-jvm
```

- Push the image
```
podman tag localhost/quarkus/sgv2-jsonapi-jvm clunvendatastax/stargate-builds:1.0.27-patchcve-7
podman push clunvendatastax/stargate-builds:1.0.27-patchcve-7
```

- Scan the Image with snyk

```
brew tap snyk/tap
brew install snyk
snyk auth
snyk container test \
clunvendatastax/stargate-builds:1.0.27-patchcve-5 \
--platform=linux/amd64 \
--json-file-output=data-api-java.json
```

