FROM gradle:8-jdk17 AS builder

WORKDIR /app

# Copy project configuration files first to cache dependencies
COPY gradle/ gradle/
COPY build.gradle* settings.gradle* gradlew ./

# Fetch dependencies
RUN ./gradlew dependencies --no-daemon || true

# Copy full source and build the ShadowJar executable
COPY . .
RUN ./gradlew shadowJar --no-daemon

# --- Stage 2: Production Runtime ---
# Switched to match the compiled bytecode version target
FROM eclipse-temurin:17-jre-alpine

# Install ffmpeg and tzdata for stream recording and time handling
RUN apk add --no-cache ffmpeg tzdata

WORKDIR /app

# Copy the compiled shadow fat jar from the builder stage
COPY --from=builder /app/build/libs/*-all.jar ./xhrec.jar

# Setup volumes for external configuration and recording outputs
VOLUME ["/app/config", "/app/recordings"]

ENV TZ=America/Chicago

ENTRYPOINT ["java", "-jar", "xhrec.jar", "-f", "/app/config/list.conf", "-o", "/app/recordings", "-u", "/app/config/users.txt", "-post", "/app/config/postprocessor.json"]