FROM clojure:temurin-21-tools-deps-alpine

WORKDIR /app

# Copy deps file first for better caching
COPY deps.edn .

# Download dependencies
RUN clojure -P

# Copy source code
COPY src ./src

# Run the application
CMD ["clojure", "-M:run"]
