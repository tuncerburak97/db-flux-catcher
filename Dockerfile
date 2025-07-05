# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Gerekli build araçlarını yükle
RUN apk add --no-cache git

# Go modüllerini kopyala ve indir
COPY go.mod go.sum ./
RUN go mod download

# Kaynak kodları kopyala
COPY . .

# Uygulamayı derle
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main ./src/cmd/main.go

# Final stage
FROM alpine:3.19

WORKDIR /app

# Install timezone data and set timezone
RUN apk add --no-cache tzdata
ENV TZ=Europe/Istanbul
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Builder'dan sadece binary'yi kopyala
COPY --from=builder /app/main .

# Config dosyasını kopyala
COPY config/config.yaml /app/config/

EXPOSE 8080

CMD ["./main"] 