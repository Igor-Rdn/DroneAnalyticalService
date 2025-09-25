# Этап сборки
FROM golang:1.24.4 AS builder

WORKDIR /app

# Копируем go.mod и go.sum для кеширования зависимостей
COPY go.mod go.sum ./
RUN go mod download

# Копируем исходники
COPY . .

# ✅ Собираем бинарник под Linux
RUN GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o main .

# Этап минимального образа
FROM alpine:3.19

WORKDIR /app

# Копируем бинарник из builder
COPY --from=builder /app/main .

# Указываем порт
EXPOSE 8080

CMD ["./main"]
