package main

import (
	"context"
	"fmt"
	"project/packages/handlers"
	"project/packages/mongodb"

	"github.com/gin-gonic/gin"
)

func main() {

	// Подключение к MongoDB
	client := mongodb.ConnectToMongoDB()
	defer client.Disconnect(context.Background())

	router := gin.Default()

	handlers.RegisterRoutes(router, client)

	router.Run(":8080")
	fmt.Println("🚀 Сервер запущен на http://localhost:8080")
}
