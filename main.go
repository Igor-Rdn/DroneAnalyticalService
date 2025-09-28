package main

import (
	"context"
	"fmt"
	"net/http"

	"log"
	"project/packages/auth"
	"project/packages/cors"
	"project/packages/handlers"
	"project/packages/mongodb"

	"github.com/gin-gonic/gin"
)

func main() {

	// Подключение к MongoDB
	client := mongodb.ConnectToMongoDB()
	defer client.Disconnect(context.Background())

	//Инициализация роутера
	router := gin.Default()

	//Проверка допустимых доменов
	router.Use(cors.CORS())

	router.OPTIONS("/*path", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	//Авторизация
	router.Use(auth.JWTAuth())

	handlers.RegisterRoutes(router, client)

	port := ":8080"

	fmt.Println("Listening and serving HTTP on :8080")
	if err := router.Run(port); err != nil {
		log.Fatal("❌ Ошибка запуска сервера:", err)
	}

}
