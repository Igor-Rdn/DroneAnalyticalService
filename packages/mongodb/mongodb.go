package mongodb

import (
	"context"
	"fmt"
	"log"
	"os"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Подключение к MongoDB
func ConnectToMongoDB() (collection *mongo.Collection) {
	var mongoClient *mongo.Client

	uri := os.Getenv("MONGO_URI")

	clientOptions := options.Client().ApplyURI(uri)

	var err error
	mongoClient, err = mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal("❌ Ошибка подключения к MongoDB:", err)
	}

	// Проверяем подключение
	err = mongoClient.Ping(context.Background(), nil)
	if err != nil {
		log.Fatal("❌ MongoDB не доступна:", err)
	}

	fmt.Println("✅ Успешное подключение к MongoDB")

	// Создаем/получаем коллекцию
	return mongoClient.Database("admin").Collection("flightData")
}
