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
func ConnectToMongoDB() *mongo.Client {

	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		uri = "mongodb://admin:secret123@localhost:27017/"
	}

	clientOptions := options.Client().ApplyURI(uri)

	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal("❌ Ошибка подключения к MongoDB:", err)
	}

	// Проверяем подключение
	err = client.Ping(context.Background(), nil)
	if err != nil {
		log.Fatal("❌ MongoDB не доступна:", err)
	}

	fmt.Println("✅ Успешное подключение к MongoDB")
	return client
}

// GetCollection возвращает коллекцию по имени
func GetCollection(client *mongo.Client, databaseName, collectionName string) *mongo.Collection {
	return client.Database(databaseName).Collection(collectionName)
}
