package mongodb

import (
	"context"
	"fmt"
	"log"
	"os"

	"go.mongodb.org/mongo-driver/bson"
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

	fmt.Println("🔄 Создание индексов для searchFields...")
	if err := сreateSearchFieldsIndexes(client); err != nil {
		fmt.Printf("⚠️  Предупреждение: не удалось создать индексы: %v", err)
	} else {
		fmt.Println("✅ Индексы успешно созданы/проверены")
	}

	return client
}

// GetCollection возвращает коллекцию по имени
func GetCollection(client *mongo.Client, databaseName, collectionName string) *mongo.Collection {
	return client.Database(databaseName).Collection(collectionName)
}

func сreateSearchFieldsIndexes(client *mongo.Client) error {
	collection := GetCollection(client, "admin", "flightData")
	ctx := context.Background()

	// Определяем индексы для searchFields
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "searchFields.dateTime", Value: 1}},
			Options: options.Index().SetName("searchFields_dateTime_1"),
		},
	}

	// Создаем индексы
	_, err := collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("ошибка создания индексов: %v", err)
	}

	return nil
}
