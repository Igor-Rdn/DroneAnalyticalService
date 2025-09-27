package handlers

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"project/packages/auth"
	"project/packages/mongodb"
	"project/packages/parsing"

	"github.com/gin-gonic/gin"
	"github.com/xuri/excelize/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type useTables struct {
	flightDataCollection *mongo.Collection
	regionListCollection *mongo.Collection
}

func RegisterRoutes(r *gin.Engine, client *mongo.Client) {

	var tables = useTables{
		mongodb.GetCollection(client, "admin", "flightData"),
		mongodb.GetCollection(client, "admin", "regionList"),
	}

	r.GET("/ping", healthCheck)
	r.GET("/regions", func(c *gin.Context) { getRegionList(c, tables) })
	r.GET("/heatmap", func(c *gin.Context) { getHeatmapData(c, tables) })

	r.POST("/upload", auth.RequireRealmRole("admin"), func(c *gin.Context) {
		uploadFiles(c, tables)
		updateRegionList(tables)
	})

}

// Запрос для тепловой карты полетов
func getHeatmapData(c *gin.Context, collection useTables) {
	flightDataCollection := collection.flightDataCollection

	// Декодируем параметр region из URL
	regionEncoded := c.Query("region")
	region, err := url.QueryUnescape(regionEncoded)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Ошибка декодирования параметра region"})
		return
	}

	if region == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Параметр region обязателен"})
		return
	}

	ctx := context.Background()

	cursor, err := flightDataCollection.Find(ctx, bson.M{"region": region})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка выполнения запроса к базе данных"})
		return
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка декодирования данных"})
		return
	}

	var coordinates []map[string]float64
	var noFoundCounter int
	for _, result := range results {
		var lat, lng float64
		found := false

		// Пытаемся получить координаты из dep->coordinates (как объект с lat/lon)
		if dep, ok := result["dep"].(bson.M); ok {
			if coords, ok := dep["coordinates"].(bson.M); ok {
				if latVal, ok := coords["lat"].(float64); ok {
					lat = latVal
				}
				if lngVal, ok := coords["lon"].(float64); ok { // Обратите внимание: "lon", а не "lng"
					lng = lngVal
				}
				if lat != 0 || lng != 0 {
					found = true
				}
			}
		}

		// Если в dep->coordinates нет данных, пробуем shr->coordinatesDep
		if !found {
			if shr, ok := result["shr"].(bson.M); ok {
				if coordsDep, ok := shr["coordinatesDep"].(bson.M); ok {
					if latVal, ok := coordsDep["lat"].(float64); ok {
						lat = latVal
					}
					if lngVal, ok := coordsDep["lon"].(float64); ok {
						lng = lngVal
					}
					if lat != 0 || lng != 0 {
						found = true
					}
				}
			}
		}

		// Добавляем координаты, если они найдены
		if found {
			coordinates = append(coordinates, map[string]float64{
				"lat": lat,
				"lng": lng, // Преобразуем lon в lng для ответа
			})
		} else {
			noFoundCounter++
		}
	}

	if noFoundCounter > 0 {
		fmt.Printf("  ❌ Координаты не найдены в документе для %d строк \n", noFoundCounter)
	}

	c.JSON(http.StatusOK, coordinates)
}

// Получаем все документы из коллекции regionList
func getRegionList(c *gin.Context, collection useTables) {

	regionListCollection := collection.regionListCollection

	ctx := context.Background()

	// Создаем структуру для ответа
	type RegionResponse struct {
		Region string `bson:"region" json:"region"`
	}

	cursor, err := regionListCollection.Find(ctx, bson.M{})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка получения данных из базы"})
		return
	}
	defer cursor.Close(ctx)

	var regions []RegionResponse
	// Если нужно вернуть только поле region без regionID
	if len(regions) == 0 {
		// Альтернативный способ - проекция в запросе
		cursor, err := regionListCollection.Find(ctx, bson.M{}, options.Find().SetProjection(bson.M{"region": 1, "_id": 0}))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка получения данных из базы"})
			return
		}
		defer cursor.Close(ctx)

		var simpleRegions []bson.M
		if err := cursor.All(ctx, &simpleRegions); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка декодирования данных"})
			return
		}

		c.JSON(http.StatusOK, simpleRegions)
		return
	}

	c.JSON(http.StatusOK, regions)
}

// Проверка доступности сервера
func healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"message": "pong"})
}

// Парсинг и загрузка файла в базу
func uploadFiles(c *gin.Context, collection useTables) {

	flightDataCollection := collection.flightDataCollection

	fmt.Println("=== НАЧАЛО ОБРАБОТКИ ДАННЫХ ДЛЯ MONGODB ===")

	file, err := c.FormFile("excel_file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Файл не получен"})
		return
	}

	uploadedFile, err := file.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка открытия файла"})
		return
	}
	defer uploadedFile.Close()

	xlsxFile, err := excelize.OpenReader(uploadedFile)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка чтения Excel"})
		return
	}
	defer xlsxFile.Close()

	// Получаем первый лист с данными
	sheets := xlsxFile.GetSheetList()
	if len(sheets) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Нет листов в файле"})
		return
	}

	firstSheet := sheets[0]
	fmt.Printf("📊 Обрабатываем лист: %s\n", firstSheet)

	rows, err := xlsxFile.GetRows(firstSheet)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка чтения строк"})
		return
	}

	// Пропускаем первую строку (заголовок) и обрабатываем данные
	var insertedCount int
	for i, row := range rows[1:] {

		flightData := parsing.CreateFlightData(i+1, row)

		// Сохраняем в MongoDB
		result, err := flightDataCollection.InsertOne(context.Background(), flightData)
		if err != nil {
			log.Printf("❌ Ошибка сохранения строки %d: %v", i+1, err)
			continue
		}

		fmt.Printf("✅ Строка %d сохранена с ID: %v\n", i+1, result.InsertedID)
		insertedCount++

	}

	fmt.Printf("\n📈 ИТОГ: Считано %d строк, успешно сохранено %d строк\n", len(rows)-1, insertedCount)
	fmt.Println("=== ДАННЫЕ УСПЕШНО СОХРАНЕНЫ В MONGODB ===")

	//http ответ
	c.JSON(http.StatusOK, gin.H{
		"message":        "Данные успешно сохранены в MongoDB",
		"total_rows":     len(rows) - 1,
		"inserted_count": insertedCount,
		"collection":     "flightData",
	})

}

// Обновляем список уникальных регионов
func updateRegionList(collection useTables) {

	regionListCollection := collection.regionListCollection
	flightDataCollection := collection.flightDataCollection

	ctx := context.Background()

	fmt.Println("🔄 Обновление списка регионов...")

	// Очищаем коллекцию regionList
	_, err := regionListCollection.DeleteMany(ctx, bson.M{})
	if err != nil {
		fmt.Printf("ошибка очистки коллекции regionList: %v", err)
	}
	fmt.Println("✅ Коллекция regionList очищена")

	// Получаем уникальные регионы из flightData
	pipeline := mongo.Pipeline{
		{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$region"}}}},
		{{Key: "$project", Value: bson.D{{Key: "region", Value: "$_id"}, {Key: "_id", Value: 0}}}},
	}

	cursor, err := flightDataCollection.Aggregate(ctx, pipeline)
	if err != nil {
		fmt.Printf("ошибка агрегации регионов: %v", err)
	}
	defer cursor.Close(ctx)

	var regions []bson.M
	if err := cursor.All(ctx, &regions); err != nil {
		fmt.Printf("ошибка декодирования регионов: %v", err)
	}

	// Вставляем уникальные регионы в regionList
	if len(regions) > 0 {
		var documents []any
		for i, region := range regions {
			// Добавляем порядковый номер для каждого региона
			document := bson.M{
				"regionID": i + 1,
				"region":   region["region"],
			}
			documents = append(documents, document)
		}

		result, err := regionListCollection.InsertMany(ctx, documents)
		if err != nil {
			fmt.Printf("ошибка вставки регионов: %v", err)
		}

		fmt.Printf("✅ В коллекцию regionList добавлено %d уникальных регионов\n", len(result.InsertedIDs))
	} else {
		fmt.Println("ℹ️  Не найдено регионов для добавления")
	}

}
