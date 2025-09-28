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
	"time"

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
	r.GET("/flight-count", func(c *gin.Context) { getFlightCount(c, tables) })
	r.GET("/avg-flight-duration", func(c *gin.Context) { getAvgFlightDuration(c, tables) })
	r.GET("/top-10", func(c *gin.Context) { getTop10Regions(c, tables) })

	r.POST("/upload", auth.RequireRealmRole("admin"), func(c *gin.Context) {
		uploadFiles(c, tables)
		updateRegionList(tables)
	})

}

func getTop10Regions(c *gin.Context, collection useTables) {
	flightDataCollection := collection.flightDataCollection

	// Получаем параметры из query string
	from := c.Query("from")
	to := c.Query("to")

	if from == "" || to == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Параметры from и to обязательны"})
		return
	}

	ctx := context.Background()

	// Парсим даты
	start, err := time.Parse(time.RFC3339, from)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Неверный формат from"})
		return
	}

	end, err := time.Parse(time.RFC3339, to)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Неверный формат to"})
		return
	}

	fmt.Printf("🏆 Получение топ-10 регионов с %s по %s\n", from, to)

	// Создаем pipeline для агрегации
	pipeline := mongo.Pipeline{
		// Фильтруем по searchFields.dateTime
		{{Key: "$match", Value: bson.M{
			"searchFields.dateTime": bson.M{
				"$gte": start,
				"$lte": end,
			},
		}}},
		// Группируем по регионам, считаем полеты и сумму дронов
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$region"},
			{Key: "flightCount", Value: bson.D{{Key: "$sum", Value: 1}}},
			{Key: "droneCount", Value: bson.D{
				{Key: "$sum", Value: bson.D{
					{Key: "$cond", Value: bson.A{
						bson.M{"$and": bson.A{
							bson.M{"$ne": bson.A{"$shr.aircraftQuantity", nil}},
							bson.M{"$gt": bson.A{"$shr.aircraftQuantity", 0}},
						}},
						"$shr.aircraftQuantity",
						1, // если aircraftQuantity пустое или 0, используем 1
					}},
				}},
			}}},
		}},
		// Сортируем по flightCount по убыванию
		{{Key: "$sort", Value: bson.M{"flightCount": -1}}},
		// Ограничиваем 10 результатами
		{{Key: "$limit", Value: 10}},
		// Проектируем в нужный формат
		{{Key: "$project", Value: bson.D{
			{Key: "_id", Value: 0},
			{Key: "region", Value: "$_id"},
			{Key: "flightCount", Value: 1},
			{Key: "droneCount", Value: 1},
		}}},
	}

	cursor, err := flightDataCollection.Aggregate(ctx, pipeline)
	if err != nil {
		fmt.Printf("❌ Ошибка агрегации: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка выполнения запроса к базе данных"})
		return
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		fmt.Printf("❌ Ошибка декодирования: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка декодирования данных"})
		return
	}

	fmt.Printf("📈 Найдено регионов в топ-10: %d\n", len(results))

	/* 	// Выводим результаты для отладки
	   	for i, result := range results {
	   		fmt.Printf("🏅 %d. Регион: %s, полетов: %v, дронов: %v\n",
	   			i+1, result["region"], result["flightCount"], result["droneCount"])
	   	} */

	c.JSON(http.StatusOK, results)
}

func getAvgFlightDuration(c *gin.Context, collection useTables) {
	flightDataCollection := collection.flightDataCollection

	// Получаем параметры из query string
	from := c.Query("from")
	to := c.Query("to")

	if from == "" || to == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Параметры from и to обязательны"})
		return
	}

	ctx := context.Background()

	// Парсим даты
	start, err := time.Parse(time.RFC3339, from)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Неверный формат from"})
		return
	}

	end, err := time.Parse(time.RFC3339, to)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Неверный формат to"})
		return
	}

	fmt.Printf("📊 Получение средней длительности полетов с %s по %s\n", from, to)

	// Создаем pipeline для агрегации
	pipeline := mongo.Pipeline{
		// Фильтруем по searchFields.dateTime
		{{Key: "$match", Value: bson.M{
			"searchFields.dateTime": bson.M{
				"$gte": start,
				"$lte": end,
			},
		}}},
		// Фильтруем документы, где есть длительность полета
		{{Key: "$match", Value: bson.M{
			"shr.flightDuration": bson.M{"$gt": 0},
		}}},
		// Добавляем поле с взвешенной длительностью (длительность * количество дронов)
		{{Key: "$addFields", Value: bson.M{
			"weightedDuration": bson.M{
				"$multiply": bson.A{
					"$shr.flightDuration",
					bson.M{
						"$cond": bson.A{
							bson.M{"$and": bson.A{
								bson.M{"$ne": bson.A{"$shr.aircraftQuantity", nil}},
								bson.M{"$gt": bson.A{"$shr.aircraftQuantity", 0}},
							}},
							"$shr.aircraftQuantity",
							1, // если aircraftQuantity пустое или 0, используем 1
						},
					},
				},
			},
		}}},
		// Группируем по регионам и вычисляем среднее
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$region"},
			{Key: "avgDurationMinutes", Value: bson.M{"$avg": "$weightedDuration"}},
		}}},
		// Проектируем в нужный формат
		{{Key: "$project", Value: bson.D{
			{Key: "_id", Value: 0},
			{Key: "region", Value: "$_id"},
			{Key: "avgDurationMinutes", Value: bson.M{"$round": bson.A{"$avgDurationMinutes", 1}}}, // округляем до 1 знака
		}}},
		// Сортируем по региону
		{{Key: "$sort", Value: bson.M{"region": 1}}},
	}

	cursor, err := flightDataCollection.Aggregate(ctx, pipeline)
	if err != nil {
		fmt.Printf("❌ Ошибка агрегации: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка выполнения запроса к базе данных"})
		return
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		fmt.Printf("❌ Ошибка декодирования: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка декодирования данных"})
		return
	}

	fmt.Printf("📈 Найдено регионов: %d\n", len(results))

	// Выводим результаты для отладки
	/* 	for _, result := range results {
		fmt.Printf("📊 Регион: %s, средняя длительность: %.1f минут\n",
			result["region"], result["avgDurationMinutes"])
	} */

	c.JSON(http.StatusOK, results)
}

// Получение статистики по количеству полетов в группах по регионам
func getFlightCount(c *gin.Context, collection useTables) {
	flightDataCollection := collection.flightDataCollection

	// Получаем параметры из query string
	from := c.Query("from")
	to := c.Query("to")

	if from == "" || to == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Параметры from и to обязательны"})
		return
	}

	ctx := context.Background()

	// Парсим даты
	start, err := time.Parse(time.RFC3339, from)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Неверный формат from"})
		return
	}

	end, err := time.Parse(time.RFC3339, to)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Неверный формат to"})
		return
	}

	fmt.Printf("📊 Получение статистики с %s по %s\n", from, to)

	filter := bson.M{
		"searchFields.dateTime": bson.M{
			"$gte": start,
			"$lte": end,
		},
	}

	// Создаем pipeline для агрегации
	pipeline := mongo.Pipeline{
		// Фильтруем по дате и региону
		{{Key: "$match", Value: filter}},
		// Группируем по регионам
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$region"},
			{Key: "flightCount", Value: bson.D{{Key: "$sum", Value: 1}}},
			{Key: "droneCount", Value: bson.D{
				{Key: "$sum", Value: bson.D{
					{Key: "$cond", Value: bson.A{
						bson.D{{Key: "$eq", Value: bson.A{"$shr.aircraftQuantity", nil}}},
						0,
						bson.D{{Key: "$toInt", Value: "$shr.aircraftQuantity"}},
					}},
				}},
			}}},
		}},
		// Проектируем в нужный формат
		{{Key: "$project", Value: bson.D{
			{Key: "_id", Value: 0},
			{Key: "region", Value: "$_id"},
			{Key: "flightCount", Value: 1},
			{Key: "droneCount", Value: 1},
		}}},
	}

	cursor, err := flightDataCollection.Aggregate(ctx, pipeline)
	if err != nil {
		fmt.Printf("❌ Ошибка агрегации: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка выполнения запроса к базе данных"})
		return
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		fmt.Printf("❌ Ошибка декодирования: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка декодирования данных"})
		return
	}

	fmt.Printf("📈 Найдено регионов: %d\n", len(results))
	c.JSON(http.StatusOK, results)
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
		var lat, lon float64
		found := false

		// Пытаемся получить координаты из dep->coordinates (как объект с lat/lon)
		if dep, ok := result["dep"].(bson.M); ok {
			if coords, ok := dep["coordinates"].(bson.M); ok {
				if latVal, ok := coords["lat"].(float64); ok {
					lat = latVal
				}
				if lonVal, ok := coords["lon"].(float64); ok { // Обратите внимание: "lon", а не "lng"
					lon = lonVal
				}
				if lat != 0 || lon != 0 {
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
					if lonVal, ok := coordsDep["lon"].(float64); ok {
						lon = lonVal
					}
					if lat != 0 || lon != 0 {
						found = true
					}
				}
			}
		}

		// Добавляем координаты, если они найдены
		if found {
			coordinates = append(coordinates, map[string]float64{
				"lat": lat,
				"lon": lon,
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

	// Очищаем коллекцию flightData
	ctx := context.Background()
	_, err := flightDataCollection.DeleteMany(ctx, bson.M{})
	if err != nil {
		fmt.Printf("ошибка очистки коллекции flightData: %v", err)
	}
	fmt.Println("✅ Коллекция flightData очищена")
	//Конец очистки УДАЛИТЬ ПОСЛЕ ОКОНЧАНИЯ РАЗРАБОТКИ

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
	fmt.Printf("🔄 Обрабатываем лист: %s\n", firstSheet)

	rows, err := xlsxFile.GetRows(firstSheet)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка чтения строк"})
		return
	}

	totalRow := len(rows) - 1
	fmt.Printf("📊 Общее количество строк: %d\n", totalRow)

	// Пропускаем первую строку (заголовок) и обрабатываем данные
	var insertedCount int
	for i, row := range rows[1:] {

		flightData := parsing.CreateFlightData(i+1, row)

		// Сохраняем в MongoDB
		_, err := flightDataCollection.InsertOne(context.Background(), flightData)
		if err != nil {
			log.Printf("❌ Ошибка сохранения строки %d: %v", i+1, err)
			continue
		}

		if i%(totalRow/10) == 0 {
			fmt.Printf("✅ Загрузка завершена на %d%%\n", i*10/(totalRow/10))
		}

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

		fmt.Printf("📈 В коллекцию regionList добавлено %d уникальных регионов\n", len(result.InsertedIDs))
	} else {
		fmt.Println("ℹ️  Не найдено регионов для добавления")
	}

}
