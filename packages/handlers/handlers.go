package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"project/packages/auth"
	"project/packages/mongodb"
	"project/packages/parsing"
	"project/packages/parsing/geoGet"
	"project/packages/parsing/geoIndex"
	"project/packages/parsing/geoSearch"
	"runtime"

	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tealeg/xlsx"

	"github.com/xuri/excelize/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type useTables struct {
	flightDataCollection       *mongo.Collection
	regionListCollection       *mongo.Collection
	aircraftTypeListCollection *mongo.Collection
	subjectListCollection      *mongo.Collection
}

var (
	initOnce sync.Once
)

func RegisterRoutes(r *gin.Engine, client *mongo.Client) {

	var tables = useTables{
		mongodb.GetCollection(client, "admin", "flightData"),
		mongodb.GetCollection(client, "admin", "regionList"),
		mongodb.GetCollection(client, "admin", "aircraftTypeList"),
		mongodb.GetCollection(client, "admin", "regionsGeo"),
	}

	// Инициализация при старте сервера
	serverInit(tables)

	r.GET("/ping", healthCheck)
	r.GET("/exists-data", func(c *gin.Context) { existsData(c, tables) })
	r.GET("/regions", func(c *gin.Context) { getRegionList(c, tables) })
	r.GET("/regions/geojson", func(c *gin.Context) { getRegionsGeo(c, tables) })
	r.GET("/flights-table", func(c *gin.Context) { getFlightTable(c, tables) })
	r.GET("/flights-table/export", func(c *gin.Context) { exportFlightTable(c, tables) })
	r.GET("/heatmap", func(c *gin.Context) { getHeatmapData(c, tables) })
	r.GET("/peak-hour", func(c *gin.Context) { getPeakHour(c, tables) })
	r.GET("/region/yearly-stats", func(c *gin.Context) { getYearlyStats(c, tables) })
	r.GET("/avg-flight-duration", func(c *gin.Context) { getAvgFlightDuration(c, tables) })
	r.GET("/top-10", func(c *gin.Context) { getTop10Regions(c, tables) })
	r.GET("/flight-count", func(c *gin.Context) { getFlightCount(c, tables) })

	r.POST("/clear-table", func(c *gin.Context) { clearTable(tables) })
	r.POST("/upload", auth.RequireRealmRole("admin"), func(c *gin.Context) {
		uploadFiles(c, tables, client)
		updateAircraftTypeList(tables)
		/* 		err := geoSearch.UpdateFlightRegions(client)
		   		if err != nil {
		   			fmt.Printf("❌ Ошибка обновления поля регион основной таблицы : %v\n", err)
		   		} */
	})
	// Отдельный endpoint для обновления региона у основной таблицы
	r.POST("/subject", func(c *gin.Context) {
		if err := geoSearch.UpdateFlightRegions(client); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"message": "Субъекты РФ обновлены"})
	})
	// Отдельный endpoint для перезагрузки 2dsphere индексов и списка регионов
	r.POST("/geoindex", func(c *gin.Context) {
		if err := geoIndex.LoadRegionsToMongo(tables.subjectListCollection); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		updateRegionList(tables)
		c.JSON(http.StatusOK, gin.H{"message": "Гео-индексы загружены"})
	})

}

// getRegionsGeo handler для получения геоданных регионов
func getRegionsGeo(c *gin.Context, tables useTables) {

	regions, err := geoGet.GetRegionsGeo(tables.subjectListCollection)
	if err != nil {
		fmt.Printf("❌ Ошибка get запроса по geojson: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Не удалось получить данные по geojson"})
		return
	}
	c.JSON(http.StatusOK, regions)
}

func serverInit(collection useTables) {

	subjectListCollection := collection.subjectListCollection

	initOnce.Do(func() {
		fmt.Println("🚀 Инициализация при старте сервера...")

		needsGeoIndex, err := isCollectionEmpty(subjectListCollection)
		if err != nil {
			fmt.Printf("⚠️ Ошибка проверки коллекции regionsGeo: %v\n", err)
			return
		}
		if needsGeoIndex {
			// Загружаем гео-индексы
			if err := geoIndex.LoadRegionsToMongo(subjectListCollection); err != nil {
				fmt.Printf("⚠️ Ошибка загрузки гео-индексов: %v", err)
			}
		}
		// Обновляем список регионов
		updateRegionList(collection)
	})

}

// isCollectionEmpty проверяет, пустая ли коллекция
func isCollectionEmpty(collection *mongo.Collection) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Считаем количество документов в коллекции
	count, err := collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		return false, fmt.Errorf("ошибка подсчета документов: %v", err)
	}

	return count == 0, nil
}

func existsData(c *gin.Context, collection useTables) {
	flightDataCollection := collection.flightDataCollection
	ctx := context.Background()

	fmt.Printf("🔍 Проверка существования данных в flightData\n")

	// Пытаемся найти хотя бы один документ
	var result bson.M
	err := flightDataCollection.FindOne(ctx, bson.M{}).Decode(&result)

	rowCount := 0
	if err == nil {
		rowCount = 1
	}

	// Формируем ответ в общей стилистике
	response := bson.M{
		"rowCount": rowCount,
	}

	c.JSON(http.StatusOK, response)
}

func clearTable(collection useTables) {
	flightDataCollection := collection.flightDataCollection

	// Очищаем коллекцию flightData
	ctx := context.Background()
	_, err := flightDataCollection.DeleteMany(ctx, bson.M{})
	if err != nil {
		fmt.Printf("ошибка очистки коллекции flightData: %v", err)
	}
	fmt.Println("✅ Коллекция flightData очищена")
	//Конец очистки УДАЛИТЬ ПОСЛЕ ОКОНЧАНИЯ РАЗРАБОТКИ
}

func getFlightTable(c *gin.Context, collection useTables) {
	flightDataCollection := collection.flightDataCollection
	aircraftTypeCollection := collection.aircraftTypeListCollection

	// Получаем параметры пагинации
	page := c.DefaultQuery("page", "1")
	limit := c.DefaultQuery("limit", "20")

	// Получаем параметр временной зоны
	timezone := c.Query("timezone")

	pageInt, err := strconv.Atoi(page)
	if err != nil || pageInt < 1 {
		pageInt = 1
	}

	limitInt, err := strconv.Atoi(limit)
	if err != nil || limitInt < 1 {
		limitInt = 50
	}
	if limitInt > 1000 {
		limitInt = 1000
	}

	// Получаем параметры фильтров
	aircraftType := c.Query("aircraftType")
	dateDepFrom := c.Query("dateDepFrom")
	dateDepTo := c.Query("dateDepTo")
	flightDurationMin := c.Query("flightDurationMin")
	flightDurationMax := c.Query("flightDurationMax")
	sid := c.Query("sid")
	region := c.Query("region")
	operatorType := c.Query("operatorType")

	// Вычисляем skip
	skip := (pageInt - 1) * limitInt

	ctx := context.Background()

	fmt.Printf("📊 Получение таблицы полетов - страница %d, лимит %d\n", pageInt, limitInt)

	// Создаем базовый фильтр для запросов
	filter := bson.M{}

	// Добавляем фильтры если они переданы
	if aircraftType != "" {
		// Разделяем строку по запятым и убираем пробелы
		aircraftTypes := strings.Split(aircraftType, ",")
		// Очищаем от пробелов
		for i, at := range aircraftTypes {
			aircraftTypes[i] = strings.TrimSpace(at)
		}
		// Если только одно значение - используем обычный фильтр
		if len(aircraftTypes) == 1 {
			filter["shr.aircraftType"] = aircraftTypes[0]
		} else {
			// Если несколько значений - используем $in
			filter["shr.aircraftType"] = bson.M{"$in": aircraftTypes}
		}
		fmt.Printf("✈️ Фильтр по типам самолетов: %v\n", aircraftTypes)
	}

	// Добавляем фильтры если они переданы
	if operatorType != "" {
		// Разделяем строку по запятым и убираем пробелы
		operatorTypes := strings.Split(operatorType, ",")
		// Очищаем от пробелов
		for i, at := range operatorTypes {
			operatorTypes[i] = strings.TrimSpace(at)
		}
		// Если только одно значение - используем обычный фильтр
		if len(operatorTypes) == 1 {
			filter["shr.operatorType"] = operatorTypes[0]
		} else {
			// Если несколько значений - используем $in
			filter["shr.operatorType"] = bson.M{"$in": operatorTypes}
		}
		fmt.Printf("✈️ Фильтр по типам самолетов: %v\n", operatorTypes)
	}

	start, _ := time.Parse(time.RFC3339, dateDepFrom)
	end, _ := time.Parse(time.RFC3339, dateDepTo)

	if dateDepFrom != "" {
		if dateDepTo != "" {
			filter["searchFields.dateTime"] = bson.M{
				"$gte": start,
				"$lte": end,
			}
		} else {
			filter["searchFields.dateTime"] = bson.M{"$gte": start}
		}
	} else if dateDepTo != "" {
		filter["searchFields.dateTime"] = bson.M{"$lte": end}
	}

	if sid != "" {
		if sidInt, err := strconv.Atoi(sid); err == nil {
			filter["shr.sid"] = sidInt
		} else {
			fmt.Printf("⚠️ Некорректный формат sid: %s\n", sid)
		}
	}

	if flightDurationMin != "" {
		durationMin, err := strconv.Atoi(flightDurationMin)
		if err == nil {
			if flightDurationMax != "" {
				durationMax, err := strconv.Atoi(flightDurationMax)
				if err == nil {
					// Если минимальное значение = 0, включаем также null значения
					if durationMin == 0 {
						filter["$or"] = []bson.M{
							{"shr.flightDuration": bson.M{"$gte": durationMin, "$lte": durationMax}},
							{"shr.flightDuration": nil},
						}
					} else {
						filter["shr.flightDuration"] = bson.M{
							"$gte": durationMin,
							"$lte": durationMax,
						}
					}
				}
			} else {
				// Если минимальное значение = 0, включаем также null значения
				if durationMin == 0 {
					filter["$or"] = []bson.M{
						{"shr.flightDuration": bson.M{"$gte": durationMin}},
						{"shr.flightDuration": nil},
					}
				} else {
					filter["shr.flightDuration"] = bson.M{"$gte": durationMin}
				}
			}
		}
	} else if flightDurationMax != "" {
		durationMax, err := strconv.Atoi(flightDurationMax)
		if err == nil {
			// Для максимального значения тоже можно включить null, если нужно
			filter["shr.flightDuration"] = bson.M{"$lte": durationMax}
		}
	}

	if region != "" {
		filter["region"] = region
	}

	// Создаем pipeline для агрегации
	pipeline := mongo.Pipeline{}

	// Добавляем стадию match если есть фильтры
	if len(filter) > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$match", Value: filter}})
	}

	// Проекция нужных полей
	projectFields := bson.M{
		"region":           1,
		"sid":              "$shr.sid",
		"aircraftIndex":    "$shr.aircraftIndex",
		"aircraftType":     "$shr.aircraftType",
		"aircraftQuantity": "$shr.aircraftQuantity",
		"operator":         "$shr.operator",
		"operatorType":     "$shr.operatorType",
		"flightDuration":   "$shr.flightDuration",
		"coordinatesDep": bson.M{
			"$let": bson.M{
				"vars": bson.M{
					"depCoords": bson.M{
						"$cond": bson.M{
							"if": bson.M{"$and": bson.A{
								bson.M{"$ifNull": bson.A{"$dep.coordinates.lat", false}},
								bson.M{"$ifNull": bson.A{"$dep.coordinates.lon", false}},
							}},
							"then": bson.M{
								"$concat": bson.A{
									bson.M{"$toString": "$dep.coordinates.lat"},
									" ",
									bson.M{"$toString": "$dep.coordinates.lon"},
								},
							},
							"else": nil,
						},
					},
					"shrCoords": bson.M{
						"$cond": bson.M{
							"if": bson.M{"$and": bson.A{
								bson.M{"$ifNull": bson.A{"$shr.coordinatesDep.lat", false}},
								bson.M{"$ifNull": bson.A{"$shr.coordinatesDep.lon", false}},
							}},
							"then": bson.M{
								"$concat": bson.A{
									bson.M{"$toString": "$shr.coordinatesDep.lat"},
									" ",
									bson.M{"$toString": "$shr.coordinatesDep.lon"},
								},
							},
							"else": nil,
						},
					},
				},
				"in": bson.M{"$ifNull": bson.A{"$$depCoords", "$$shrCoords"}},
			},
		},
		"coordinatesArr": bson.M{
			"$let": bson.M{
				"vars": bson.M{
					"arrCoords": bson.M{
						"$cond": bson.M{
							"if": bson.M{"$and": bson.A{
								bson.M{"$ifNull": bson.A{"$arr.coordinates.lat", false}},
								bson.M{"$ifNull": bson.A{"$arr.coordinates.lon", false}},
							}},
							"then": bson.M{
								"$concat": bson.A{
									bson.M{"$toString": "$arr.coordinates.lat"},
									" ",
									bson.M{"$toString": "$arr.coordinates.lon"},
								},
							},
							"else": nil,
						},
					},
					"shrCoords": bson.M{
						"$cond": bson.M{
							"if": bson.M{"$and": bson.A{
								bson.M{"$ifNull": bson.A{"$shr.coordinatesArr.lat", false}},
								bson.M{"$ifNull": bson.A{"$shr.coordinatesArr.lon", false}},
							}},
							"then": bson.M{
								"$concat": bson.A{
									bson.M{"$toString": "$shr.coordinatesArr.lat"},
									" ",
									bson.M{"$toString": "$shr.coordinatesArr.lon"},
								},
							},
							"else": nil,
						},
					},
				},
				"in": bson.M{"$ifNull": bson.A{"$$arrCoords", "$$shrCoords"}},
			},
		},
		//"rawText": "$shr.rawText",
	}

	// Добавляем поля дат с учетом временной зоны
	if timezone != "" {
		// Получаем смещение в часах для временной зоны
		offsetHours := getTimezoneOffset(timezone)

		projectFields["dateDep"] = bson.M{
			"$cond": bson.M{
				"if": bson.M{"$ne": bson.A{"$searchFields.dateTime", nil}},
				"then": bson.M{"$dateToString": bson.M{
					"date":   bson.M{"$add": bson.A{"$searchFields.dateTime", offsetHours * 60 * 60 * 1000}}, // смещение в миллисекундах
					"format": "%Y-%m-%dT%H:%M:%S.%LZ",
				}},
				"else": nil,
			},
		}
		projectFields["dateArr"] = bson.M{
			"$cond": bson.M{
				"if": bson.M{"$ne": bson.A{"$searchFields.arrDatetime", nil}},
				"then": bson.M{"$dateToString": bson.M{
					"date":   bson.M{"$add": bson.A{"$searchFields.arrDatetime", offsetHours * 60 * 60 * 1000}}, // смещение в миллисекундах
					"format": "%Y-%m-%dT%H:%M:%S.%LZ",
				}},
				"else": nil,
			},
		}
	} else {
		// Без временной зоны - возвращаем как есть
		projectFields["dateDep"] = "$searchFields.dateTime"
		projectFields["dateArr"] = "$searchFields.arrDatetime"
	}

	// Проекция нужных полей
	pipeline = append(pipeline, bson.D{{Key: "$project", Value: projectFields}})

	// Сортировка по sid
	pipeline = append(pipeline, bson.D{{Key: "$sort", Value: bson.M{"sid": 1}}})

	// Пагинация
	pipeline = append(pipeline, bson.D{{Key: "$skip", Value: skip}})
	pipeline = append(pipeline, bson.D{{Key: "$limit", Value: limitInt}})

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

	// Получаем общее количество с учетом фильтров
	countFilter := filter
	totalCount, err := flightDataCollection.CountDocuments(ctx, countFilter)
	if err != nil {
		fmt.Printf("❌ Ошибка подсчета общего количества: %v\n", err)
		totalCount = 0
	}

	// Получаем метаданные для фильтров
	var filtersMeta bson.M

	// Получаем список aircraftTypes
	aircraftTypesCursor, err := aircraftTypeCollection.Find(ctx, bson.M{})
	var aircraftTypes []string
	if err == nil {
		defer aircraftTypesCursor.Close(ctx)
		var types []bson.M
		if err := aircraftTypesCursor.All(ctx, &types); err == nil {
			for _, t := range types {
				if aircraftType, ok := t["aircraftType"].(string); ok {
					aircraftTypes = append(aircraftTypes, aircraftType)
				}
			}
		}
	}

	operatorTypes := []string{"Юр. лицо", "Физ. лицо", "Не определено"}

	maxDurationPipeline := mongo.Pipeline{
		{
			{Key: "$group", Value: bson.M{
				"_id":               nil,
				"maxFlightDuration": bson.M{"$max": "$shr.flightDuration"},
			}},
		},
	}

	maxDurationCursor, err := flightDataCollection.Aggregate(ctx, maxDurationPipeline)
	var maxFlightDuration int = 0
	if err == nil {
		defer maxDurationCursor.Close(ctx)
		var maxResults []bson.M
		if err := maxDurationCursor.All(ctx, &maxResults); err == nil && len(maxResults) > 0 {
			maxFlightDuration = int(maxResults[0]["maxFlightDuration"].(float64))
		}
	} else {
		fmt.Printf("❌ Ошибка получения maxFlightDuration: %v\n", err)
	}

	fmt.Printf("📊 Максимальная продолжительность полета по всей таблице: %d\n", maxFlightDuration)

	filtersMeta = bson.M{
		"aircraftTypes":     aircraftTypes,
		"maxFlightDuration": maxFlightDuration,
		"operatorTypes":     operatorTypes,
	}

	fmt.Printf("📈 Получено %d записей из %d (страница %d)\n", len(results), totalCount, pageInt)

	// Формируем ответ с метаданными пагинации и фильтров
	response := bson.M{
		"data": results,
		"pagination": bson.M{
			"page":       pageInt,
			"limit":      limitInt,
			"total":      totalCount,
			"totalPages": (totalCount + int64(limitInt) - 1) / int64(limitInt),
		},
		"filtersMeta": filtersMeta,
	}

	c.JSON(http.StatusOK, response)
}

// getTimezoneOffset возвращает смещение временной зоны в часах
func getTimezoneOffset(timezone string) int64 {

	russianTimezones := map[string]int64{
		"Europe/Kaliningrad": -2,  // UTC+2 - Калининградская область
		"Europe/Moscow":      -3,  // UTC+3 - Москва, Центральная Россия
		"Europe/Samara":      -4,  // UTC+4 - Самара, Удмуртия
		"Asia/Yekaterinburg": -5,  // UTC+5 - Екатеринбург, Урал
		"Asia/Omsk":          -6,  // UTC+6 - Омск, Западная Сибирь
		"Asia/Krasnoyarsk":   -7,  // UTC+7 - Красноярск, Центральная Сибирь
		"Asia/Irkutsk":       -8,  // UTC+8 - Иркутск, Восточная Сибирь
		"Asia/Chita":         -9,  // UTC+9 - Чита, Забайкалье
		"Asia/Vladivostok":   -10, // UTC+10 - Владивосток, Приморский край
		"Asia/Magadan":       -11, // UTC+11 - Магадан, Магаданская область
		"Asia/Kamchatka":     -12, // UTC+12 - Камчатка
		"Asia/Anadyr":        -12, // UTC+12 - Анадырь, Чукотка
		"Europe/Volgograd":   -3,  // UTC+3 - Волгоград
		"Europe/Astrakhan":   -4,  // UTC+4 - Астрахань
		"UTC":                0,   // UTC+0
	}

	if offset, exists := russianTimezones[timezone]; exists {
		return offset
	}

	// По умолчанию UTC
	return 0
}

func getYearlyStats(c *gin.Context, collection useTables) {
	flightDataCollection := collection.flightDataCollection

	// Получаем параметры из query string
	region := c.Query("region")
	from := c.Query("from")
	to := c.Query("to")

	if region == "" || from == "" || to == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Параметры region, from и to обязательны"})
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

	// Нормализуем даты (отбрасываем время)
	startDate := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, start.Location())
	endDate := time.Date(end.Year(), end.Month(), end.Day(), 0, 0, 0, 0, end.Location())

	fmt.Printf("📅 Получение статистики по дням для региона '%s' с %s по %s\n",
		region, startDate.Format("2006-01-02"), endDate.Format("2006-01-02"))

	// Создаем pipeline для агрегации
	pipeline := mongo.Pipeline{
		// Фильтруем по региону и дате
		{{Key: "$match", Value: bson.M{
			"region": region,
			"searchFields.dateTime": bson.M{
				"$gte": startDate,
				"$lte": endDate.Add(24*time.Hour - time.Second),
			},
		}}},
		// Извлекаем дату (без времени)
		{{Key: "$project", Value: bson.M{
			"date": bson.M{
				"$dateToString": bson.M{
					"format": "%Y-%m-%d",
					"date":   "$searchFields.dateTime",
				},
			},
		}}},
		// Группируем по дате
		{{Key: "$group", Value: bson.M{
			"_id":         "$date",
			"flightCount": bson.M{"$sum": 1},
		}}},
		// Проектируем в нужный формат
		{{Key: "$project", Value: bson.M{
			"_id":         0,
			"date":        "$_id",
			"flightCount": 1,
		}}},
		// Сортируем по дате
		{{Key: "$sort", Value: bson.M{"date": 1}}},
	}

	cursor, err := flightDataCollection.Aggregate(ctx, pipeline)
	if err != nil {
		fmt.Printf("❌ Ошибка агрегации: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка выполнения запроса к базе данных"})
		return
	}
	defer cursor.Close(ctx)

	var aggResults []bson.M
	if err := cursor.All(ctx, &aggResults); err != nil {
		fmt.Printf("❌ Ошибка декодирования: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка декодирования данных"})
		return
	}

	// Создаем мапу для быстрого доступа к результатам агрегации
	resultMap := make(map[string]int)
	for _, result := range aggResults {
		if date, ok := result["date"].(string); ok {
			if flightCount, ok := result["flightCount"].(int32); ok {
				resultMap[date] = int(flightCount)
			}
		}
	}

	// Генерируем полный список дней в диапазоне
	var fullResults []bson.M
	for current := startDate; !current.After(endDate); current = current.AddDate(0, 0, 1) {
		dateStr := current.Format("2006-01-02")

		flightCount := 0
		if count, exists := resultMap[dateStr]; exists {
			flightCount = count
		}

		fullResults = append(fullResults, bson.M{
			"date":        dateStr,
			"flightCount": flightCount,
		})
	}

	fmt.Printf("📈 Статистика по дням: найдено %d дней с активностью из %d дней в диапазоне\n",
		len(aggResults), len(fullResults))

	c.JSON(http.StatusOK, fullResults)
}

func getPeakHour(c *gin.Context, collection useTables) {
	flightDataCollection := collection.flightDataCollection

	// Получаем параметры из query string
	region := c.Query("region")
	date := c.Query("date")

	if region == "" || date == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Параметры region и date обязательны"})
		return
	}

	// Парсим дату
	targetDate, err := time.Parse("2006-01-02", date)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Неверный формат date. Используйте YYYY-MM-DD"})
		return
	}

	// Вычисляем временной диапазон для целевого дня
	startOfDay := time.Date(targetDate.Year(), targetDate.Month(), targetDate.Day(), 0, 0, 0, 0, time.UTC)
	endOfDay := startOfDay.Add(24*time.Hour - time.Second)

	ctx := context.Background()

	fmt.Printf("⏰ Получение статистики по часам для региона '%s' за %s\n", region, date)

	// Создаем pipeline для агрегации
	pipeline := mongo.Pipeline{
		// Фильтруем по региону и дате
		{{Key: "$match", Value: bson.M{
			"region": region,
			"searchFields.dateTime": bson.M{
				"$gte": startOfDay,
				"$lte": endOfDay,
			},
		}}},
		// Извлекаем час из datetime
		{{Key: "$project", Value: bson.M{
			"hour":             bson.M{"$hour": "$searchFields.dateTime"},
			"aircraftQuantity": "$shr.aircraftQuantity",
		}}},
		// Группируем по часам
		{{Key: "$group", Value: bson.M{
			"_id":         "$hour",
			"flightCount": bson.M{"$sum": 1},
			"droneCount":  bson.M{"$sum": "$aircraftQuantity"},
		}}},
		// Форматируем час в строку
		{{Key: "$project", Value: bson.M{
			"_id": 0,
			"hour": bson.M{
				"$concat": bson.A{
					bson.M{"$toString": "$_id"},
					":00",
				},
			},
			"flightCount": 1,
			"droneCount":  1,
		}}},
		// Сортируем по часу
		{{Key: "$sort", Value: bson.M{"hour": 1}}},
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

	fmt.Printf("📈 Найдено часов с активностью: %d\n", len(results))

	c.JSON(http.StatusOK, results)
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

// getExistingSIDs возвращает множество существующих SID из базы данных
func getExistingSIDs(collection *mongo.Collection) (map[int]bool, error) {
	ctx := context.Background()
	existingSIDs := make(map[int]bool)

	// Проекция только поля SID для оптимизации
	opts := options.Find().SetProjection(bson.M{"shr.sid": 1})

	cursor, err := collection.Find(ctx, bson.M{"shr.sid": bson.M{"$ne": nil}}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	// Структура для временного хранения результатов
	var results []struct {
		SHR struct {
			SID *int `bson:"sid"`
		} `bson:"shr"`
	}

	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	// Заполняем множество SID
	for _, result := range results {
		if result.SHR.SID != nil {
			existingSIDs[*result.SHR.SID] = true
		}
	}

	return existingSIDs, nil
}

// bulkInsert выполняет массовую вставку документов
func bulkInsert(collection *mongo.Collection, documents []any) error {
	ctx := context.Background()

	// Используем неупорядоченную вставку для лучшей производительности
	opts := options.BulkWrite().SetOrdered(false)

	var models []mongo.WriteModel
	for _, doc := range documents {
		model := mongo.NewInsertOneModel().SetDocument(doc)
		models = append(models, model)
	}

	_, err := collection.BulkWrite(ctx, models, opts)
	return err
}

func validateHeaders(headers []string) bool {

	if len(headers) < 4 {
		return false
	}
	// Проверяем названия колонок (регистронезависимо)
	expectedHeaders := map[int]string{
		1: "shr",
		2: "dep",
		3: "arr",
	}

	for i, expected := range expectedHeaders {
		if i < len(headers) {
			actual := strings.ToLower(strings.TrimSpace(headers[i]))
			if actual != expected {
				fmt.Printf("❌ Неверный заголовок колонки %d: ожидается '%s', получено '%s'\n",
					i, expected, actual)
				return false
			}
		}
	}

	return true
}

// Парсинг и загрузка файла в базу
func uploadFiles(c *gin.Context, collection useTables, client *mongo.Client) {

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
	fmt.Printf("🔄 Обрабатываем лист: %s\n", firstSheet)

	// ПОТОКОВОЕ чтение БЕЗ загрузки всего файла в память
	rowsStream, err := xlsxFile.Rows(firstSheet)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка чтения строк"})
		return
	}
	defer rowsStream.Close()

	// Читаем ПЕРВУЮ строку (заголовки) и валидируем СРАЗУ
	if !rowsStream.Next() {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Файл пустой"})
		return
	}

	headers, err := rowsStream.Columns(excelize.Options{
		RawCellValue: true,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка чтения заголовков"})
		return
	}

	// ВАЛИДАЦИЯ ЗАГОЛОВКОВ ПЕРЕНЕСЕНА НАВЕРХ
	if !validateHeaders(headers) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Неверный формат файла. Ожидаются колонки: region, shr, dep, arr"})
		return
	}

	// СОЗДАЕМ ГЕОСЕРВИС ДЛЯ ИНТЕГРАЦИИ
	geoService := geoSearch.NewGeoService(client) // client - ваш MongoDB client
	geoParser := parsing.NewGeoIntegratedParser(geoService)

	// Получаем существующие SID из базы для проверки уникальности
	existingSIDs, err := getExistingSIDs(flightDataCollection)
	if err != nil {
		fmt.Printf("❌ Ошибка получения существующих SID: %v\n", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка проверки уникальности данных"})
		return
	}

	fmt.Printf("🔍 Найдено %d существующих SID в базе\n", len(existingSIDs))

	// Каналы для параллельной обработки
	jobs := make(chan []string, 50000)
	results := make(chan parsing.FlightData, 50000)

	// Запускаем worker'ов для параллельного парсинга
	numWorkers := runtime.NumCPU()
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for row := range jobs {
				flightData := geoParser.CreateFlightDataWithRegion(row)
				results <- flightData
			}
		}(w)
	}

	// Собираем результаты
	go func() {
		wg.Wait()
		close(results)
	}()

	// Читаем и обрабатываем строки ПОТОКОВО (начинаем со ВТОРОЙ строки, так как первую уже прочитали)
	var documents []any
	var insertedCount int
	var duplicateCount int
	var totalProcessed int
	batchSize := 1000

	// Запускаем горутину для отправки заданий
	go func() {
		// Читаем оставшиеся строки (начиная со второй)
		for rowsStream.Next() {
			row, err := rowsStream.Columns(excelize.Options{
				RawCellValue: true,
			})
			if err != nil {
				fmt.Printf("❌ Ошибка чтения строки: %v\n", err)
				continue
			}

			jobs <- row
		}
		close(jobs)

		if err := rowsStream.Error(); err != nil {
			fmt.Printf("❌ Ошибка потока строк: %v\n", err)
		}
	}()

	// Обрабатываем результаты
	for flightData := range results {
		totalProcessed++

		// Показываем прогресс каждые 5000 строк
		if totalProcessed%10000 == 0 {
			fmt.Printf("✅ Обработано %d строк\n", totalProcessed)
		}

		isUnique := true
		if _, exists := existingSIDs[flightData.SHRData.SID]; exists {
			isUnique = false
			duplicateCount++
		}

		if isUnique {
			documents = append(documents, flightData)
			existingSIDs[flightData.SHRData.SID] = true
			insertedCount++
		}

		// Bulk insert
		if len(documents) >= batchSize {
			if len(documents) > 0 {
				err := bulkInsert(flightDataCollection, documents)
				if err != nil {
					fmt.Printf("❌ Ошибка сохранения в базу: %v\n", err)
				}
				documents = nil
			}
		}
	}

	// Финальный bulk insert
	if len(documents) > 0 {
		err := bulkInsert(flightDataCollection, documents)
		if err != nil {
			fmt.Printf("❌ Ошибка финального сохранения: %v\n", err)
		}
	}

	fmt.Printf("\n📈 ИТОГ: Обработано %d строк, сохранено %d строк\n", totalProcessed, insertedCount)
	fmt.Println("=== ДАННЫЕ УСПЕШНО СОХРАНЕНЫ В MONGODB ===")

	//http ответ
	c.JSON(http.StatusOK, gin.H{
		"message":        "Данные успешно сохранены",
		"totalRows":      insertedCount,
		"processed":      totalProcessed,
		"insertedCount":  insertedCount,
		"duplicateCount": duplicateCount,
		"collection":     "flightData",
	})
}

// Обновляем список уникальных регионов
func updateRegionList(collection useTables) {

	regionListCollection := collection.regionListCollection
	subjectListCollection := collection.subjectListCollection

	ctx := context.Background()

	fmt.Println("🔄 Обновление списка регионов...")

	// Очищаем коллекцию regionList
	_, err := regionListCollection.DeleteMany(ctx, bson.M{})
	if err != nil {
		fmt.Printf("ошибка очистки коллекции regionList: %v", err)
	}
	fmt.Println("✅ Коллекция regionList очищена")

	// Получаем уникальные регионы из regionsGeo
	pipeline := mongo.Pipeline{
		{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$name"}}}},
		{{Key: "$project", Value: bson.D{{Key: "name", Value: "$_id"}, {Key: "_id", Value: 0}}}},
		{{Key: "$sort", Value: bson.D{{Key: "name", Value: 1}}}},
	}

	cursor, err := subjectListCollection.Aggregate(ctx, pipeline)
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
				"region":   region["name"],
			}
			documents = append(documents, document)
		}

		documents = append(documents, bson.M{
			"regionID": len(documents) + 1,
			"region":   "Регион не определен",
		})

		result, err := regionListCollection.InsertMany(ctx, documents)
		if err != nil {
			fmt.Printf("ошибка вставки регионов: %v", err)
		}

		fmt.Printf("📈 В коллекцию regionList добавлено %d уникальных регионов\n", len(result.InsertedIDs))
	} else {
		fmt.Println("ℹ️  Не найдено регионов для добавления")
	}

}

// Обновляем список уникальных типов воздушных судов
func updateAircraftTypeList(collection useTables) {
	aircraftTypeList := collection.aircraftTypeListCollection
	flightDataCollection := collection.flightDataCollection

	ctx := context.Background()

	fmt.Println("🔄 Обновление списка aircraftType...")

	// Очищаем коллекцию aircraftTypeList
	_, err := aircraftTypeList.DeleteMany(ctx, bson.M{})
	if err != nil {
		fmt.Printf("❌ Ошибка очистки коллекции aircraftTypeList: %v\n", err)
		return
	}
	fmt.Println("✅ Коллекция aircraftTypeList очищена")

	// Получаем уникальные типы ВС из flightData
	pipeline := mongo.Pipeline{
		// Группируем по shr.aircraftType
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$shr.aircraftType"},
		}}},
		// Фильтруем ненулевые значения
		{{Key: "$match", Value: bson.M{
			"_id": bson.M{"$ne": nil},
		}}},
		// Проектируем в нужный формат
		{{Key: "$project", Value: bson.D{
			{Key: "_id", Value: 0},
			{Key: "aircraftType", Value: "$_id"},
		}}},
		// Сортируем по типу ВС
		{{Key: "$sort", Value: bson.M{"aircraftType": 1}}},
	}

	cursor, err := flightDataCollection.Aggregate(ctx, pipeline)
	if err != nil {
		fmt.Printf("❌ Ошибка агрегации aircraftTypeList: %v\n", err)
		return
	}
	defer cursor.Close(ctx)

	var aircraftTypes []bson.M
	if err := cursor.All(ctx, &aircraftTypes); err != nil {
		fmt.Printf("❌ Ошибка декодирования aircraftTypeList: %v\n", err)
		return
	}

	// Вставляем уникальные типы ВС в aircraftTypeList
	if len(aircraftTypes) > 0 {
		var documents []any

		for i, aircraftType := range aircraftTypes {
			// Извлекаем значение aircraftType
			aircraftTypeValue, ok := aircraftType["aircraftType"].(string)
			if !ok || aircraftTypeValue == "" {
				continue
			}

			document := bson.M{
				"aircraftTypeID": i + 1,
				"aircraftType":   aircraftTypeValue,
			}
			documents = append(documents, document)
		}

		if len(documents) > 0 {
			result, err := aircraftTypeList.InsertMany(ctx, documents)
			if err != nil {
				fmt.Printf("❌ Ошибка вставки типов aircraftTypeList: %v\n", err)
			} else {
				fmt.Printf("📈 В коллекцию aircraftTypeList добавлено %d уникальных типов ВС\n", len(result.InsertedIDs))
			}
		}
	}
}

// Экспорт файла
func exportFlightTable(c *gin.Context, collection useTables) {
	flightDataCollection := collection.flightDataCollection

	// Получаем параметр формата
	format := c.DefaultQuery("format", "json")
	if format != "json" && format != "xlsx" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Неподдерживаемый формат. Используйте json или xlsx"})
		return
	}

	// Получаем параметры фильтров (те же что и в getFlightTable)
	aircraftType := c.Query("aircraftType")
	dateDepFrom := c.Query("dateDepFrom")
	dateDepTo := c.Query("dateDepTo")
	flightDurationMin := c.Query("flightDurationMin")
	flightDurationMax := c.Query("flightDurationMax")
	sid := c.Query("sid")
	region := c.Query("region")
	operatorType := c.Query("operatorType")

	ctx := context.Background()

	fmt.Printf("📤 Экспорт таблицы полетов в формате %s\n", format)

	// Создаем базовый фильтр (используем ту же логику что и в getFlightTable)
	filter := bson.M{}

	// Добавляем фильтры если они переданы
	if aircraftType != "" {
		aircraftTypes := strings.Split(aircraftType, ",")
		for i, at := range aircraftTypes {
			aircraftTypes[i] = strings.TrimSpace(at)
		}
		if len(aircraftTypes) == 1 {
			filter["shr.aircraftType"] = aircraftTypes[0]
		} else {
			filter["shr.aircraftType"] = bson.M{"$in": aircraftTypes}
		}
	}

	// ДОБАВЛЯЕМ ФИЛЬТР ПО ТИПУ ОПЕРАТОРА
	if operatorType != "" {
		operatorTypes := strings.Split(operatorType, ",")
		for i, ot := range operatorTypes {
			operatorTypes[i] = strings.TrimSpace(ot)
		}
		if len(operatorTypes) == 1 {
			filter["shr.operatorType"] = operatorTypes[0]
		} else {
			filter["shr.operatorType"] = bson.M{"$in": operatorTypes}
		}
		fmt.Printf("🏢 Фильтр по типам операторов: %v\n", operatorTypes)
	}

	start, _ := time.Parse(time.RFC3339, dateDepFrom)
	end, _ := time.Parse(time.RFC3339, dateDepTo)

	if dateDepFrom != "" {
		if dateDepTo != "" {
			filter["searchFields.dateTime"] = bson.M{
				"$gte": start,
				"$lte": end,
			}
		} else {
			filter["searchFields.dateTime"] = bson.M{"$gte": start}
		}
	} else if dateDepTo != "" {
		filter["searchFields.dateTime"] = bson.M{"$lte": end}
	}

	if sid != "" {
		if sidInt, err := strconv.Atoi(sid); err == nil {
			filter["shr.sid"] = sidInt
		}
	}

	if flightDurationMin != "" {
		durationMin, err := strconv.Atoi(flightDurationMin)
		if err == nil {
			if flightDurationMax != "" {
				durationMax, err := strconv.Atoi(flightDurationMax)
				if err == nil {
					if durationMin == 0 {
						filter["$or"] = []bson.M{
							{"shr.flightDuration": bson.M{"$gte": durationMin, "$lte": durationMax}},
							{"shr.flightDuration": nil},
						}
					} else {
						filter["shr.flightDuration"] = bson.M{
							"$gte": durationMin,
							"$lte": durationMax,
						}
					}
				}
			} else {
				if durationMin == 0 {
					filter["$or"] = []bson.M{
						{"shr.flightDuration": bson.M{"$gte": durationMin}},
						{"shr.flightDuration": nil},
					}
				} else {
					filter["shr.flightDuration"] = bson.M{"$gte": durationMin}
				}
			}
		}
	} else if flightDurationMax != "" {
		durationMax, err := strconv.Atoi(flightDurationMax)
		if err == nil {
			filter["shr.flightDuration"] = bson.M{"$lte": durationMax}
		}
	}

	if region != "" {
		filter["region"] = region
	}

	// Создаем pipeline для агрегации (без пагинации)
	pipeline := mongo.Pipeline{}

	// Добавляем стадию match если есть фильтры
	if len(filter) > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$match", Value: filter}})
	}

	// Проекция нужных полей
	pipeline = append(pipeline, bson.D{{Key: "$project", Value: bson.M{
		"region":           1,
		"sid":              "$shr.sid",
		"aircraftIndex":    "$shr.aircraftIndex",
		"aircraftType":     "$shr.aircraftType",
		"aircraftQuantity": "$shr.aircraftQuantity",
		"operator":         "$shr.operator",
		"operatorType":     "$shr.operatorType",
		"dateDep":          "$searchFields.dateTime",
		"dateArr":          "$searchFields.arrDatetime",
		"flightDuration":   "$shr.flightDuration",
		"coordinatesDep": bson.M{
			"$let": bson.M{
				"vars": bson.M{
					"depCoords": bson.M{
						"$cond": bson.M{
							"if": bson.M{"$and": bson.A{
								bson.M{"$ifNull": bson.A{"$dep.coordinates.lat", false}},
								bson.M{"$ifNull": bson.A{"$dep.coordinates.lon", false}},
							}},
							"then": bson.M{
								"$concat": bson.A{
									bson.M{"$toString": "$dep.coordinates.lat"},
									" ",
									bson.M{"$toString": "$dep.coordinates.lon"},
								},
							},
							"else": nil,
						},
					},
					"shrCoords": bson.M{
						"$cond": bson.M{
							"if": bson.M{"$and": bson.A{
								bson.M{"$ifNull": bson.A{"$shr.coordinatesDep.lat", false}},
								bson.M{"$ifNull": bson.A{"$shr.coordinatesDep.lon", false}},
							}},
							"then": bson.M{
								"$concat": bson.A{
									bson.M{"$toString": "$shr.coordinatesDep.lat"},
									" ",
									bson.M{"$toString": "$shr.coordinatesDep.lon"},
								},
							},
							"else": nil,
						},
					},
				},
				"in": bson.M{"$ifNull": bson.A{"$$depCoords", "$$shrCoords"}},
			},
		},
		"coordinatesArr": bson.M{
			"$let": bson.M{
				"vars": bson.M{
					"arrCoords": bson.M{
						"$cond": bson.M{
							"if": bson.M{"$and": bson.A{
								bson.M{"$ifNull": bson.A{"$arr.coordinates.lat", false}},
								bson.M{"$ifNull": bson.A{"$arr.coordinates.lon", false}},
							}},
							"then": bson.M{
								"$concat": bson.A{
									bson.M{"$toString": "$arr.coordinates.lat"},
									" ",
									bson.M{"$toString": "$arr.coordinates.lon"},
								},
							},
							"else": nil,
						},
					},
					"shrCoords": bson.M{
						"$cond": bson.M{
							"if": bson.M{"$and": bson.A{
								bson.M{"$ifNull": bson.A{"$shr.coordinatesArr.lat", false}},
								bson.M{"$ifNull": bson.A{"$shr.coordinatesArr.lon", false}},
							}},
							"then": bson.M{
								"$concat": bson.A{
									bson.M{"$toString": "$shr.coordinatesArr.lat"},
									" ",
									bson.M{"$toString": "$shr.coordinatesArr.lon"},
								},
							},
							"else": nil,
						},
					},
				},
				"in": bson.M{"$ifNull": bson.A{"$$arrCoords", "$$shrCoords"}},
			},
		},
	}}})

	// Сортировка по sid
	pipeline = append(pipeline, bson.D{{Key: "$sort", Value: bson.M{"sid": 1}}})

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

	fmt.Printf("📊 Экспортируется %d записей\n", len(results))

	// Генерируем имя файла с timestamp
	timestamp := time.Now().Format("02-01-2006")
	filename := fmt.Sprintf("flights_export_%s", timestamp)

	switch format {
	case "json":
		exportJSON(c, results, filename)
	case "xlsx":
		exportXLSX(c, results, filename)
	}
}

// exportJSON экспортирует данные в JSON файл
func exportJSON(c *gin.Context, data []bson.M, filename string) {
	// Конвертируем в чистый JSON
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка формирования JSON"})
		return
	}

	// Устанавливаем заголовки для скачивания
	c.Header("Content-Description", "File Transfer")
	c.Header("Content-Disposition", "attachment; filename="+filename+".json")
	c.Header("Content-Type", "application/json")
	c.Header("Content-Length", strconv.Itoa(len(jsonData)))

	// Отправляем файл
	c.Data(http.StatusOK, "application/json", jsonData)

	fmt.Printf("✅ JSON файл экспортирован: %s.json\n", filename)
}

// exportXLSX экспортирует данные в XLSX файл
func exportXLSX(c *gin.Context, data []bson.M, filename string) {
	file := xlsx.NewFile()
	sheet, err := file.AddSheet("Flights")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка создания XLSX файла"})
		return
	}

	// Создаем заголовки - ДОБАВЛЯЕМ ОПЕРАТОРА И ТИП ОПЕРАТОРА
	headers := []string{
		"Регион", "Системный ID", "Индекс ВС", "Тип ВС", "Количество ВС",
		"Время вылета", "Время прибытия", "Длительность полета (мин)",
		"Координаты вылета", "Координаты прибытия", "Оператор", "Тип оператора",
	}

	headerRow := sheet.AddRow()
	for _, header := range headers {
		cell := headerRow.AddCell()
		cell.Value = header
		cell.GetStyle().Font.Bold = true
	}

	// Заполняем данные
	for _, record := range data {
		row := sheet.AddRow()

		// Регион
		if region, ok := record["region"].(string); ok {
			row.AddCell().Value = region
		} else {
			row.AddCell().Value = ""
		}

		// SID
		if sid, ok := record["sid"].(int64); ok {
			row.AddCell().SetString(fmt.Sprint(sid))
		} else {
			row.AddCell().Value = ""
		}

		// Индекс ВС
		if aircraftIndex, ok := record["aircraftIndex"].(string); ok {
			row.AddCell().Value = aircraftIndex
		} else {
			row.AddCell().Value = ""
		}

		// Тип ВС
		if aircraftType, ok := record["aircraftType"].(string); ok {
			row.AddCell().Value = aircraftType
		} else {
			row.AddCell().Value = ""
		}

		// Кол-во ВС
		if aircraftQuantity, ok := record["aircraftQuantity"].(int32); ok {
			row.AddCell().SetInt(int(aircraftQuantity))
		} else {
			row.AddCell().Value = ""
		}

		// Дата вылета
		if dateDep, ok := record["dateDep"].(primitive.DateTime); ok {
			row.AddCell().Value = time.Unix(int64(dateDep)/1000, 0).Format("02.01.2006 15:04")
		} else {
			row.AddCell().Value = ""
		}

		// Дата прилета
		if dateArr, ok := record["dateArr"].(primitive.DateTime); ok {
			row.AddCell().Value = time.Unix(int64(dateArr)/1000, 0).Format("02.01.2006 15:04")
		} else {
			row.AddCell().Value = ""
		}

		// Длительность полета
		if flightDuration, ok := record["flightDuration"].(float64); ok {
			row.AddCell().SetInt(int(flightDuration))
		} else {
			row.AddCell().Value = ""
		}

		// Координаты вылета
		if coordsDep, ok := record["coordinatesDep"].(string); ok {
			row.AddCell().Value = coordsDep
		} else {
			row.AddCell().Value = ""
		}

		// Координаты прилета
		if coordsArr, ok := record["coordinatesArr"].(string); ok {
			row.AddCell().Value = coordsArr
		} else {
			row.AddCell().Value = ""
		}

		// Оператор
		if operator, ok := record["operator"].(string); ok {
			row.AddCell().Value = operator
		} else {
			row.AddCell().Value = ""
		}

		// Тип оператора
		if operatorType, ok := record["operatorType"].(string); ok {
			row.AddCell().Value = operatorType
		} else {
			row.AddCell().Value = ""
		}

	}

	// Сохраняем во временный буфер
	var buf bytes.Buffer
	if err := file.Write(&buf); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка сохранения XLSX файла"})
		return
	}

	// Устанавливаем заголовки для скачивания
	c.Header("Content-Description", "File Transfer")
	c.Header("Content-Disposition", "attachment; filename="+filename+".xlsx")
	c.Header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	c.Header("Content-Length", strconv.Itoa(buf.Len()))

	// Отправляем файл
	c.Data(http.StatusOK, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", buf.Bytes())

	fmt.Printf("✅ XLSX файл экспортирован: %s.xlsx\n", filename)
}

/* // Или еще более компактный вариант:
func formatDateTimeShort(t time.Time) string {
	return fmt.Sprintf("%02d.%02d.%d %d:%02d",
		t.Day(), t.Month(), t.Year(),
		t.Hour(), t.Minute())
} */
