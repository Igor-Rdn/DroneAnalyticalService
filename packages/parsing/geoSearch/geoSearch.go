package geoSearch

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// GeoService сервис для геопространственного поиска с 2dsphere индексом

// FlightData структура для хранения данных о полете
type FlightData struct {
	ID        primitive.ObjectID `bson:"_id"`
	Departure struct {
		Coordinates *Coordinate `bson:"coordinates,omitempty"`
	} `bson:"dep"`
	SHRData struct {
		CoordinatesDep *Coordinate `bson:"coordinatesDep,omitempty"`
	} `bson:"shr"`
}

// UpdateOperation операция обновления
type UpdateOperation struct {
	ID      primitive.ObjectID
	Subject string
}

// Coordinate структура координат
type Coordinate struct {
	Lat float64 `bson:"lat"`
	Lon float64 `bson:"lon"`
}

// GeoService сервис для геопространственного поиска с 2dsphere индексом
type GeoService struct {
	client            *mongo.Client
	regionsCollection *mongo.Collection
	cache             *RegionCache // Кэш для часто запрашиваемых регионов
}

// RegionCache кэш для хранения результатов поиска регионов
type RegionCache struct {
	sync.RWMutex
	data map[CacheKey]string
}

type CacheKey struct {
	lat float64
	lon float64
}

// NewGeoService создает новый геосервис
func NewGeoService(client *mongo.Client) *GeoService {
	return &GeoService{
		client:            client,
		regionsCollection: client.Database("admin").Collection("regionsGeo"),
		cache: &RegionCache{
			data: make(map[CacheKey]string),
		},
	}
}

// FindRegionForPoint использует 2dsphere индекс для поиска региона (ОПТИМИЗИРОВАННАЯ ВЕРСИЯ)
func (gs *GeoService) FindRegionForPoint(lat, lon float64) (string, error) {
	// Проверяем кэш сначала
	if cached := gs.getFromCache(lat, lon); cached != "" {
		return cached, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second) // Уменьшаем таймаут
	defer cancel()

	// GeoJSON точка для поиска
	point := bson.M{
		"type":        "Point",
		"coordinates": []float64{lon, lat},
	}

	// ОПТИМИЗИРОВАННЫЙ ЗАПРОС - используем Find вместо Aggregate
	filter := bson.M{
		"geometry": bson.M{
			"$geoIntersects": bson.M{
				"$geometry": point,
			},
		},
	}

	opts := options.FindOne().
		SetProjection(bson.M{"name": 1}).
		SetMaxTime(3 * time.Second) // Ограничиваем время выполнения

	regionName := "Регион не определен"

	var result bson.M
	if err := gs.regionsCollection.FindOne(ctx, filter, opts).Decode(&result); err != nil {
		return regionName, nil
	}

	if name, ok := result["name"].(string); ok {
		regionName = name
	}

	// Сохраняем в кэш
	gs.setCache(lat, lon, regionName)

	return regionName, nil
}

// FindRegionsForPointsBatch пакетный поиск регионов для нескольких точек
func (gs *GeoService) FindRegionsForPointsBatch(points []struct {
	Lat float64
	Lon float64
	ID  primitive.ObjectID
}) (map[primitive.ObjectID]string, error) {
	results := make(map[primitive.ObjectID]string)

	// Сначала проверяем кэш
	for _, point := range points {
		if cached := gs.getFromCache(point.Lat, point.Lon); cached != "" {
			results[point.ID] = cached
		}
	}

	// Если все точки найдены в кэше, возвращаем результат
	if len(results) == len(points) {
		return results, nil
	}

	// Ищем оставшиеся точки в базе
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Создаем условия для $or запроса
	var orConditions []bson.M
	pointMap := make(map[string]primitive.ObjectID) // Для сопоставления точек с ID

	for _, point := range points {
		if _, exists := results[point.ID]; exists {
			continue // Уже найдено в кэше
		}

		pointKey := fmt.Sprintf("%.6f:%.6f", point.Lat, point.Lon)
		pointMap[pointKey] = point.ID

		orConditions = append(orConditions, bson.M{
			"geometry": bson.M{
				"$geoIntersects": bson.M{
					"$geometry": bson.M{
						"type":        "Point",
						"coordinates": []float64{point.Lon, point.Lat},
					},
				},
			},
		})
	}

	if len(orConditions) == 0 {
		return results, nil
	}

	filter := bson.M{"$or": orConditions}
	opts := options.Find().SetProjection(bson.M{"name": 1})

	cursor, err := gs.regionsCollection.Find(ctx, filter, opts)
	if err != nil {
		return results, fmt.Errorf("ошибка пакетного поиска: %v", err)
	}
	defer cursor.Close(ctx)

	// Обрабатываем результаты
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}

		// Здесь нужна дополнительная логика для сопоставления результатов с точками
		// В реальном сценарии нужно выполнять отдельные запросы или использовать $geoWithin
	}

	return results, nil
}

// getFromCache получает значение из кэша
func (gs *GeoService) getFromCache(lat, lon float64) string {
	gs.cache.RLock()
	defer gs.cache.RUnlock()

	key := CacheKey{lat: lat, lon: lon}
	if value, exists := gs.cache.data[key]; exists {
		return value
	}
	return ""
}

// setCache сохраняет значение в кэш
func (gs *GeoService) setCache(lat, lon float64, value string) {
	gs.cache.Lock()
	defer gs.cache.Unlock()

	key := CacheKey{lat: lat, lon: lon}
	gs.cache.data[key] = value

	// Ограничиваем размер кэша (опционально)
	if len(gs.cache.data) > 10000 {
		// Простая стратегия: очищаем кэш когда он становится слишком большим
		gs.cache.data = make(map[CacheKey]string)
	}
}

// UpdateFlightRegions ОПТИМИЗИРОВАННАЯ версия
func UpdateFlightRegions(client *mongo.Client) error {
	geoService := NewGeoService(client)
	flightDataCollection := geoService.client.Database("admin").Collection("flightData")
	ctx := context.Background()

	fmt.Println("🚀 Запускаем ОПТИМИЗИРОВАННУЮ обработку с 2dsphere индексом...")

	// Используем проекцию чтобы не загружать лишние данные
	opts := options.Find().SetProjection(bson.M{
		"_id": 1,
		"dep": 1,
		"shr": 1,
	})

	cursor, err := flightDataCollection.Find(ctx, bson.M{}, opts)
	if err != nil {
		return fmt.Errorf("ошибка получения данных полетов: %v", err)
	}
	defer cursor.Close(ctx)

	startTime := time.Now()

	// Увеличиваем буферы для лучшей производительности
	jobs := make(chan FlightData, 20000)
	results := make(chan UpdateOperation, 20000)

	// Настройки параллелизма - можно увеличить если CPU позволяет
	numWorkers := runtime.NumCPU() // Увеличиваем количество воркеров
	batchSize := 2000              // Увеличиваем размер батча

	// Запускаем worker'ов
	var wgWorkers sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wgWorkers.Add(1)
		go geoService.regionWorkerOptimized(i, jobs, results, &wgWorkers)
	}

	// Запускаем сборщик результатов
	var wgCollector sync.WaitGroup
	wgCollector.Add(1)
	collectorErr := make(chan error, 1)

	go func() {
		defer wgCollector.Done()
		err := geoService.resultCollectorOptimized(flightDataCollection, results, startTime, batchSize)
		if err != nil {
			collectorErr <- err
		}
	}()

	// Отправляем данные в workers
	var sentCount int
	var lastLogTime = time.Now()

	for cursor.Next(ctx) {
		var flightData FlightData
		if err := cursor.Decode(&flightData); err != nil {
			log.Printf("❌ Ошибка декодирования: %v", err)
			continue
		}

		jobs <- flightData
		sentCount++

		// Логируем прогресс отправки
		if sentCount%10000 == 0 || time.Since(lastLogTime) > 2*time.Second {
			elapsed := time.Since(startTime)
			speed := float64(sentCount) / elapsed.Seconds()
			fmt.Printf("📤 Отправлено: %d | Скорость: %.1f doc/сек\n", sentCount, speed)
			lastLogTime = time.Now()
		}
	}

	// Завершаем пайплайн
	close(jobs)
	wgWorkers.Wait()
	close(results)
	wgCollector.Wait()

	// Проверяем ошибки сборщика
	select {
	case err := <-collectorErr:
		return err
	default:
	}

	totalTime := time.Since(startTime)
	fmt.Printf("\n🎉 ОПТИМИЗИРОВАННАЯ ОБРАБОТКА ЗАВЕРШЕНА!\n")
	fmt.Printf("📊 ИТОГИ:\n")
	fmt.Printf("   Обработано документов: %d\n", sentCount)
	fmt.Printf("   Общее время: %v\n", totalTime.Round(time.Second))
	fmt.Printf("   Средняя скорость: %.1f документов/сек\n", float64(sentCount)/totalTime.Seconds())

	return nil
}

// regionWorkerOptimized оптимизированный воркер
func (gs *GeoService) regionWorkerOptimized(id int, jobs <-chan FlightData, results chan<- UpdateOperation, wg *sync.WaitGroup) {
	defer wg.Done()

	var processed int
	var batch []FlightData
	batchSize := 50 // Обрабатываем небольшими батчами

	for job := range jobs {
		batch = append(batch, job)

		if len(batch) >= batchSize {
			gs.processBatch(batch, results)
			processed += len(batch)
			batch = batch[:0] // Очищаем batch
		}
	}

	// Обрабатываем оставшиеся
	if len(batch) > 0 {
		gs.processBatch(batch, results)
		processed += len(batch)
	}

	fmt.Printf("  ✅ Worker %d завершил (%d записей)\n", id, processed)
}

// processBatch обрабатывает батч точек
func (gs *GeoService) processBatch(batch []FlightData, results chan<- UpdateOperation) {
	for _, job := range batch {
		var subjectName string = "Регион не определен"

		// Определяем координаты
		var lat, lon float64
		if job.Departure.Coordinates != nil {
			lat = job.Departure.Coordinates.Lat
			lon = job.Departure.Coordinates.Lon
		} else if job.SHRData.CoordinatesDep != nil {
			lat = job.SHRData.CoordinatesDep.Lat
			lon = job.SHRData.CoordinatesDep.Lon
		}

		// Ищем регион через 2dsphere индекс
		if lat != 0 || lon != 0 {
			if regionName, err := gs.FindRegionForPoint(lat, lon); err == nil {
				subjectName = regionName
			}
		}

		results <- UpdateOperation{
			ID:      job.ID,
			Subject: subjectName,
		}
	}
}

// resultCollectorOptimized оптимизированный сборщик
func (gs *GeoService) resultCollectorOptimized(
	collection *mongo.Collection,
	results <-chan UpdateOperation,
	startTime time.Time,
	batchSize int,
) error {
	var operations []mongo.WriteModel
	var processedCount int
	var lastLogTime = time.Now()

	for result := range results {
		processedCount++

		update := mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": result.ID}).
			SetUpdate(bson.M{"$set": bson.M{"region": result.Subject}})
		operations = append(operations, update)

		if len(operations) >= batchSize {
			if err := gs.executeBulkUpdateOptimized(collection, operations); err != nil {
				return fmt.Errorf("ошибка bulk update: %v", err)
			}
			operations = []mongo.WriteModel{}
		}

		if processedCount%5000 == 0 || time.Since(lastLogTime) > 2*time.Second {
			elapsed := time.Since(startTime)
			speed := float64(processedCount) / elapsed.Seconds()
			fmt.Printf("📊 Обработано: %d | Скорость: %.1f doc/сек\n", processedCount, speed)
			lastLogTime = time.Now()
		}
	}

	if len(operations) > 0 {
		if err := gs.executeBulkUpdateOptimized(collection, operations); err != nil {
			return fmt.Errorf("ошибка финального bulk update: %v", err)
		}
	}

	fmt.Printf("✅ Сборщик завершил работу (%d записей)\n", processedCount)
	return nil
}

// executeBulkUpdateOptimized оптимизированная bulk операция
func (gs *GeoService) executeBulkUpdateOptimized(collection *mongo.Collection, operations []mongo.WriteModel) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // Уменьшаем таймаут
	defer cancel()

	_, err := collection.BulkWrite(ctx, operations)
	if err != nil {
		return fmt.Errorf("ошибка bulk update (%d операций): %v", len(operations), err)
	}

	return nil
}
