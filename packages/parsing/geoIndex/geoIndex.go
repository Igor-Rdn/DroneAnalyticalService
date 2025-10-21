package geoIndex

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type GeoJSONGeometry struct {
	Type        string          `json:"type"`
	Coordinates json.RawMessage `json:"coordinates"`
}

type GeoJSONFeature struct {
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties"`
	Geometry   GeoJSONGeometry        `json:"geometry"`
}

type GeoJSONFile struct {
	Type     string           `json:"type"`
	Features []GeoJSONFeature `json:"features"`
	CRS      interface{}      `json:"crs,omitempty"`
}

// LoadRegionsToMongo загружает регионы из множества GeoJSON файлов в MongoDB
func LoadRegionsToMongo(regionsCollection *mongo.Collection) error {

	// Получаем путь к директории исполняемого файла
	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("ошибка получения пути исполняемого файла: %v", err)
	}

	// Строим путь к папке regions-new относительно исполняемого файла
	regionsDir := filepath.Join(filepath.Dir(exePath), "geojsonFiles")

	if regionsDir == "" {
		log.Println("не найден путь к файлам GEOJSON")
		return fmt.Errorf("не найден путь к файлам GEOJSON ")
	}

	ctx := context.Background()

	fmt.Printf("🗺️ Загружаем регионы из папки: %s\n", regionsDir)

	// Получаем список всех GeoJSON файлов в папке
	files, err := getGeoJSONFiles(regionsDir)
	if err != nil {
		return fmt.Errorf("ошибка чтения папки: %v", err)
	}

	fmt.Printf("📁 Найдено %d GeoJSON файлов\n", len(files))

	// Очищаем коллекцию
	_, err = regionsCollection.DeleteMany(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("ошибка очистки коллекции: %v", err)
	}

	var regionsToInsert []interface{}
	var totalLoadedCount int
	var totalErrorCount int

	// Обрабатываем каждый файл
	for _, file := range files {

		loadedCount, errorCount, err := processGeoJSONFile(file, &regionsToInsert)
		if err != nil {
			log.Printf("⚠️ Ошибка обработки файла %s: %v", file, err)
			totalErrorCount++
			continue
		}

		totalLoadedCount += loadedCount
		totalErrorCount += errorCount

	}

	fmt.Printf("\n📊 ИТОГИ:\n")
	fmt.Printf("   Успешно обработано: %d регионов\n", totalLoadedCount)
	fmt.Printf("   С ошибками: %d регионов\n", totalErrorCount)

	if totalLoadedCount == 0 {
		return fmt.Errorf("не удалось загрузить ни одного региона")
	}

	// Вставляем регионы
	fmt.Printf("\n💾 Вставляем %d регионов в MongoDB...\n", len(regionsToInsert))
	result, err := regionsCollection.InsertMany(ctx, regionsToInsert)
	if err != nil {
		return fmt.Errorf("ошибка вставки регионов: %v", err)
	}
	fmt.Printf("✅ В MongoDB загружено %d регионов\n", len(result.InsertedIDs))

	// СОЗДАЕМ 2DSPHERE ИНДЕКС!
	fmt.Println("🔧 Создаем 2dsphere индекс...")

	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "geometry", Value: "2dsphere"},
		},
	}

	// Создаем индекс с таймаутом
	ctxIndex, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	indexName, err := regionsCollection.Indexes().CreateOne(ctxIndex, indexModel)
	if err != nil {
		return fmt.Errorf("ошибка создания 2dsphere индекса: %v", err)
	}

	fmt.Printf("✅ 2dsphere индекс создан успешно: %s\n", indexName)

	// Проверяем что индекс создался
	cursor, err := regionsCollection.Indexes().List(ctx)
	if err == nil {
		var indexes []bson.M
		cursor.All(ctx, &indexes)
		fmt.Printf("🔍 Создано индексов в коллекции: %d\n", len(indexes))
		for _, index := range indexes {
			if key, ok := index["key"].(bson.M); ok {
				if _, hasGeo := key["geometry"]; hasGeo {
					fmt.Printf("✅ Найден 2dsphere индекс: %v\n", index["name"])
				}
			}
		}
	}

	return nil
}

// getGeoJSONFiles возвращает список всех GeoJSON файлов в папке
func getGeoJSONFiles(dirPath string) ([]string, error) {
	var files []string

	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Пропускаем директории
		if info.IsDir() {
			return nil
		}

		// Проверяем расширение файла
		ext := strings.ToLower(filepath.Ext(path))
		if ext == ".geojson" || ext == ".json" {
			files = append(files, path)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return files, nil
}

// processGeoJSONFile обрабатывает один GeoJSON файл
func processGeoJSONFile(filePath string, regionsToInsert *[]interface{}) (int, int, error) {
	// Читаем GeoJSON файл
	data, err := os.ReadFile(filePath)
	if err != nil {
		return 0, 0, fmt.Errorf("ошибка чтения файла: %v", err)
	}

	var geoJSONFile GeoJSONFile
	err = json.Unmarshal(data, &geoJSONFile)
	if err != nil {
		return 0, 0, fmt.Errorf("ошибка парсинга JSON: %v", err)
	}

	var loadedCount int
	var errorCount int

	// Обрабатываем каждую фичу в файле
	for i, feature := range geoJSONFile.Features {
		// Извлекаем название региона из поля "official_name:ru"
		regionName := extractOfficialName(feature.Properties)
		if regionName == "" {
			// Если нет официального названия, используем альтернативные
			regionName = extractRegionName(feature.Properties, i)
			if regionName == "" {
				log.Printf("⚠️ Не найдено название региона в файле %s", filepath.Base(filePath))
				errorCount++
				continue
			}
		}

		// Конвертируем геометрию в формат MongoDB
		mongoGeometry, err := convertGeoJSONGeometry(feature.Geometry)
		if err != nil {
			log.Printf("⚠️ Ошибка конвертации геометрии для региона %s: %v", regionName, err)
			errorCount++
			continue
		}

		if mongoGeometry == nil {
			log.Printf("⚠️ Пустая геометрия для региона %s", regionName)
			errorCount++
			continue
		}

		// Валидируем геометрию
		if err := validateGeometry(mongoGeometry); err != nil {
			log.Printf("⚠️ Невалидная геометрия для региона %s: %v", regionName, err)
			errorCount++
			continue
		}

		// Создаем документ региона
		regionDoc := bson.M{
			"name":     regionName,
			"geometry": mongoGeometry,
		}

		*regionsToInsert = append(*regionsToInsert, regionDoc)
		loadedCount++

	}

	return loadedCount, errorCount, nil
}

// extractOfficialName извлекает официальное название региона из поля "official_name:ru"
func extractOfficialName(properties map[string]interface{}) string {
	if properties == nil {
		return ""
	}

	// Пробуем поле "official_name:ru"
	if officialName, exists := properties["official_name:ru"]; exists {
		if name, ok := officialName.(string); ok && name != "" {
			return name
		}
	}

	// Пробуем поле "official_name" (без языка)
	if officialName, exists := properties["official_name"]; exists {
		if name, ok := officialName.(string); ok && name != "" {
			return name
		}
	}

	return ""
}

// extractRegionName извлекает название региона из properties (fallback)
func extractRegionName(properties map[string]interface{}, index int) string {
	if properties == nil {
		return fmt.Sprintf("Регион_%d", index+1)
	}

	// Пробуем поле "region" в properties
	if regionValue, exists := properties["region"]; exists {
		if regionName, ok := regionValue.(string); ok && regionName != "" {
			return regionName
		}
	}

	// Пробуем различные варианты названий
	possibleKeys := []string{
		"alt_name:ru", "alt_name",
		"name:ru", "name",
		"int_name", "NAME",
		"region_name", "subject",
	}

	for _, key := range possibleKeys {
		if value, exists := properties[key]; exists {
			if name, ok := value.(string); ok && name != "" {
				return name
			}
		}
	}

	return fmt.Sprintf("Регион_%d", index+1)
}

// convertGeoJSONGeometry конвертирует геометрию из GeoJSON в формат MongoDB
func convertGeoJSONGeometry(geometry GeoJSONGeometry) (bson.M, error) {
	result := bson.M{
		"type": geometry.Type,
	}

	switch geometry.Type {
	case "Point":
		var coords []float64
		if err := json.Unmarshal(geometry.Coordinates, &coords); err != nil {
			return nil, fmt.Errorf("ошибка парсинга Point координат: %v", err)
		}
		result["coordinates"] = coords

	case "LineString":
		var coords [][]float64
		if err := json.Unmarshal(geometry.Coordinates, &coords); err != nil {
			return nil, fmt.Errorf("ошибка парсинга LineString координат: %v", err)
		}
		result["coordinates"] = coords

	case "Polygon":
		var coords [][][]float64
		if err := json.Unmarshal(geometry.Coordinates, &coords); err != nil {
			return nil, fmt.Errorf("ошибка парсинга Polygon координат: %v", err)
		}
		result["coordinates"] = coords

	case "MultiPolygon":
		var coords [][][][]float64
		if err := json.Unmarshal(geometry.Coordinates, &coords); err != nil {
			return nil, fmt.Errorf("ошибка парсинга MultiPolygon координат: %v", err)
		}
		result["coordinates"] = coords

	default:
		return nil, fmt.Errorf("неподдерживаемый тип геометрии: %s", geometry.Type)
	}

	return result, nil
}

// validateGeometry проверяет геометрию перед вставкой в MongoDB
func validateGeometry(geometry bson.M) error {
	if geometry == nil {
		return fmt.Errorf("геометрия не может быть nil")
	}

	geoType, ok := geometry["type"].(string)
	if !ok {
		return fmt.Errorf("отсутствует тип геометрии")
	}

	coords, ok := geometry["coordinates"]
	if !ok {
		return fmt.Errorf("отсутствуют координаты")
	}

	switch geoType {
	case "Point":
		if points, ok := coords.([]float64); !ok || len(points) != 2 {
			return fmt.Errorf("неверные координаты для Point")
		}
	case "LineString":
		if points, ok := coords.([][]float64); !ok || len(points) < 2 {
			return fmt.Errorf("неверные координаты для LineString")
		}
	case "Polygon":
		if rings, ok := coords.([][][]float64); !ok || len(rings) == 0 {
			return fmt.Errorf("неверные координаты для Polygon")
		}
		if rings, ok := coords.([][][]float64); ok && len(rings) > 0 {
			outerRing := rings[0]
			if len(outerRing) < 4 {
				return fmt.Errorf("полигон должен иметь минимум 4 точки")
			}
			first := outerRing[0]
			last := outerRing[len(outerRing)-1]
			if first[0] != last[0] || first[1] != last[1] {
				return fmt.Errorf("полигон не замкнут")
			}
		}
	case "MultiPolygon":
		if polygons, ok := coords.([][][][]float64); !ok || len(polygons) == 0 {
			return fmt.Errorf("неверные координаты для MultiPolygon")
		}
	default:
		return fmt.Errorf("неподдерживаемый тип геометрии: %s", geoType)
	}

	return nil
}
