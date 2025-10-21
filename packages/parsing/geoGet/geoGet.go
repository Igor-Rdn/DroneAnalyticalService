package geoGet

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// RegionGeoResponse структура для ответа API
type RegionGeoResponse struct {
	Region  string                 `json:"region"`
	GeoJSON GeoJSONFeatureResponse `json:"geojson"`
}

// GeoJSONFeatureResponse структура GeoJSON Feature для ответа
type GeoJSONFeatureResponse struct {
	Type       string                 `json:"type"`
	Geometry   GeometryResponse       `json:"geometry"`
	Properties map[string]interface{} `json:"properties"`
}

// GeometryResponse структура геометрии для ответа
type GeometryResponse struct {
	Type        string      `json:"type"`
	Coordinates interface{} `json:"coordinates"` // interface{} чтобы принимать любые координаты
}

// GetRegionsGeo возвращает все регионы с их геоданными в формате GeoJSON
func GetRegionsGeo(collection *mongo.Collection) ([]RegionGeoResponse, error) {
	ctx := context.Background()

	// Используем простой поиск и ручное преобразование
	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("ошибка поиска регионов: %v", err)
	}
	defer cursor.Close(ctx)

	var rawDocs []bson.M
	if err := cursor.All(ctx, &rawDocs); err != nil {
		return nil, fmt.Errorf("ошибка декодирования документов: %v", err)
	}

	var results []RegionGeoResponse
	for _, doc := range rawDocs {
		regionName, ok := doc["name"].(string)
		if !ok {
			continue
		}

		geometry, ok := doc["geometry"].(bson.M)
		if !ok {
			continue
		}

		geoType, _ := geometry["type"].(string)
		coordinates := geometry["coordinates"]

		if regionName == "Чукотский автономный округ" {
			coordinates = normalizeGeometry(coordinates)
		}

		response := RegionGeoResponse{
			Region: regionName,
			GeoJSON: GeoJSONFeatureResponse{
				Type: "Feature",
				Geometry: GeometryResponse{
					Type:        geoType,
					Coordinates: coordinates, // Просто используем координаты как есть
				},
				Properties: map[string]interface{}{},
			},
		}

		results = append(results, response)

	}

	return results, nil
}

// Применяет нормализацию координат для Чукотки
func normalizeGeometry(coordinates any) any {

	fmt.Printf("🔧 Начинаем нормализацию Чукотки. Тип coordinates: %T\n", coordinates)

	return processBSONArray(coordinates.(bson.A))
}

// processBSONArray обрабатывает bson.A (массив из MongoDB)
func processBSONArray(data bson.A) [][][][]float64 {
	var result [][][][]float64

	for _, polyItem := range data {
		if polyArray, ok := polyItem.(bson.A); ok {
			var polygon [][][]float64

			for _, ringItem := range polyArray {
				if ringArray, ok := ringItem.(bson.A); ok {
					var ring [][]float64

					for _, pointItem := range ringArray {
						if pointArray, ok := pointItem.(bson.A); ok {
							var coord []float64

							for _, numItem := range pointArray {
								switch num := numItem.(type) {
								case float64:
									coord = append(coord, num)
								case int32:
									coord = append(coord, float64(num))
								case int64:
									coord = append(coord, float64(num))
								default:
									fmt.Printf("⚠️ Неподдерживаемый тип числа: %T\n", num)
									coord = append(coord, 0)
								}
							}

							// Применяем коррекцию для Чукотки
							if len(coord) >= 2 {
								lng := coord[0]
								if lng > -170 {
									coord[0] = lng - 0.0000005 // Сдвигаем долготу
								}
							}

							ring = append(ring, coord)
						}
					}

					polygon = append(polygon, ring)
				}
			}

			result = append(result, polygon)
		}
	}

	return result
}
