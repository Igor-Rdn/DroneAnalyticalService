package parsing

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	coorinates "project/packages/parsing/coordinates"
	"project/packages/parsing/datetime"
)

type FlightData struct {
	// Метаданные
	ID           string `bson:"_id,omitempty" json:"id"`
	FlightDataID int    `bson:"flightDataID" json:"flightDataID"`
	// Основная информация (столбец A)
	Region string `bson:"region" json:"region"`
	// Парсинг из столбца B (SHR - основные данные)
	SHRData SHRData `bson:"shr" json:"shr"`
	// Парсинг из столбца C (DEP - вылет)
	Departure DepartureData `bson:"dep" json:"dep"`
	// Парсинг из столбца D (ARR - прибытие)
	Arrival ArrivalData `bson:"arr" json:"arr"`
	// Извлеченные ключевые поля для быстрого поиска
	SearchFields SearchField `bson:"searchFields" json:"searchFields"`
}

// SHRData - данные из столбца B (основная информация о полете)
type SHRData struct {
	RawText          string                 `bson:"rawText" json:"rawText"`
	SID              *int                   `bson:"sid" json:"sid"`
	AircraftIndex    string                 `bson:"aircraftIndex" json:"aircraftIndex"`
	AircraftType     string                 `bson:"aircraftType" json:"aircraftType"`
	AircraftQuantity string                 `bson:"aircraftQuantity" json:"aircraftQuantity"`
	CoordinatesDep   *coorinates.Coordinate `bson:"coordinatesDep,omitempty" json:"coordinatesDep"`
	CoordinatesArr   *coorinates.Coordinate `bson:"coordinatesArr,omitempty" json:"coordinatesArr"`
	DateTime         *time.Time             `bson:"dateTime" json:"dateTime"`
	Operator         string                 `bson:"operator" json:"operator"`
	Remarks          string                 `bson:"remarks" json:"remarks"`
}

// DepartureData - данные о вылете из столбца C
type DepartureData struct {
	RawText     string                 `bson:"rawText" json:"rawText"`
	SID         *int                   `bson:"sid" json:"sid"`
	DateTime    *time.Time             `bson:"dateTime" json:"dateTime"`
	Airport     string                 `bson:"airport" json:"airport"`
	Coordinates *coorinates.Coordinate `bson:"coordinates,omitempty" json:"coordinates"`
	//Time        string                 `bson:"time" json:"time"`
}

// ArrivalData - данные о прибытии из столбца D
type ArrivalData struct {
	RawText     string                 `bson:"rawText" json:"rawText"`
	SID         *int                   `bson:"sid" json:"sid"`
	DateTime    *time.Time             `bson:"dateTime" json:"dateTime"`
	Airport     string                 `bson:"airport" json:"airport"`
	Coordinates *coorinates.Coordinate `bson:"coordinates,omitempty" json:"coordinates"`
	//Time        string                 `bson:"time" json:"time"`
}

// KeyFields - ключевые поля для индексации и поиска
type SearchField struct {
	SID         *int                   `bson:"sid" json:"sid"`
	Region      string                 `bson:"region" json:"region"`
	DateTime    *time.Time             `bson:"dateTime" json:"dateTime"`
	Operator    string                 `bson:"operator" json:"operator"`
	Coordinates *coorinates.Coordinate `bson:"coordinates,omitempty" json:"coordinates"`
}

// Безопасное получение элемента из массива
func safeGet(arr []string, index int) string {
	if index < len(arr) {
		return arr[index]
	}
	return ""
}

func parseInt(s string) *int {
	if s == "" {
		return nil
	}
	if val, err := strconv.Atoi(s); err == nil {
		return &val
	}
	return nil
}

// Создание структуры FlightData из строки Excel
func CreateFlightData(rowNum int, row []string) FlightData {
	region := safeGet(row, 0)
	shrRaw := safeGet(row, 1)
	depRaw := safeGet(row, 2)
	arrRaw := safeGet(row, 3)

	// Парсим данные
	shrData := parseSHRData(shrRaw)
	depData := parseDepartureData(depRaw)
	arrData := parseArrivalData(arrRaw)

	// Создаем ключевые поля для индексации
	searchField := SearchField{
		Region:   region,
		DateTime: shrData.DateTime,
		Operator: shrData.Operator,
		SID:      shrData.SID,
	}

	return FlightData{
		FlightDataID: rowNum,
		Region:       region,
		SHRData:      shrData,
		Departure:    depData,
		Arrival:      arrData,
		SearchFields: searchField,
	}
}

func extractData(expression string, rawText string) string {

	if matches := regexp.MustCompile(expression).FindStringSubmatch(rawText); len(matches) > 1 {
		return matches[1]
	}
	return ""
}

// Парсинг SHR данных из столбца B
func parseSHRData(rawText string) SHRData {
	shr := SHRData{
		RawText: rawText,
	}

	shr.AircraftIndex = extractData(`SHR-([A-Z0-9]+)`, rawText)
	shr.AircraftType = extractData(`TYP/\d*([A-Z]+)`, rawText)
	shr.AircraftQuantity = extractData(`TYP/([0-9]+)`, rawText)
	shr.CoordinatesDep, _ = coorinates.ParseAviationCoordinate(extractData(`DEP/([0-9]+[NS][0-9]+[EW])`, rawText))
	shr.CoordinatesArr, _ = coorinates.ParseAviationCoordinate(extractData(`DEST/([0-9]+[NS][0-9]+[EW])`, rawText))
	shr.DateTime = datetime.ParseDate(extractData(`DOF/([0-9]+)`, rawText), "")
	shr.SID = parseInt(extractData(`SID/([0-9]+)`, rawText))

	// Парсинг OPR
	if idx := strings.Index(rawText, "OPR/"); idx != -1 {
		shr.Operator = "todo operator..."
	}

	// Парсинг RMK
	if idx := strings.Index(rawText, "RMK/"); idx != -1 {
		shr.Remarks = "todo remarks..."
	}

	return shr
}

// Парсинг данных о вылете из столбца C
func parseDepartureData(rawText string) DepartureData {
	dep := DepartureData{
		RawText: rawText,
	}

	dep.SID = parseInt(extractData(`-SID ([0-9]+)`, rawText))
	dep.DateTime = datetime.ParseDate(extractData(`-ADD ([0-9]+)`, rawText), extractData(`-ATD ([0-9]+)`, rawText))
	//dep.Time = extractData(`-ATD ([0-9]+)`, rawText)
	dep.Airport = extractData(`-ADEP ([A-Z0-9]+)`, rawText)
	dep.Coordinates, _ = coorinates.ParseAviationCoordinate(extractData(`-ADEPZ ([0-9]+[NS][0-9]+[EW])`, rawText))

	return dep
}

// Парсинг данных о прибытии из столбца D
func parseArrivalData(rawText string) ArrivalData {
	arr := ArrivalData{
		RawText: rawText,
	}

	arr.SID = parseInt(extractData(`-SID ([0-9]+)`, rawText))
	arr.DateTime = datetime.ParseDate(extractData(`-ADA ([0-9]+)`, rawText), extractData(`-ATA ([0-9]+)`, rawText))
	//arr.Time = extractData(`-ATA ([0-9]+)`, rawText)
	arr.Airport = extractData(`-ADARR ([A-Z0-9]+)`, rawText)
	arr.Coordinates, _ = coorinates.ParseAviationCoordinate(extractData(`-ADARRZ ([0-9]+[NS][0-9]+[EW])`, rawText))

	return arr
}
