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
	AircraftQuantity int                    `bson:"aircraftQuantity" json:"aircraftQuantity"`
	CoordinatesDep   *coorinates.Coordinate `bson:"coordinatesDep,omitempty" json:"coordinatesDep"`
	CoordinatesArr   *coorinates.Coordinate `bson:"coordinatesArr,omitempty" json:"coordinatesArr"`
	DateTime         *time.Time             `bson:"dateTime" json:"dateTime"`
	FlightDuration   *float64               `bson:"flightDuration" json:"flightDuration"`
	TimeDep          string                 `bson:"timeDep" json:"timeDep"`
	TimeArr          string                 `bson:"timeArr" json:"timeArr"`
	Date             string                 `bson:"date" json:"date"`
	Operator         string                 `bson:"operator" json:"operator"`
	Remarks          string                 `bson:"remarks" json:"remarks"`
}

// DepartureData - данные о вылете из столбца C
type DepartureData struct {
	RawText     string                 `bson:"rawText" json:"rawText"`
	DateTime    *time.Time             `bson:"dateTime" json:"dateTime"`
	Airport     string                 `bson:"airport" json:"airport"`
	Coordinates *coorinates.Coordinate `bson:"coordinates,omitempty" json:"coordinates"`
	//Time        string                 `bson:"time" json:"time"`
}

// ArrivalData - данные о прибытии из столбца D
type ArrivalData struct {
	RawText     string                 `bson:"rawText" json:"rawText"`
	DateTime    *time.Time             `bson:"dateTime" json:"dateTime"`
	Airport     string                 `bson:"airport" json:"airport"`
	Coordinates *coorinates.Coordinate `bson:"coordinates,omitempty" json:"coordinates"`
	//Time        string                 `bson:"time" json:"time"`
}

// KeyFields - ключевые поля для индексации и поиска
type SearchField struct {
	SID      *int       `bson:"sid" json:"sid"`
	Region   string     `bson:"region" json:"region"`
	DateTime *time.Time `bson:"dateTime" json:"dateTime"`
	//DateTimeDep *time.Time `bson:"DateTimeDep" json:"DateTimeDep"`
	//DateTimeArr *time.Time `bson:"DateTimeArr" json:"DateTimeArr"`
}

// Безопасное получение элемента из массива(если в какой то строке exel не заполнен столбец)
func safeGet(arr []string, index int) string {
	if index < len(arr) {
		return arr[index]
	}
	return ""
}

// Для превращения строки в int с поддержкой nil
func parseInt(s string) *int {
	if s == "" {
		return nil
	}
	if val, err := strconv.Atoi(s); err == nil {
		return &val
	}
	return nil
}

// Для превращения строки в int с поддержкой nil
func parseFloat(f float64) *float64 {
	val := f
	return &val
}

// Coalesce возвращает первое ненулевое значение
func Coalesce[T any](values ...*T) *T {
	for _, v := range values {
		if v != nil {
			return v
		}
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
		SID:      shrData.SID,
		Region:   region,
		DateTime: Coalesce(depData.DateTime, datetime.ParseDate(shrData.Date, shrData.TimeDep), datetime.ParseDate(shrData.Date, shrData.TimeArr)),
	}

	//Считаем длительность полета
	depDatetime := Coalesce(depData.DateTime, datetime.ParseDate(shrData.Date, shrData.TimeDep))
	arrDatetime := Coalesce(arrData.DateTime, datetime.ParseDate(shrData.Date, shrData.TimeArr))
	duration := arrDatetime.Sub(*depDatetime)
	shrData.FlightDuration = parseFloat(duration.Minutes())
	if *shrData.FlightDuration < 0 {
		shrData.FlightDuration = nil
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

func parseField(fieldNumber int, rawText string) (res string) {

	lines := strings.Split(rawText, "\n")
	dashCount := 0

	for _, line := range lines {
		line = strings.TrimSpace(line)
		// Ищем строки, начинающиеся с прочерка
		if strings.HasPrefix(line, "-") {
			dashCount++

			// Если это нужный нам по порядку тег
			if dashCount == fieldNumber {
				res = extractData(`^-\w{4}(\d{4})`, line)
				break
			}
		}
	}
	return res
}

// Парсинг SHR данных из столбца B
func parseSHRData(rawText string) SHRData {
	shr := SHRData{
		RawText: rawText,
	}

	shr.AircraftIndex = extractData(`SHR-([A-Z0-9]+)`, rawText)
	shr.AircraftType = extractData(`TYP/\d*([A-Z]+)`, rawText)
	shr.AircraftQuantity = 1
	if res := extractData(`TYP/([0-9]+)`, rawText); res != "" {
		shr.AircraftQuantity, _ = strconv.Atoi(res)
	}
	shr.CoordinatesDep, _ = coorinates.ParseAviationCoordinate(extractData(`DEP/([0-9]+[NS][0-9]+[EW])`, rawText))
	shr.CoordinatesArr, _ = coorinates.ParseAviationCoordinate(extractData(`DEST/([0-9]+[NS][0-9]+[EW])`, rawText))
	shr.DateTime = datetime.ParseDate(extractData(`DOF/([0-9]+)`, rawText), "")
	shr.SID = parseInt(extractData(`SID/([0-9]+)`, rawText))

	shr.TimeDep = parseField(1, rawText)
	shr.TimeArr = parseField(3, rawText)
	shr.Date = extractData(`DOF/([0-9]+)`, rawText)

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

	arr.DateTime = datetime.ParseDate(extractData(`-ADA ([0-9]+)`, rawText), extractData(`-ATA ([0-9]+)`, rawText))
	//arr.Time = extractData(`-ATA ([0-9]+)`, rawText)
	arr.Airport = extractData(`-ADARR ([A-Z0-9]+)`, rawText)
	arr.Coordinates, _ = coorinates.ParseAviationCoordinate(extractData(`-ADARRZ ([0-9]+[NS][0-9]+[EW])`, rawText))

	return arr
}
