package parsing

import (
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	coorinates "project/packages/parsing/coordinates"
	"project/packages/parsing/datetime"
)

// Предкомпилированные регулярки для часто используемых паттернов
var (
	shrAircraftRegex = regexp.MustCompile(`SHR-([A-Z0-9]+)`)
	typCountRegex    = regexp.MustCompile(`TYP/([0-9]+)`)
	typTypeRegex     = regexp.MustCompile(`TYP/\d*([A-Z]+)`)
	depCoordRegex    = regexp.MustCompile(`DEP/([0-9]+[NS][0-9]+[EW])`)
	destCoordRegex   = regexp.MustCompile(`DEST/([0-9]+[NS][0-9]+[EW])`)
	dofRegex         = regexp.MustCompile(`DOF/([0-9]+)`)
	sidRegex         = regexp.MustCompile(`SID/([0-9]+)`)
	fieldLineRegex   = regexp.MustCompile(`^-\w{4}(\d{4})`)
	addRegex         = regexp.MustCompile(`-ADD ([0-9]+)`)
	atdRegex         = regexp.MustCompile(`-ATD ([0-9]+)`)
	//adepRegex        = regexp.MustCompile(`-ADEP ([A-Z0-9]+)`)
	adepzRegex = regexp.MustCompile(`-ADEPZ ([0-9]+[NS][0-9]+[EW])`)
	adaRegex   = regexp.MustCompile(`-ADA ([0-9]+)`)
	ataRegex   = regexp.MustCompile(`-ATA ([0-9]+)`)
	//adarrRegex       = regexp.MustCompile(`-ADARR ([A-Z0-9]+)`)
	adarrzRegex = regexp.MustCompile(`-ADARRZ ([0-9]+[NS][0-9]+[EW])`)
)

type FlightData struct {
	ID           string        `bson:"_id,omitempty" json:"id"`
	SHRData      SHRData       `bson:"shr" json:"shr"`
	Departure    DepartureData `bson:"dep" json:"dep"`
	Arrival      ArrivalData   `bson:"arr" json:"arr"`
	SearchFields SearchField   `bson:"searchFields" json:"searchFields"`
	Region       string        `bson:"region,omitempty" json:"region"`
}

// Остальные структуры остаются без изменений...
type SHRData struct {
	RawText          string                 `bson:"rawText" json:"rawText"`
	SID              int                    `bson:"sid" json:"sid"`
	AircraftIndex    *string                `bson:"aircraftIndex" json:"aircraftIndex"`
	AircraftType     *string                `bson:"aircraftType" json:"aircraftType"`
	AircraftQuantity int                    `bson:"aircraftQuantity" json:"aircraftQuantity"`
	CoordinatesDep   *coorinates.Coordinate `bson:"coordinatesDep,omitempty" json:"coordinatesDep"`
	CoordinatesArr   *coorinates.Coordinate `bson:"coordinatesArr,omitempty" json:"coordinatesArr"`
	DateTime         *time.Time             `bson:"dateTime" json:"dateTime"`
	FlightDuration   *float64               `bson:"flightDuration" json:"flightDuration"`
	TimeDep          string                 `bson:"timeDep" json:"timeDep"`
	TimeArr          string                 `bson:"timeArr" json:"timeArr"`
	Date             string                 `bson:"date" json:"date"`
	Operator         string                 `bson:"operator" json:"operator"`
	OperatorType     string                 `bson:"operatorType" json:"operatorType"`
	//Remarks          string                 `bson:"remarks" json:"remarks"`
}

type DepartureData struct {
	RawText     string                 `bson:"rawText" json:"rawText"`
	DateTime    *time.Time             `bson:"dateTime" json:"dateTime"`
	Coordinates *coorinates.Coordinate `bson:"coordinates,omitempty" json:"coordinates"`
}

type ArrivalData struct {
	RawText     string                 `bson:"rawText" json:"rawText"`
	DateTime    *time.Time             `bson:"dateTime" json:"dateTime"`
	Coordinates *coorinates.Coordinate `bson:"coordinates,omitempty" json:"coordinates"`
}

type SearchField struct {
	DateTime    *time.Time `bson:"dateTime" json:"dateTime"`
	ArrDatetime *time.Time `bson:"arrDatetime" json:"arrDatetime"`
}

// Оптимизированная safeGet - избегаем проверки границ если знаем структуру данных
func safeGet(arr []string, index int) string {
	if index < len(arr) {
		return arr[index]
	}
	return ""
}

// Оптимизированный Coalesce - избегаем создания слайса
func Coalesce[T any](values ...*T) *T {
	for i := 0; i < len(values); i++ {
		if values[i] != nil {
			return values[i]
		}
	}
	return nil
}

// Добавляем метод для установки региона
func (fd *FlightData) SetRegion(region string) {
	fd.Region = region
}

// Создание структуры FlightData из строки Excel
func CreateFlightData(row []string) FlightData {
	// Быстрое извлечение без лишних аллокаций
	shrRaw := safeGet(row, 1)
	depRaw := safeGet(row, 2)
	arrRaw := safeGet(row, 3)

	// Парсим данные
	shrData := parseSHRData(shrRaw)
	depData := parseDepartureData(depRaw)
	arrData := parseArrivalData(arrRaw)

	// Оптимизированное создание SearchField
	searchField := createSearchField(&shrData, &depData, &arrData)

	// Расчет длительности полета
	if searchField.DateTime != nil && searchField.ArrDatetime != nil {
		duration := searchField.ArrDatetime.Sub(*searchField.DateTime).Minutes()
		if duration > 0 {
			shrData.FlightDuration = &duration
		}
	}

	return FlightData{
		SHRData:      shrData,
		Departure:    depData,
		Arrival:      arrData,
		SearchFields: searchField,
	}
}

// Оптимизированная extractData с предкомпилированными regexp
func extractData(regex *regexp.Regexp, rawText string) string {
	if matches := regex.FindStringSubmatch(rawText); len(matches) > 1 {
		// Возвращаем первую непустую группу
		for i := 1; i < len(matches); i++ {
			if matches[i] != "" {
				return matches[i]
			}
		}
	}
	return ""
}

// Оптимизированный parseField - избегаем лишних аллокаций
func parseField(fieldNumber int, rawText string) string {
	dashCount := 0
	start := 0

	for {
		// Ищем начало следующей строки
		lineEnd := strings.IndexByte(rawText[start:], '\n')
		var line string
		if lineEnd == -1 {
			line = rawText[start:]
		} else {
			line = rawText[start : start+lineEnd]
			start += lineEnd + 1
		}

		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "-") {
			dashCount++
			if dashCount == fieldNumber {
				if matches := fieldLineRegex.FindStringSubmatch(line); len(matches) > 1 {
					return matches[1]
				}
				break
			}
		}

		if lineEnd == -1 {
			break
		}
	}
	return ""
}

// Оптимизированный парсинг SHR данных
func parseSHRData(rawText string) SHRData {
	shr := SHRData{RawText: rawText}

	// AircraftIndex
	if aIndex := extractData(shrAircraftRegex, rawText); aIndex != "" {
		switch aIndex {
		case "ZZZZZ", "ZZZZ", "ZZZ", "ZZ", "Z":
			// nil
		default:
			shr.AircraftIndex = &aIndex
		}
	}

	// AircraftType
	if aircraftType := extractData(typTypeRegex, rawText); aircraftType != "" {
		shr.AircraftType = &aircraftType
	}

	// AircraftQuantity
	shr.AircraftQuantity = 1
	if countStr := extractData(typCountRegex, rawText); countStr != "" {
		if count, err := strconv.Atoi(countStr); err == nil {
			shr.AircraftQuantity = count
		}
	}

	// Coordinates
	shr.CoordinatesDep, _ = coorinates.ParseAviationCoordinate(extractData(depCoordRegex, rawText))
	shr.CoordinatesArr, _ = coorinates.ParseAviationCoordinate(extractData(destCoordRegex, rawText))

	// DateTime и SID
	shr.DateTime = datetime.ParseDate(extractData(dofRegex, rawText), "")
	sid, _ := strconv.Atoi(extractData(sidRegex, rawText))
	shr.SID = sid

	// Время и дата
	shr.TimeDep = parseField(1, rawText)
	shr.TimeArr = parseField(3, rawText)
	shr.Date = extractData(dofRegex, rawText)

	operatorResult := ExtractAndClassifyOperator(rawText)
	shr.Operator = operatorResult.Operator
	shr.OperatorType = operatorResult.OperatorType

	return shr
}

// Оптимизированный createSearchField
func createSearchField(shr *SHRData, dep *DepartureData, arr *ArrivalData) SearchField {
	date := shr.Date
	timeDep := shr.TimeDep
	timeArr := shr.TimeArr

	depDatetime := Coalesce(
		dep.DateTime,
		datetime.ParseDate(date, timeDep),
	)

	arrDatetime := Coalesce(
		arr.DateTime,
		datetime.ParseDate(date, timeArr),
	)

	// Проверка корректности времени прибытия
	if arrDatetime != nil && depDatetime != nil && arrDatetime.Before(*depDatetime) {
		arrDatetime = nil
	}

	return SearchField{
		DateTime:    Coalesce(depDatetime, arrDatetime),
		ArrDatetime: arrDatetime,
	}
}

// Упрощенные функции парсинга Departure и Arrival
func parseDepartureData(rawText string) DepartureData {
	dep := DepartureData{RawText: rawText}

	dep.DateTime = datetime.ParseDate(extractData(addRegex, rawText), extractData(atdRegex, rawText))

	//dep.Airport = extractData(adepRegex, rawText)
	dep.Coordinates, _ = coorinates.ParseAviationCoordinate(extractData(adepzRegex, rawText))

	return dep
}

func parseArrivalData(rawText string) ArrivalData {
	arr := ArrivalData{RawText: rawText}

	arr.DateTime = datetime.ParseDate(extractData(adaRegex, rawText), extractData(ataRegex, rawText))

	//arr.Airport = extractData(adarrRegex, rawText)
	arr.Coordinates, _ = coorinates.ParseAviationCoordinate(extractData(adarrzRegex, rawText))

	return arr
}

// GeoIntegratedParser объединяет парсинг и геопоиск
type GeoIntegratedParser struct {
	geoService GeoServiceInterface
}

// GeoServiceInterface интерфейс для геосервиса
type GeoServiceInterface interface {
	FindRegionForPoint(lat, lon float64) (string, error)
}

// NewGeoIntegratedParser создает новый парсер с интеграцией геопоиска
func NewGeoIntegratedParser(geoService GeoServiceInterface) *GeoIntegratedParser {
	return &GeoIntegratedParser{
		geoService: geoService,
	}
}

// CreateFlightDataWithRegion создает FlightData с автоматическим определением региона
func (p *GeoIntegratedParser) CreateFlightDataWithRegion(row []string) FlightData {
	flightData := CreateFlightData(row)

	// Определяем координаты для поиска региона
	var lat, lon float64

	// Приоритет: координаты вылета из Departure
	if flightData.Departure.Coordinates != nil {
		lat = flightData.Departure.Coordinates.Lat
		lon = flightData.Departure.Coordinates.Lon
	} else if flightData.SHRData.CoordinatesDep != nil {
		lat = flightData.SHRData.CoordinatesDep.Lat
		lon = flightData.SHRData.CoordinatesDep.Lon
	}

	// Если есть координаты - ищем регион
	if lat != 0 || lon != 0 {
		if region, err := p.geoService.FindRegionForPoint(lat, lon); err == nil {
			flightData.SetRegion(region)
		}
	} else {
		flightData.SetRegion("Регион не определен")
	}

	return flightData
}

// ProcessBatchWithRegion пакетная обработка с определением регионов
func (p *GeoIntegratedParser) ProcessBatchWithRegion(rows [][]string) []FlightData {
	var results []FlightData
	var wg sync.WaitGroup
	var mu sync.Mutex

	batchSize := 50
	semaphore := make(chan struct{}, 16) // Ограничиваем параллелизм

	for i := 0; i < len(rows); i += batchSize {
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}

		batch := rows[i:end]
		wg.Add(1)

		go func(batch [][]string) {
			defer wg.Done()
			semaphore <- struct{}{}        // Занимаем слот
			defer func() { <-semaphore }() // Освобождаем слот

			var batchResults []FlightData
			for _, row := range batch {
				flightData := p.CreateFlightDataWithRegion(row)
				batchResults = append(batchResults, flightData)
			}

			mu.Lock()
			results = append(results, batchResults...)
			mu.Unlock()
		}(batch)
	}

	wg.Wait()
	return results
}

// OperatorParser сервис для парсинга операторов
type OperatorParser struct{}

// NewOperatorParser создает новый парсер операторов
func NewOperatorParser() *OperatorParser {
	return &OperatorParser{}
}

// OperatorResult результат парсинга оператора
type OperatorResult struct {
	Operator     string
	OperatorType string
}

// Константы и словари
var (
	// Ключи, ограничивающие блок OPR/...
	nextKeys = []string{
		"REG", "TYP", "RMK", "DOF", "EET", "SID", "DEP", "DEST", "NAV", "CODE",
		"PBN", "COM", "DAT", "SUR", "PER", "ORGN", "EOBT", "SEL", "RVR", "ALTN",
		"ALT", "RALT", "TALT", "STS", "OPR",
	}

	// Частые ведомства (кириллица) → считаем ЮЛ
	agencyWordsCyr = []string{
		"АДМИНИСТРАЦИЯ", "ДЕПАРТАМЕНТ", "МИНИСТЕРСТВО", "УПРАВЛЕНИЕ", "ГЛАВНОЕ УПРАВЛЕНИЕ",
		"ГУ", "УМВД", "ГУВД", "МЧС", "МВД", "ФСИН", "ФСБ", "РОСГВАРДИЯ", "РОСАВИАЦИЯ", "РОСТРАНСНАДЗОР",
		"ЦУКС", "ПРАВИТЕЛЬСТВО", "ГОСУДАРСТВЕННОЕ", "РЕСПУБЛИКИ", "ОБЛАСТИ", "ГОРОДА",
	}

	// Ведомства (латиница/транслит/leet)
	agencyWordsLat = []string{
		"ROSGVARDI", "UPRAVLENI", "MINISTERSTV", "DEPARTAMENT", "ADMINISTRAT",
		"MCHS", "M4S", "M4C", "MVD", "MWD", "FSB", "FSIN", "GUVD", "UMVD",
	}

	// ОПФ/корп. формы (кириллица/латиница/«зеркала»)
	legalTokens = []string{
		"ООО", "OOO", "АО", "AO", "ПАО", "ЗАО", "3AO", "ОАО", "OAO", "ФГУП", "ГУП", "МУП", "ЧУП", "СПАО", "НАО", "HAO",
		"АНО", "AHO", "НКО", "HKO", "ФОНД", "СРО", "CPO", "ТОО", "TOO", "ПК", "СКО", "CKO", "АОЗТ", "AO3T",
		"LLC", "LTD", "INC", "JSC", "PJSC", "GMBH", "AG", "PLC",
	}

	// Слова, часто встречающиеся у организаций на латинице
	legalHintsLat = []string{
		"COMPANY", "CO", "CORP", "CORPORATION", "GROUP", "HOLDING", "MEDIA", "STUDIO", "PRODUCTION", "SERVICES",
		"AERO", "AIR", "AVIATION", "UAV", "DRONE", "TECH", "TECHNOLOG", "LAB", "CENTER", "CENTRE",
		"UNIVERSIT", "INSTITUT", "INSTITUTE", "ACADEM", "AKADEMI", "AGENCY",
	}

	// Список суффиксов фамилий (кириллица)
	surnameSuffixesCyr = []string{
		"ОВ", "ЕВ", "ЁВ", "ИН", "ЫН", "ИЙ", "ЫЙ", "АЯ", "ЕНКО", "ЕНЬКО", "УК", "ЮК",
		"СКИЙ", "ЦКИЙ", "КИН", "ЧУК", "ЕЦ", "АН", "ЯН", "ЯНЦ", "АДЗЕ", "ШВИЛИ", "ИДЗЕ",
		"ИЧ", "ОВА", "ЕВА", "ИНА", "ЫНА", "СКАЯ", "ЦКАЯ",
	}

	// Список суффиксов фамилий (латиница/транслит)
	surnameSuffixesLat = []string{
		"OV", "EV", "YEV", "IN", "YN", "IY", "YY", "AYA",
		"ENKO", "UK", "YUK", "SKIY", "SKY", "TSKIY", "CKIY", "KIN", "CHUK", "ETS",
		"AN", "YAN", "IADZE", "ADZE", "SHVILI", "IDZE", "ICH", "OVA", "EVA", "INA", "YNA", "SKAYA", "CKAYA",
	}

	// Кириллица → латиница (верхний регистр)
	cyrToLatMap = map[rune]string{
		'А': "A", 'Б': "B", 'В': "V", 'Г': "G", 'Д': "D", 'Е': "E", 'Ё': "YO",
		'Ж': "ZH", 'З': "Z", 'И': "I", 'Й': "Y", 'К': "K", 'Л': "L", 'М': "M",
		'Н': "N", 'О': "O", 'П': "P", 'Р': "R", 'С': "S", 'Т': "T", 'У': "U",
		'Ф': "F", 'Х': "KH", 'Ц': "TS", 'Ч': "CH", 'Ш': "SH", 'Щ': "SHCH", 'Ъ': "",
		'Ы': "Y", 'Ь': "", 'Э': "E", 'Ю': "YU", 'Я': "YA",
	}

	// Leet-подстановки
	leetLat = map[string]string{"0": "O", "3": "Z", "4": "CH"}
	leetCyr = map[string]string{"3": "З", "4": "Ч", "0": "О"}
)

// =======================
// Утилиты нормализации
// =======================

// normalizeSpaces нормализует пробелы
func normalizeSpaces(s string) string {
	spaceRegex := regexp.MustCompile(`[ \t\r\n]+`)
	return strings.TrimSpace(spaceRegex.ReplaceAllString(s, " "))
}

// stripPhones удаляет телефоны
func stripPhones(s string) string {
	phoneRegex := regexp.MustCompile(`\+?\d[\d\-\s()]{6,}`)
	return phoneRegex.ReplaceAllString(s, " ")
}

// stripLeadingPunct удаляет ведущие пунктуационные символы
func stripLeadingPunct(s string) string {
	leadingPunctRegex := regexp.MustCompile(`^[/\-]+\s*`)
	return leadingPunctRegex.ReplaceAllString(s, "")
}

// stripTrailingDelims удаляет завершающие разделители
func stripTrailingDelims(s string) string {
	trailingDelimsRegex := regexp.MustCompile(`[\/\-\.,;]+$`)
	return strings.TrimSpace(trailingDelimsRegex.ReplaceAllString(s, ""))
}

// applyLeetCyr применяет leet-подстановки для кириллицы
func applyLeetCyr(s string) string {
	result := s
	for find, replace := range leetCyr {
		result = strings.ReplaceAll(result, find, replace)
	}

	return result
}

// applyLeetLat применяет leet-подстановки для латиницы
func applyLeetLat(s string) string {
	result := s
	for find, replace := range leetLat {
		result = strings.ReplaceAll(result, find, replace)
	}
	return result
}

/*
func specialReplacements(str string) string {

	str = strings.ReplaceAll(str, "ЕВИ4", "ЕВИЧ")
	str = strings.ReplaceAll(str, "ОВИ4", "ОВИЧ")
	return str
} */

// cyrToLat транслитерирует кириллицу в латиницу
func cyrToLat(str string) string {
	var result strings.Builder
	for _, ch := range strings.ToUpper(str) {
		if replacement, exists := cyrToLatMap[ch]; exists {
			result.WriteString(replacement)
		} else {
			result.WriteRune(ch)
		}
	}
	return result.String()
}

// makeTokenRegex создает регулярное выражение для поиска токена с границами
func makeTokenRegex(token string) *regexp.Regexp {
	wordChars := `A-Za-zА-Яа-яЁё0-9`
	notWord := `(?:^|[^` + wordChars + `])`
	notWordEnd := `(?:$|[^` + wordChars + `])`
	return regexp.MustCompile(notWord + regexp.QuoteMeta(token) + notWordEnd)
}

// =======================
// Извлечение блока OPR
// =======================

// ExtractOPR извлекает текст OPR из сырого текста
func ExtractOPR(rawText string) string {
	s := strings.ToUpper(strings.TrimSpace(rawText))
	if s == "" {
		return ""
	}

	// Ищем OPR/
	oprRegex := regexp.MustCompile(`OPR\/+`)
	match := oprRegex.FindStringIndex(s)
	if match == nil {
		return ""
	}

	start := match[1]
	tail := s[start:]

	// Создаем регулярное выражение для поиска следующего ключа
	nextKeyPattern := `\s*(?:\d+\s+)?(?:` + strings.Join(nextKeys, "|") + `)(?:\/|\b)`
	nextKeyRegex := regexp.MustCompile(nextKeyPattern)

	keyMatch := nextKeyRegex.FindStringIndex(tail)
	var end int
	if keyMatch != nil {
		end = keyMatch[0]
	} else {
		end = len(tail)
	}

	out := tail[:end]

	// Нормализации
	out = normalizeSpaces(out)
	out = stripLeadingPunct(out)
	out = stripTrailingDelims(out)

	return out
}

// =======================
// Классификация оператора
// =======================

// preparedText подготовленный текст для классификации
type preparedText struct {
	vRaw string
	vCyr string
	vLat string
}

// prepareForClassification подготавливает текст для классификации
func prepareForClassification(opr string) preparedText {
	s := strings.TrimSpace(opr)
	if s == "" {
		return preparedText{}
	}

	// Убираем телефоны и кавычки
	s = stripPhones(s)
	quoteRegex := regexp.MustCompile(`[«»"“”'` + "`" + `]+`)
	s = quoteRegex.ReplaceAllString(s, " ")
	s = normalizeSpaces(s)

	// Ветвь кириллицы (leet → кириллица)
	vCyr := strings.ToUpper(applyLeetCyr(s))

	// Ветвь латиницы: leet → латиница, + транслит кириллицы → латиница
	leetLat := applyLeetLat(s)
	vLat := strings.ToUpper(cyrToLat(leetLat))

	return preparedText{
		vRaw: s,
		vCyr: vCyr,
		vLat: vLat,
	}
}

// containsAnyToken проверяет наличие любого токена в строке
func containsAnyToken(str string, tokens []string) bool {
	for _, token := range tokens {
		regex := makeTokenRegex(token)
		if regex.MatchString(str) {
			return true
		}
	}
	return false
}

// containsAgencyLatSmart проверяет наличие ведомств на латинице
func containsAgencyLatSmart(vLat string) bool {
	if containsAnyToken(vLat, agencyWordsLat) {
		return true
	}

	// Проверяем MVD/MWD
	mvdRegex := regexp.MustCompile(`(?:^|[^A-Za-z0-9])M[VW]D(?:$|[^A-Za-z0-9])`)
	if mvdRegex.MatchString(vLat) {
		return true
	}

	// Проверяем MCHS
	return containsAnyToken(vLat, []string{"MCHS"})
}

// countFioCyr подсчитывает количество ФИО в кириллической строке
func countFioCyr(s string) int {
	// Ищем слова из кириллических букв (возможно с дефисами)
	wordRegex := regexp.MustCompile(`[А-ЯЁ]+(?:-[А-ЯЁ]+)?`)
	matches := wordRegex.FindAllString(s, -1)

	if len(matches) < 2 {
		return 0
	}

	count := 0
	for i := 0; i < len(matches)-1; i++ {
		first := strings.ToUpper(matches[i])
		second := strings.ToUpper(matches[i+1])

		// Проверяем суффиксы фамилий
		hasSurnameSuffix := false
		for _, suffix := range surnameSuffixesCyr {
			if strings.HasSuffix(first, suffix) {
				hasSurnameSuffix = true
				break
			}
		}

		// Проверяем отчества
		hasPatronymic := strings.HasSuffix(second, "ИЧ") || strings.HasSuffix(second, "ВИЧ")

		if hasSurnameSuffix || hasPatronymic {
			count++
			i++ // Пропускаем следующее слово
		}
	}

	return count
}

// countFioLat подсчитывает количество ФИО в латинской строке
func countFioLat(s string) int {
	// Ищем слова из латинских букв (возможно с дефисами)
	wordRegex := regexp.MustCompile(`[A-Z]+(?:-[A-Z]+)?`)
	matches := wordRegex.FindAllString(s, -1)

	if len(matches) < 2 {
		return 0
	}

	count := 0
	for i := 0; i < len(matches)-1; i++ {
		first := strings.ToUpper(matches[i])
		second := strings.ToUpper(matches[i+1])

		// Проверяем суффиксы фамилий
		hasSurnameSuffix := false
		for _, suffix := range surnameSuffixesLat {
			if strings.HasSuffix(first, suffix) {
				hasSurnameSuffix = true
				break
			}
		}

		// Проверяем отчества
		hasPatronymic := strings.HasSuffix(second, "ICH") || strings.HasSuffix(second, "VICH")

		if hasSurnameSuffix || hasPatronymic {
			count++
			i++ // Пропускаем следующее слово
		}
	}

	return count
}

// ClassifyOperatorKind классифицирует тип оператора
func ClassifyOperatorKind(opr string) string {
	raw := strings.TrimSpace(opr)
	if raw == "" {
		return "Не определено"
	}

	prepared := prepareForClassification(raw)

	// 1) Индивидуальный предприниматель / гражданин
	if matched, _ := regexp.MatchString(`^\s*ИП(?:\s|\.|$)`, prepared.vRaw); matched {
		return "Физ. лицо"
	}
	if containsAnyToken(prepared.vCyr, []string{"ГРАЖДАНИН"}) {
		return "Физ. лицо"
	}
	if strings.Contains(prepared.vCyr, "ЧАСТНОЕ ЛИЦО") {
		return "Физ. лицо"
	}
	if containsAnyToken(prepared.vLat, []string{"INDIVIDUAL", "PRIVATE PERSON"}) {
		return "Физ. лицо"
	}

	// 2) ЮЛ: ведомства и ОПФ
	hasLegalTokens := containsAnyToken(prepared.vCyr, agencyWordsCyr) ||
		containsAgencyLatSmart(prepared.vLat) ||
		containsAnyToken(prepared.vCyr, legalTokens) ||
		containsAnyToken(prepared.vLat, legalTokens)

	if hasLegalTokens {
		return "Юр. лицо"
	}

	// 3) ФИО (учитываем, что их может быть несколько в OPR)
	fioCount := countFioCyr(prepared.vCyr) + countFioLat(prepared.vLat)
	if fioCount >= 1 {
		return "Физ. лицо"
	}

	// 4) Хинты на организации (только если ФИО не нашли)
	if containsAnyToken(prepared.vLat, legalHintsLat) {
		return "Юр. лицо"
	}

	// 5) Не распознали
	return "Не определено"
}

// ExtractAndClassifyOperator извлекает и классифицирует оператора
func ExtractAndClassifyOperator(rawText string) OperatorResult {
	operator := ExtractOPR(rawText)
	if operator == "" {
		return OperatorResult{}
	}

	operatorType := ClassifyOperatorKind(operator)

	return OperatorResult{
		Operator:     operator,
		OperatorType: operatorType,
	}
}
