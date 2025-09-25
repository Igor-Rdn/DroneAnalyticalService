package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"project/packages/mongodb"
	"project/packages/parsing"

	"github.com/gin-gonic/gin"
	"github.com/xuri/excelize/v2"
)

func main() {

	// Подключение к MongoDB
	collection := mongodb.ConnectToMongoDB()

	router := gin.Default()

	// Пинг-роут для healthcheck
	router.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "pong"})
	})

	router.POST("/upload", func(c *gin.Context) {
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
			result, err := collection.InsertOne(context.Background(), flightData)
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
	})

	fmt.Println("🚀 Сервер запущен на http://localhost:8080")
	router.Run(":8080")

}
