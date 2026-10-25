package main

import (
	"log"
	"net/http"
	"os"
	"relay/internal/ingestion"
	"relay/internal/types"

	"github.com/gin-gonic/gin"
)

var ingestionSvc *ingestion.Service

func HandleEvent(ctx *gin.Context) {
	var evt types.Event
	if err := ctx.BindJSON(&evt); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"success": false})
		return
	}
	published := ingestionSvc.PublishEvent(ctx.Request.Context(), evt)
	if !published {
		ctx.JSON(http.StatusBadRequest, gin.H{"success": false})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"success": true})
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	router := gin.Default()
	router.GET("/readyz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})
	router.POST("/", HandleEvent)
	var err error
	ingestionSvc, err = ingestion.NewService(os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("Ingestion service initialization failed: %v", err)
		return
	}
	if err := router.Run(":" + port); err != nil {
		log.Fatalf("Failed to start API server: %v", err)
	}
}
