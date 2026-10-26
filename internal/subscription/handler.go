package subscription

import (
	"errors"
	"log"
	"net/http"
	"strconv"

	"relay/types"

	"github.com/gin-gonic/gin"
)

type Handler struct{ service *Service }

func RegisterRoutes(router gin.IRoutes, service *Service) {
	h := &Handler{service: service}
	router.POST("/subscribers", h.createSubscriber)
	router.GET("/subscribers", h.listSubscribers)
	router.GET("/subscribers/:id", h.getSubscriber)
	router.PUT("/subscribers/:id", h.updateSubscriber)
	router.DELETE("/subscribers/:id", h.deleteSubscriber)
	router.POST("/subscriptions", h.createSubscription)
	router.GET("/subscriptions", h.listSubscriptions)
	router.GET("/subscriptions/:id", h.getSubscription)
	router.PUT("/subscriptions/:id", h.updateSubscription)
	router.DELETE("/subscriptions/:id", h.deleteSubscription)
}

func respond(c *gin.Context, status int, value any, err error) {
	if err != nil {
		message := err.Error()
		switch {
		case errors.Is(err, ErrInvalid):
			status = http.StatusBadRequest
		case errors.Is(err, ErrNotFound):
			status = http.StatusNotFound
		case errors.Is(err, ErrConflict):
			status = http.StatusConflict
		default:
			log.Printf("Subscription registry request failed: %v", err)
			status, message = http.StatusInternalServerError, "subscription registry request failed"
		}
		c.JSON(status, gin.H{"error": message})
		return
	}
	if status == http.StatusNoContent {
		c.Status(status)
		return
	}
	c.JSON(status, value)
}

func requestID(c *gin.Context) (int64, bool) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id must be a positive integer"})
		return 0, false
	}
	return id, true
}

func listOptions(c *gin.Context) (types.ListOptions, bool) {
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "100"))
	if err != nil || limit < 1 || limit > 1000 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be between 1 and 1000"})
		return types.ListOptions{}, false
	}
	offset, err := strconv.Atoi(c.DefaultQuery("offset", "0"))
	if err != nil || offset < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "offset must be nonnegative"})
		return types.ListOptions{}, false
	}
	return types.ListOptions{Limit: limit, Offset: offset}, true
}

func bind(c *gin.Context, request any) bool {
	if err := c.ShouldBindJSON(request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON request body"})
		return false
	}
	return true
}

func (h *Handler) createSubscriber(c *gin.Context) {
	var req types.SubscriberRequest
	if !bind(c, &req) {
		return
	}
	row, err := h.service.CreateSubscriber(c.Request.Context(), req)
	respond(c, http.StatusCreated, row, err)
}

func (h *Handler) getSubscriber(c *gin.Context) {
	id, ok := requestID(c)
	if !ok {
		return
	}
	row, err := h.service.GetSubscriber(c.Request.Context(), id)
	respond(c, http.StatusOK, row, err)
}

func (h *Handler) listSubscribers(c *gin.Context) {
	options, ok := listOptions(c)
	if !ok {
		return
	}
	rows, err := h.service.ListSubscribers(c.Request.Context(), options)
	respond(c, http.StatusOK, rows, err)
}

func (h *Handler) updateSubscriber(c *gin.Context) {
	id, ok := requestID(c)
	if !ok {
		return
	}
	var req types.SubscriberRequest
	if !bind(c, &req) {
		return
	}
	row, err := h.service.UpdateSubscriber(c.Request.Context(), id, req)
	respond(c, http.StatusOK, row, err)
}

func (h *Handler) deleteSubscriber(c *gin.Context) {
	id, ok := requestID(c)
	if !ok {
		return
	}
	respond(c, http.StatusNoContent, nil, h.service.DeleteSubscriber(c.Request.Context(), id))
}

func (h *Handler) createSubscription(c *gin.Context) {
	var req types.SubscriptionRequest
	if !bind(c, &req) {
		return
	}
	row, err := h.service.CreateSubscription(c.Request.Context(), req)
	respond(c, http.StatusCreated, row, err)
}

func (h *Handler) getSubscription(c *gin.Context) {
	id, ok := requestID(c)
	if !ok {
		return
	}
	row, err := h.service.GetSubscription(c.Request.Context(), id)
	respond(c, http.StatusOK, row, err)
}

func (h *Handler) listSubscriptions(c *gin.Context) {
	options, ok := listOptions(c)
	if !ok {
		return
	}
	rows, err := h.service.ListSubscriptions(c.Request.Context(), options)
	respond(c, http.StatusOK, rows, err)
}

func (h *Handler) updateSubscription(c *gin.Context) {
	id, ok := requestID(c)
	if !ok {
		return
	}
	var req types.SubscriptionRequest
	if !bind(c, &req) {
		return
	}
	row, err := h.service.UpdateSubscription(c.Request.Context(), id, req)
	respond(c, http.StatusOK, row, err)
}

func (h *Handler) deleteSubscription(c *gin.Context) {
	id, ok := requestID(c)
	if !ok {
		return
	}
	respond(c, http.StatusNoContent, nil, h.service.DeleteSubscription(c.Request.Context(), id))
}
