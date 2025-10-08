package api

import (
	"encoding/json"
	"log"
	"net/http"
)

type Handler struct {
}

type HealthResponse struct {
	Status string `json:"status"`
}

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) GetHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(HealthResponse{
		Status: "UP",
	})
	if err != nil {
		log.Printf("Error encoding response: %v", err)
		http.Error(w, "Error encoding response", 500)
		return
	}
}
