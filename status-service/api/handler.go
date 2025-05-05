package api

import (
	"encoding/json"
	"log"
	"net/http"
)

type Handler struct {
	sp StatusProvider
}

type StatusProvider interface {
	GetLastProcessedTick() (tick uint32, err error)
	GetSkippedTicks() ([]uint32, error)
}

type StatusResponse struct {
	ProcessedTransactionTick uint32 `json:"processedTransactionTick"`
}

type HealthResponse struct {
	Status string `json:"status"`
}

type SkippedTicksResponse struct {
	SkippedTransactionTicks []uint32 `json:"skippedTransactionTicks"`
}

func NewHandler(sp StatusProvider) *Handler {
	return &Handler{sp: sp}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {

	lastProcessedTick, err := h.sp.GetLastProcessedTick()
	if err != nil {
		log.Printf("Error getting last processed tick: %v", err)
		http.Error(w, "Error getting last processed tick", 500)
		return
	}

	err = json.NewEncoder(w).Encode(StatusResponse{
		ProcessedTransactionTick: lastProcessedTick,
	})
	if err != nil {
		log.Printf("Error encoding response: %v", err)
		http.Error(w, "Error encoding response", 500)
		return
	}

	w.Header().Add("Content-Type", "application/json")
}

func (h *Handler) GetSkippedTicks(w http.ResponseWriter, _ *http.Request) {
	ticks, err := h.sp.GetSkippedTicks()
	if err != nil {
		log.Printf("Error getting skipped ticks: %v", err)
		http.Error(w, "Error getting skipped ticks", 500)
		return
	}

	w.Header().Add("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(SkippedTicksResponse{
		SkippedTransactionTicks: ticks,
	})
	if err != nil {
		log.Printf("Error encoding response: %v", err)
		http.Error(w, "Error encoding response", 500)
		return
	}
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

func (h *Handler) GetStatus(w http.ResponseWriter, _ *http.Request) {
	lastProcessedTick, err := h.sp.GetLastProcessedTick()
	if err != nil {
		log.Printf("Error getting last processed tick: %v", err)
		http.Error(w, "Error getting last processed tick", 500)
		return
	}

	w.Header().Add("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(StatusResponse{
		ProcessedTransactionTick: lastProcessedTick,
	})
	if err != nil {
		log.Printf("Error encoding response: %v", err)
		http.Error(w, "Error encoding response", 500)
		return
	}
}
