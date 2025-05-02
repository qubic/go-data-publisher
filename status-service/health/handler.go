package health

import (
	"encoding/json"
	"log"
	"net/http"
)

type Handler struct {
	sp StatusProvider
}

type StatusProvider interface {
	GetLastProcessedTick() uint32
}

type StatusResponse struct {
	Status              string `json:"status"`
	LatestProcessedTick uint32 `json:"latestProcessedTick"`
}

func NewHandler(sp StatusProvider) *Handler {
	return &Handler{sp: sp}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(StatusResponse{
		Status:              "UP",
		LatestProcessedTick: h.sp.GetLastProcessedTick(),
	})
	if err != nil {
		log.Printf("Error writing status response: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}
